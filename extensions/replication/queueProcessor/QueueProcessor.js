'use strict'; // eslint-disable-line

const async = require('async');
const assert = require('assert');
const AWS = require('aws-sdk');
const BackOff = require('backo');

const Logger = require('werelogs').Logger;

const errors = require('arsenal').errors;
const jsutil = require('arsenal').jsutil;
const VaultClient = require('vaultclient').Client;

const authdata = require('../../../conf/authdata.json');

const BackbeatConsumer = require('../../../lib/BackbeatConsumer');
const BackbeatClient = require('../../../lib/clients/BackbeatClient');
const QueueEntry = require('../utils/QueueEntry');
const CredentialsManager = require('../../../credentials/CredentialsManager');

class _AccountAuthManager {
    constructor(authConfig, log) {
        assert.strictEqual(authConfig.type, 'account');

        this.log = log;
        const accountInfo = authdata.accounts.find(
            account => account.name === authConfig.account);
        if (accountInfo === undefined) {
            throw Error(`No such account registered: ${authConfig.account}`);
        }
        if (accountInfo.arn === undefined) {
            throw Error(`Configured account ${authConfig.account} has no ` +
                        '"arn" property defined');
        }
        if (accountInfo.canonicalID === undefined) {
            throw Error(`Configured account ${authConfig.account} has no ` +
                        '"canonicalID" property defined');
        }
        if (accountInfo.displayName === undefined) {
            throw Error(`Configured account ${authConfig.account} has no ` +
                        '"displayName" property defined');
        }
        this.accountArn = accountInfo.arn;
        this.canonicalID = accountInfo.canonicalID;
        this.displayName = accountInfo.displayName;
        this.credentials = new AWS.Credentials(accountInfo.keys.access,
                                               accountInfo.keys.secret);
    }

    getCredentials() {
        return this.credentials;
    }

    lookupAccountAttributes(accountId, cb) {
        const localAccountId = this.accountArn.split(':')[3];
        if (localAccountId !== accountId) {
            this.log.error('Target account for replication must match ' +
                           'configured destination account ARN',
                           { targetAccountId: accountId,
                             localAccountId });
            return process.nextTick(() => cb(errors.AccountNotFound));
        }
        // return local account's attributes
        return process.nextTick(
            () => cb(null, { canonicalID: this.canonicalID,
                             displayName: this.displayName }));
    }
}

class _RoleAuthManager {
    constructor(authConfig, roleArn, log) {
        this._log = log;
        const { host, port } = authConfig.vault;
        this._vaultclient = new VaultClient(host, port);
        this.credentials = new CredentialsManager(host, port,
                                                  'replication', roleArn);
    }

    getCredentials() {
        return this.credentials;
    }

    lookupAccountAttributes(accountId, cb) {
        this._vaultclient.getCanonicalIdsByAccountIds([accountId], {},
            (err, res) => {
                if (err) {
                    return cb(err);
                }
                if (!res || !res.message || !res.message.body
                    || res.message.body.length === 0) {
                    return cb(errors.AccountNotFound);
                }
                return cb(null, {
                    canonicalID: res.message.body[0].canonicalId,
                    displayName: res.message.body[0].name,
                });
            });
    }
}


function _extractAccountIdFromRole(role) {
    return role.split(':')[4];
}

class QueueProcessor {

    /**
     * Create a queue processor object to activate Cross-Region
     * Replication from a kafka topic dedicated to store replication
     * entries to a target S3 endpoint.
     *
     * @constructor
     * @param {Object} zkConfig - zookeeper configuration object
     * @param {string} zkConfig.endpoint - zookeeper endpoint string
     *   as "host:port[/chroot]"
     * @param {Object} sourceConfig - source S3 configuration
     * @param {Object} sourceConfig.s3 - s3 endpoint configuration object
     * @param {Object} sourceConfig.auth - authentication info on source
     * @param {Object} destConfig - target S3 configuration
     * @param {Object} destConfig.s3 - s3 endpoint configuration object
     * @param {Object} destConfig.auth - authentication info on target
     * @param {Object} repConfig - replication configuration object
     * @param {String} repConfig.topic - replication topic name
     * @param {String} repConfig.queueProcessor - config object
     *   specific to queue processor
     * @param {String} repConfig.queueProcessor.groupId - kafka
     *   consumer group ID
     * @param {Logger} logConfig - logging configuration object
     * @param {String} logConfig.logLevel - logging level
     * @param {Logger} logConfig.dumpLevel - dump level
     */
    constructor(zkConfig, sourceConfig, destConfig, repConfig, logConfig) {
        this.zkConfig = zkConfig;
        this.sourceConfig = sourceConfig;
        this.destConfig = destConfig;
        this.repConfig = repConfig;
        this.logConfig = logConfig;

        this.logger = new Logger('Backbeat:Replication:QueueProcessor',
                                 { level: logConfig.logLevel,
                                   dump: logConfig.dumpLevel });
        this.log = this.logger.newRequestLogger();

        this.s3sourceAuthManager = null;
        this.s3destAuthManager = null;
        this.S3source = null;
        this.backbeatSource = null;
        this.backbeatDest = null;
    }

    _createAuthManager(authConfig, roleArn) {
        if (authConfig.type === 'account') {
            return new _AccountAuthManager(authConfig, this.log);
        }
        return new _RoleAuthManager(authConfig, roleArn, this.log);
    }

    _getBucketReplicationRoles(entry, cb) {
        this.log.debug('getting bucket replication',
                       { entry: entry.getLogInfo() });
        const entryRolesString = entry.getReplicationRoles();
        let entryRoles;
        if (entryRolesString !== undefined) {
            entryRoles = entryRolesString.split(',');
        }
        if (entryRoles === undefined || entryRoles.length !== 2) {
            this.log.error('expecting two roles separated by a ' +
                           'comma in replication configuration',
                           { entry: entry.getLogInfo(),
                             origin: this.sourceConfig.s3 });
            return cb(errors.InternalError);
        }
        this._setupClients(entryRoles[0], entryRoles[1]);
        return this.S3source.getBucketReplication(
            { Bucket: entry.getBucket() }, (err, data) => {
                if (err) {
                    this.log.error('error getting replication ' +
                                   'configuration from S3',
                                   { entry: entry.getLogInfo(),
                                     origin: this.sourceConfig.s3,
                                     error: { message: err.message },
                                     errorStack: err.stack,
                                     httpStatus: err.statusCode });
                    return cb(err);
                }
                const roles = data.ReplicationConfiguration.Role.split(',');
                if (roles.length !== 2) {
                    this.log.error('expecting two roles separated by a ' +
                                   'comma in replication configuration',
                                   { entry: entry.getLogInfo(),
                                     origin: this.sourceConfig.s3 });
                    return cb(errors.InternalError);
                }
                return cb(null, roles[0], roles[1]);
            });
    }

    _processRoles(sourceEntry, sourceRole, destEntry, targetRole, cb) {
        this.log.debug('processing role for destination',
                       { entry: destEntry.getLogInfo(),
                         role: targetRole });
        const targetAccountId = _extractAccountIdFromRole(targetRole);
        this.s3destAuthManager.lookupAccountAttributes(
            targetAccountId, (err, accountAttr) => {
                if (err) {
                    return cb(err);
                }
                this.log.debug('setting owner info in target metadata',
                               { entry: destEntry.getLogInfo(),
                                 accountAttr });
                destEntry.setOwner(accountAttr.canonicalID,
                                   accountAttr.displayName);
                return cb(null, accountAttr);
            });
    }

    _getData(entry, cb) {
        this.log.debug('getting data', { entry: entry.getLogInfo() });
        const req = this.S3source.getObject({
            Bucket: entry.getBucket(),
            Key: entry.getObjectKey(),
            VersionId: entry.getEncodedVersionId() });
        const incomingMsg = req.createReadStream();
        req.on('error', err => {
            this.log.error('an error occurred when getting data from S3',
                           { entry: entry.getLogInfo(),
                             origin: this.sourceConfig.s3,
                             error: { message: err.message },
                             httpStatus: err.statusCode });
            incomingMsg.emit('error', err);
        });
        return cb(null, incomingMsg);
    }

    _putData(entry, sourceStream, cb) {
        this.log.debug('putting data', { entry: entry.getLogInfo() });
        const cbOnce = jsutil.once(cb);
        sourceStream.on('error', err => {
            this.log.error('error from source S3 server',
                           { entry: entry.getLogInfo(),
                             error: { message: err.message } });
            return cbOnce(err);
        });
        this.backbeatDest.putData({
            Bucket: entry.getBucket(),
            Key: entry.getObjectKey(),
            CanonicalID: entry.getOwnerCanonicalId(),
            ContentLength: entry.getContentLength(),
            ContentMD5: entry.getContentMD5(),
            Body: sourceStream,
        }, (err, data) => {
            if (err) {
                this.log.error('an error occurred when putting data to S3',
                               { entry: entry.getLogInfo(),
                                 origin: this.destConfig.s3,
                                 error: { message: err.message },
                                 errorStack: err.stack });
                return cbOnce(err);
            }
            return cbOnce(null, data.Location);
        });
    }

    _putMetadata(where, entry, cb) {
        this.log.debug('putting metadata',
                       { where, entry: entry.getLogInfo(),
                         replicationStatus: entry.getReplicationStatus() });
        const cbOnce = jsutil.once(cb);
        const target = where === 'source' ?
                  this.backbeatSource : this.backbeatDest;
        const mdBlob = entry.getMetadataBlob();
        target.putMetadata({
            Bucket: entry.getBucket(),
            Key: entry.getObjectKey(),
            ContentLength: Buffer.byteLength(mdBlob),
            Body: mdBlob,
        }, (err, data) => {
            if (err) {
                this.log.error('an error occurred when putting metadata to S3',
                               { entry: entry.getLogInfo(),
                                 origin: this.destConfig.s3,
                                 error: { message: err.message },
                                 errorStack: err.stack });
                return cbOnce(err);
            }
            return cbOnce(null, data);
        });
    }

    _setupClients(sourceRole, targetRole) {
        this.s3sourceAuthManager =
            this._createAuthManager(this.sourceConfig.auth, sourceRole);
        this.s3destAuthManager =
            this._createAuthManager(this.destConfig.auth, targetRole);
        this.S3source = new AWS.S3({
            endpoint: `${this.sourceConfig.s3.transport}://` +
                `${this.sourceConfig.s3.host}:${this.sourceConfig.s3.port}`,
            credentials:
            this.s3sourceAuthManager.getCredentials(),
            sslEnabled: true,
            s3ForcePathStyle: true,
            signatureVersion: 'v4',
        });
        this.backbeatSource = new BackbeatClient({
            endpoint: `${this.sourceConfig.s3.transport}://` +
                `${this.sourceConfig.s3.host}:${this.sourceConfig.s3.port}`,
            credentials:
            this.s3sourceAuthManager.getCredentials(),
            sslEnabled: true,
        });

        this.backbeatDest = new BackbeatClient({
            endpoint: `${this.destConfig.s3.transport}://` +
                `${this.destConfig.s3.host}:${this.destConfig.s3.port}`,
            credentials:
            this.s3destAuthManager.getCredentials(),
            sslEnabled: true,
        });
    }

    _processKafkaEntry(kafkaEntry, done) {
        const sourceEntry = QueueEntry.createFromKafkaEntry(kafkaEntry);
        if (sourceEntry.error) {
            this.log.error('error processing source entry',
                           { error: sourceEntry.error });
            return process.nextTick(() => done(errors.InternalError));
        }
        const backoffCtx = new BackOff({ min: 1000, max: 300000,
                                         jitter: 0.1, factor: 1.5 });
        return this._tryProcessQueueEntry(sourceEntry, backoffCtx, done);
    }

    _tryProcessQueueEntry(sourceEntry, backoffCtx, done) {
        const destEntry = sourceEntry.toReplicaEntry();

        this.log.debug('processing entry',
                       { entry: sourceEntry.getLogInfo() });

        const _handleReplicationOutcome = err => {
            if (!err) {
                this.log.debug('replication succeeded for object, updating ' +
                               'source replication status to COMPLETED',
                               { entry: sourceEntry.getLogInfo() });
                return this._tryUpdateReplicationStatus(
                    sourceEntry.toCompletedEntry(), backoffCtx, done);
            }
            // Rely on AWS SDK notion of retryable error to decide if
            // we should set the entry replication status to FAILED
            // (non retryable) or retry later.
            if (err.retryable) {
                this.log.warn('temporary failure to replicate object',
                              { entry: sourceEntry.getLogInfo(),
                                error: err });
                return this._retryProcessQueueEntry(sourceEntry, backoffCtx,
                                                    done);
            }
            this.log.debug('replication failed permanently for object, ' +
                           'updating replication status to FAILED',
                           { entry: sourceEntry.getLogInfo() });
            return this._tryUpdateReplicationStatus(
                sourceEntry.toFailedEntry(), backoffCtx, done);
        };

        if (sourceEntry.isDeleteMarker()) {
            return async.waterfall([
                next => {
                    this._getBucketReplicationRoles(sourceEntry, next);
                },
                (sourceRole, targetRole, next) => {
                    this._processRoles(sourceEntry, sourceRole,
                                       destEntry, targetRole, next);
                },
                // put metadata in target bucket
                (accountAttr, next) => {
                    // TODO check that bucket role matches role in metadata
                    this._putMetadata('target', destEntry, next);
                },
            ], _handleReplicationOutcome);
        }
        return async.waterfall([
            // get data stream from source bucket
            next => {
                this._getBucketReplicationRoles(sourceEntry, next);
            },
            (sourceRole, targetRole, next) => {
                this._processRoles(sourceEntry, sourceRole,
                                   destEntry, targetRole, next);
            },
            (accountAttr, next) => {
                // TODO check that bucket role matches role in metadata
                this._getData(sourceEntry, next);
            },
            // put data in target bucket
            (stream, next) => {
                this._putData(destEntry, stream, next);
            },
            // update location, replication status and put metadata in
            // target bucket
            (location, next) => {
                destEntry.setLocation(location);
                this._putMetadata('target', destEntry, next);
            },
        ], _handleReplicationOutcome);
    }

    _tryUpdateReplicationStatus(updatedSourceEntry, backoffCtx, done) {
        const _doneUpdate = err => {
            if (!err) {
                this.log.info('replication status updated',
                              { entry: updatedSourceEntry.getLogInfo(),
                                replicationStatus:
                                updatedSourceEntry.getReplicationStatus() });
                return done();
            }
            this.log.error('an error occurred when writing replication ' +
                           'status',
                           { entry: updatedSourceEntry.getLogInfo(),
                             replicationStatus:
                             updatedSourceEntry.getReplicationStatus() });
            // Rely on AWS SDK notion of retryable error to decide if
            // we should retry or give up updating the status.
            if (err.retryable) {
                return this._retryUpdateReplicationStatus(
                    updatedSourceEntry, backoffCtx, done);
            }
            return done();
        };

        if (this.backbeatSource !== null) {
            return this._putMetadata('source',
                                     updatedSourceEntry, _doneUpdate);
        }
        this.log.info('replication status update skipped',
                      { entry: updatedSourceEntry.getLogInfo(),
                        replicationStatus:
                        updatedSourceEntry.getReplicationStatus() });
        return done();
    }

    _retryProcessQueueEntry(sourceEntry, backoffCtx, done) {
        const retryDelayMs = backoffCtx.duration();
        this.log.info('scheduled retry of entry replication',
                      { entry: sourceEntry.getLogInfo(),
                        retryDelay: `${retryDelayMs}ms` });
        setTimeout(this._tryProcessQueueEntry.bind(
            this, sourceEntry, backoffCtx, done),
                   retryDelayMs);
    }

    _retryUpdateReplicationStatus(updatedSourceEntry, backoffCtx, done) {
        const retryDelayMs = backoffCtx.duration();
        this.log.info('scheduled retry of replication status update',
                      { entry: updatedSourceEntry.getLogInfo(),
                        retryDelay: `${retryDelayMs}ms` });
        setTimeout(this._tryUpdateReplicationStatus.bind(
            this, updatedSourceEntry, backoffCtx, done),
                   retryDelayMs);
    }

    start() {
        const consumer = new BackbeatConsumer({
            zookeeper: this.zkConfig,
            log: this.logConfig,
            topic: this.repConfig.topic,
            groupId: this.repConfig.queueProcessor.groupId,
            concurrency: 1, // replication has to process entries in
                            // order, so one at a time
            queueProcessor: this._processKafkaEntry.bind(this),
        });
        consumer.on('error', () => {});
        consumer.subscribe();

        this.log.info('queue processor is ready to consume ' +
                      'replication entries');
    }
}

module.exports = QueueProcessor;
