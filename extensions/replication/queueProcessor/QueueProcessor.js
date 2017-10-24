'use strict'; // eslint-disable-line

const http = require('http');

const Logger = require('werelogs').Logger;

const errors = require('arsenal').errors;
const RoundRobin = require('arsenal').network.RoundRobin;
const VaultClient = require('vaultclient').Client;

const BackbeatConsumer = require('../../../lib/BackbeatConsumer');
const QueueEntry = require('../utils/QueueEntry');
const ReplicationTaskScheduler = require('./ReplicationTaskScheduler');
const QueueProcessorTask = require('./QueueProcessorTask');
const MultipleBackendTask = require('./MultipleBackendTask');
const MetricsProducer = require('../../../lib/MetricsProducer');

/**
* Given that the largest object JSON from S3 is about 1.6 MB and adding some
* padding to it, Backbeat replication topic is currently setup with a config
* max.message.bytes.limit to 5MB. Consumers need to update their fetchMaxBytes
* to get atleast 5MB put in the Kafka topic, adding a little extra bytes of
* padding for approximation.
*/
const CONSUMER_FETCH_MAX_BYTES = 5000020;

class QueueProcessor {

    /**
     * Create a queue processor object to activate Cross-Region
     * Replication from a kafka topic dedicated to store replication
     * entries to a target S3 endpoint.
     *
     * @constructor
     * @param {Object} zkConfig - zookeeper configuration object
     * @param {string} zkConfig.connectionString - zookeeper connection string
     *   as "host:port[/chroot]"
     * @param {Object} sourceConfig - source S3 configuration
     * @param {Object} sourceConfig.s3 - s3 endpoint configuration object
     * @param {Object} sourceConfig.auth - authentication info on source
     * @param {Object} destConfig - target S3 configuration
     * @param {Object} destConfig.auth - authentication info on target
     * @param {Object} repConfig - replication configuration object
     * @param {String} repConfig.topic - replication topic name
     * @param {String} repConfig.queueProcessor - config object
     *   specific to queue processor
     * @param {String} repConfig.queueProcessor.groupId - kafka
     *   consumer group ID
     * @param {String} repConfig.queueProcessor.retryTimeoutS -
     *   number of seconds before giving up retries of an entry
     *   replication
     * @param {Object} mConfig - metrics configuration
     * @param {Object} redisConfig - redis configurations
     */
    constructor(zkConfig, sourceConfig, destConfig, repConfig, mConfig,
    redisConfig) {
        this.zkConfig = zkConfig;
        this.sourceConfig = sourceConfig;
        this.destConfig = destConfig;
        this.repConfig = repConfig;
        this.mConfig = mConfig;
        this.redisConfig = redisConfig;

        this._mProducer = null;

        this.logger = new Logger('Backbeat:Replication:QueueProcessor');

        // global variables
        // TODO: for SSL support, create HTTPS agents instead
        this.sourceHTTPAgent = new http.Agent({ keepAlive: true });
        this.destHTTPAgent = new http.Agent({ keepAlive: true });

        // FIXME support multiple destination sites
        if (destConfig.bootstrapList.length > 0) {
            this.destHosts =
                new RoundRobin(destConfig.bootstrapList[0].servers,
                               { defaultPort: 80 });
        } else {
            this.destHosts = null;
        }

        if (sourceConfig.auth.type === 'role') {
            const { host, port } = sourceConfig.auth.vault;
            this.sourceVault = new VaultClient(host, port);
        }
        if (destConfig.auth.type === 'role') {
            // vault client cache per destination
            this.destVaults = {};
        }

        this.taskScheduler = new ReplicationTaskScheduler(
            (ctx, done) => ctx.task.processQueueEntry(ctx.entry, done));
    }

    getStateVars() {
        return {
            sourceConfig: this.sourceConfig,
            destConfig: this.destConfig,
            repConfig: this.repConfig,
            destHosts: this.destHosts,
            sourceHTTPAgent: this.sourceHTTPAgent,
            destHTTPAgent: this.destHTTPAgent,
            sourceVault: this.sourceVault,
            destVaults: this.destVaults,
            logger: this.logger,
        };
    }

    start() {
        const mProducer = new MetricsProducer(this.zkConfig, this.mConfig);
        mProducer.setupProducer(err => {
            if (err) {
                this.logger.error('error starting queue processor', {
                    error: err,
                });
                return;
            }
            this._mProducer = mProducer;
            const consumer = new BackbeatConsumer({
                zookeeper: { connectionString: this.zkConfig.connectionString },
                topic: this.repConfig.topic,
                groupId: this.repConfig.queueProcessor.groupId,
                concurrency: this.repConfig.queueProcessor.concurrency,
                queueProcessor: this.processKafkaEntry.bind(this),
                fetchMaxBytes: CONSUMER_FETCH_MAX_BYTES,
                mConfig: this.mConfig,
            });
            consumer.on('error', () => {});
            consumer.subscribe();

            consumer.on('sendMetrics', data => {
                // data = { ops, bytes }
                const batchSize = this._mProducer.getBatch();
                this._mProducer.send([], err => {
                    if (err) {
                        this.logger.error('error publishing metrics-specific '
                        + 'entries from log to topic', {
                            method: 'QueueProcessor.start',
                            topic: this.mConfig.topic,
                            entryCount: batchSize,
                            error: err,
                        });
                    }
                    this.logger.debug('entries published successfully to '
                    + 'topic', {
                        method: 'QueueProcessor.start',
                        topic: this.mConfig.topic,
                        entryCount: batchSize,
                    });
                });
            });

            this.logger.info('queue processor is ready to consume ' +
                             'replication entries');
        });
    }

    /**
     * Proceed to the replication of an object given a kafka
     * replication queue entry
     *
     * @param {object} kafkaEntry - entry generated by the queue populator
     * @param {string} kafkaEntry.key - kafka entry key
     * @param {string} kafkaEntry.value - kafka entry value
     * @param {function} done - callback function
     * @return {undefined}
     */
    processKafkaEntry(kafkaEntry, done) {
        const sourceEntry = QueueEntry.createFromKafkaEntry(kafkaEntry);
        if (sourceEntry.error) {
            this.logger.error('error processing source entry',
                              { error: sourceEntry.error });
            return process.nextTick(() => done(errors.InternalError));
        }

        // TODO: If type === 'metrics' && op === 'processed'
        // Skip the below or we will continue processing already
        // processed entries within metrics

        const task = (storageType => {
            switch (storageType) {
            case 'aws_s3':
                return new MultipleBackendTask(this);
            case 'metrics':
                return this._mProducer;
            default:
                return new QueueProcessorTask(this);
            }
        })(sourceEntry.getReplicationStorageType());

        return this.taskScheduler.push({ task, entry: sourceEntry },
        `${sourceEntry.getBucket()}/${sourceEntry.getObjectVersionedKey()}`,
        done);

        // OLD CODE:
        // return this.taskScheduler.push({
        //     task: sourceEntry.getReplicationStorageType() === 'aws_s3' ?
        //         new MultipleBackendTask(this) : new QueueProcessorTask(this),
        //     entry: sourceEntry,
        // },
        // `${sourceEntry.getBucket()}/${sourceEntry.getObjectVersionedKey()}`,
        // done);
    }
}

module.exports = QueueProcessor;
