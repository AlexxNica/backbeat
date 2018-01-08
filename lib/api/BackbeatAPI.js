'use strict'; // eslint-disable-line strict

const zookeeper = require('node-zookeeper-client');

const BackbeatProducer = require('../BackbeatProducer');
const Healthcheck = require('./Healthcheck');
const routes = require('./routes');

/**
 * Class representing Backbeat API endpoints and internals
 *
 * @class
 */
class BackbeatAPI {
    /**
     * @constructor
     * @param {object} config - configurations for setup
     * @param {werelogs.Logger} logger - Logger object
     */
    constructor(config, logger) {
        this._zkConfig = config.zookeeper;
        this._repConfig = config.extensions.replication;
        this._crrTopic = this._repConfig.topic;
        this._metricsTopic = config.metrics.topic;
        this._queuePopulator = config.queuePopulator;
        this._kafkaHost = config.kafka.hosts;
        this._logger = logger;

        this._crrProducer = null;
        this._metricProducer = null;
        this._healthcheck = null;
        this._zkClient = null;
        this._probe = null;
    }

    /**
     * Check if incoming request is valid
     * @param {string} route - request route
     * @return {boolean} true/false
     */
    isValidRoute(route) {
        return routes.map(route => route.path).includes(route);
    }

    /**
     * Check if Zookeeper and Producer are connected
     * @return {boolean} true/false
     */
    isConnected() {
        return this._zkClient.getState().name === 'SYNC_CONNECTED'
            && this._crrProducer.isReady() && this._metricProducer.isReady();
    }

    /**
     * Get Kafka healthcheck
     * @param {function} cb - callback(error, data)
     * @return {undefined}
     */
    healthcheck(cb) {
        return this._healthcheck.getHealthcheck((err, data) => {
            if (err) {
                this._logger.error('error getting healthcheck', {
                    error: err.message,
                });
                return cb(errors.InternalError);
            }
            return cb(null, data);
        });
    }

    /**
     * Get Kafka healthcheck
     * @param {function} cb - callback(error, data)
     * @return {undefined}
     */
    deepHealthcheck(cb) {
        return this._healthcheck.getDeepHealthcheck(cb);
    }

    /**
     * Setup internals
     * @param {function} cb - callback(error)
     * @return {undefined}
     */
    setupInternals(cb) {
        async.series([
            next => this._setZookeeper(next),
            next => this._setProducer(this._metricsTopic, (err, producer) => {
                if (err) {
                    return next(err);
                }
                this._metricProducer = producer;
                return next();
            }),
            next => this._setProducer(this._crrTopic, (err, producer) => {
                if (err) {
                    return next(err);
                }
                this._crrProducer = producer;
                return next();
            }),
        ], err => {
            if (err) {
                this._logger.error('error setting up internal clients');
                return cb(err);
            }
            this._healthcheck = new Healthcheck(this._repConfig, this._zkClient,
                this._crrProducer, this._metricProducer);
            this._logger.info('BackbeatAPI setup ready');
            return cb();
        });
    }

    _setProducer(topic, cb) {
        const producer = new BackbeatProducer({
            zookeeper: { connectionString: this._zkConfig.connectionString },
            topic,
        });

        producer.once('error', cb);
        producer.once('ready', () => {
            producer.removeAllListeners('error');
            producer.on('error', err => {
                this._logger.error('error from backbeat producer', {
                    error: err,
                });
                return cb(err);
            });
            return cb(null, producer);
        });
    }

    _setZookeeper(cb) {
        const populatorZkPath = this._queuePopulator.zookeeperPath;
        const zookeeperUrl =
            `${this._zkConfig.connectionString}${populatorZkPath}`;

        const zkClient = zookeeper.createClient(zookeeperUrl, {
            autoCreateNamespace: this._zkConfig.autoCreateNamespace,
        });
        zkClient.connect();

        zkClient.once('error', cb);
        zkClient.once('state', event => {
            if (event.name !== 'SYNC_CONNECTED' || event.code !== 3) {
                return cb('error setting up zookeeper');
            }
            zkClient.removeAllListeners('error');
            this._zkClient = zkClient;
            return cb();
        });
    }
}

module.exports = BackbeatAPI;
