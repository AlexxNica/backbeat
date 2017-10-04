'use strict'; // eslint-disable-line

const werelogs = require('werelogs');

const QueueProcessor = require('./QueueProcessor');
const MetricsProducer = require('../../../lib/MetricsProducer');

const config = require('../../../conf/Config');
const zkConfig = config.zookeeper;
const repConfig = config.extensions.replication;
const sourceConfig = repConfig.source;
const destConfig = repConfig.destination;
const mConfig = config.metrics;

const log = new werelogs.Logger('Backbeat:QueueProcessor:task');
werelogs.configure({ level: config.log.logLevel,
    dump: config.log.dumpLevel });

const metricsProducer = new MetricsProducer(zkConfig, mConfig);
metricsProducer.setupProducer(err => {
    if (err) {
        log.error('error starting metrics producer for queue processor', {
            error: err,
            method: 'MetricsProducer::setupProducer',
        });
        return undefined;
    }
    const queueProcessor = new QueueProcessor(zkConfig, sourceConfig,
        destConfig, repConfig, metricsProducer);
    return queueProcessor.start();
});
