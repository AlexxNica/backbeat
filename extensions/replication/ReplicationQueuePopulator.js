const { isMasterKey } = require('arsenal/lib/versioning/Version');

const QueuePopulatorExtension =
          require('../../lib/queuePopulator/QueuePopulatorExtension');
const QueueEntry = require('./utils/QueueEntry');

class ReplicationQueuePopulator extends QueuePopulatorExtension {
    constructor(params) {
        super(params);
        this.repConfig = params.config;
    }

    filter(entry) {
        if (entry.type !== 'put' || isMasterKey(entry.key)) {
            return;
        }
        const value = JSON.parse(entry.value);
        const queueEntry = new QueueEntry(entry.bucket, entry.key, value);
        const sanityCheckRes = queueEntry.checkSanity();
        if (sanityCheckRes) {
            return;
        }
        if (queueEntry.getReplicationStatus() !== 'PENDING') {
            return;
        }
        this.publish(this.repConfig.topic,
                     `${queueEntry.getBucket()}/${queueEntry.getObjectKey()}`,
                     JSON.stringify(entry));
    }
}

module.exports = ReplicationQueuePopulator;