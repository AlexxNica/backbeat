'use strict'; // eslint-disable-line strict

const { EventEmitter } = require('events');

/**
 * Probe used for deep healthchecks to store data and catch event data
 *
 * @class
 */
class Probe extends EventEmitter {
    /**
     * @constructor
     * @param {string} id - uuid assigned to a probe
     */
    constructor(id) {
        super();
        this.id = id;
        this.store = null;
    }

    checkStore() {
        // get length of undefined values (still to be set)
        let count = 0;
        Object.keys(this.store).forEach(partition => {
            if (this.store[partition] === undefined) {
                count++;
            }
        });
        return count;
    }

    getId() {
        return this.id;
    }

    getStore() {
        return this.store;
    }

    setStoreData(partition, value) {
        this.store[partition] = value;
    }

    setStoreErrors() {
        for (const partition in this.store) {
            if (this.store[partition] === undefined) {
                this.store[partition] = 'error';
            }
        }
    }

    setStorePartition(partition) {
        this.store[partition] = 'ok';
    }

    setupStore(partitions) {
        this.store = partitions.reduce((store, partition) => {
            // eslint-disable-next-line
            store[partition] = undefined;
            return store;
        }, {});
    }
}

module.exports = Probe;
