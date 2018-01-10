/*
    This file contains all unique details about API routes exposed by Backbeats
    API server.
*/

// default Redis entry keys
// TODO: This should be moved to constants file. PR#126
const testIsOn = process.env.TEST_SWITCH === '1';
const OPS = testIsOn ? 'test:bb:ops' : 'bb:crr:ops';
const BYTES = testIsOn ? 'test:bb:bytes' : 'bb:crr:bytes';
const OPS_DONE = testIsOn ? 'test:bb:opsdone' : 'bb:crr:opsdone';
const BYTES_DONE = testIsOn ? 'test:bb:bytesdone' : 'bb:crr:bytesdone';

module.exports = [
    {
        path: '/_/healthcheck',
        method: 'getHealthcheck',
    },
    {
        path: '/_/metrics/backlog',
        method: 'getBacklog',
        dataPoints: [OPS, OPS_DONE, BYTES, BYTES_DONE],
    },
    {
        path: '/_/metrics/completions',
        method: 'getCompletions',
        dataPoints: [OPS_DONE, BYTES_DONE],
    },
    {
        path: '/_/metrics/throughput',
        method: 'getThroughput',
        dataPoints: [OPS_DONE, BYTES_DONE],
    },
    {
        path: '/_/metrics',
        method: 'getAllMetrics',
        dataPoints: [OPS, OPS_DONE, BYTES, BYTES_DONE],
    },
    {
        path: '/_/healthcheck/deep',
        method: 'getDeepHealthcheck',
    },
];
