'use strict';

require('./init')();

console.log('Version: ' + process.version);

let config = require('./config'),
    mongoose = require('mongoose'),
    _ = require('lodash'),
    mqtt = require('mqtt'),
    cbor = require('cbor'),
    redis = require('redis'),
    microdate = require('./lib/microdate'),
    topicSinglepart = require('./lib/topic-singlepart'),
    topicMultipart = require('./lib/topic-multipart');

mongoose.Promise = global.Promise;
mongoose.connect(config.db, config.dbOptions, function(err) {
    if (err) {
        console.log('Error connecting to MongoDB.');
    } else {
        console.log('Connected to MongoDB');
    }
});

config.mqttoptions.clientId += '_' + process.pid;

let client  = mqtt.connect(config.mqtt, config.mqttoptions);
let amqp = require('amqplib').connect(config.amqp);
let redisClient = redis.createClient(config.redis);

console.log('Started on IP ' + config.ip + '. NODE_ENV=' + process.env.NODE_ENV);

redisClient.on('connect', function() {
    console.log('Redis client connected');
});

redisClient.on('error', function (err) {
    console.log('Error connecting to Redis server: ' + err);
});

client.on('error', function (error) {
    console.log('Error connecting to MQTT Server with username ' + config.mqttoptions.username + ' - ' + error);
    process.exit(1);
});

client.on('connect', function () {
    console.log('Connected to MQTT server: ' + config.mqtt);
    // Subscribe to hydrant pubs, use $share/workers/ prefix to enable round robin shared subscription
    client.subscribe([
        '$share/workers/+/v1/pressure',
        '$share/workers/+/v1/temperature',
        '$share/workers/+/v1/battery',
        '$share/workers/+/v1/reset',
        '$share/workers/+/v1/location',
        '$share/workers/+/v1/pressure-event',
        '$share/workers/+/v1/rssi',
        '$share/workers/+/v1/hydrophone'
    ], {qos: 2});
    client.subscribe([
        '+/v1/pressure',
        '+/v1/temperature',
        '+/v1/battery',
        '+/v1/reset',
        '+/v1/location',
        '+/v1/pressure-event',
        '+/v1/rssi',
        '+/v1/hydrophone'
    ], {qos: 2});
    client.subscribe([
        '$queue/+/v1/pressure',
        '$queue/+/v1/temperature',
        '$queue/+/v1/battery',
        '$queue/+/v1/reset',
        '$queue/+/v1/location',
        '$queue/+/v1/pressure-event',
        '$queue/+/v1/rssi',
        '$queue/+/v1/hydrophone'
    ], {qos: 2});
});

client.on('reconnect', function () {
   console.log('Reconnecting to MQTT server...');
});

client.on('close', function () {
    console.log('MQTT connection closed.');
});

client.on('offline', function () {
    console.log('MQTT client went offline.');
});

client.on('message', function (topic, message) {
    let [topicId, version, type] = topic.split('/');

    console.log('Message received, topic is: ' + topic);

    return;

    //console.log('Message from device ' + deviceId + ' of type ' + type);

    let validTypes = ['pressure', 'temperature', 'battery','reset', 'location', 'pressure-event', 'rssi', 'hydrophone'];

    if (!_.includes(validTypes, type)) {
        return;
    }

    let cborOptions ={
        tags: { 30: (val) => {
                return val;
            }
        }
    };

    cbor.decodeFirst(message, cborOptions, function(err, decoded) {

        if (err !== null) {
            console.log('Error decoding CBOR: ' + err);
            return;
        }

        let data = {
            timestamp: microdate.parseISOString(decoded.date.toISOString())
        };

        switch (type) {
            case 'pressure':
                data.sensorType = 1;
                data.min     = decoded.min;
                data.max     = decoded.max;
                data.avg     = decoded.avg;
                data.point   = decoded.value;
                data.samples = decoded.n;
                topicSinglepart.handleData(amqp, data, topicId);
                break;
            case 'temperature':
                data.sensorType = 2;
                data.min     = null;
                data.max     = null;
                data.avg     = null;
                data.point   = decoded.value;
                data.samples = null;
                topicSinglepart.handleData(amqp, data, topicId);
                break;
            case 'battery':
                data.sensorType = 4;
                data.min     = null;
                data.max     = null;
                data.avg     = null;
                data.point   = decoded.value;
                data.samples = null;
                topicSinglepart.handleData(amqp, data, topicId);
                break;
            case 'rssi':
                data.sensorType = 10;
                data.min     = decoded.min;
                data.max     = decoded.max;
                data.avg     = decoded.avg;
                data.point   = decoded.value;
                data.samples = decoded.n;
                topicSinglepart.handleData(amqp, data, topicId);
                break;
            case 'reset':
                topicSinglepart.deviceResetLog(topicId, decoded);
                break;
            case 'location':
                topicSinglepart.updateGeolocation(topicId, decoded.latitude, decoded.longitude);
                break;
            case 'pressure-event':
                topicMultipart.handlePartData('p', amqp, topicId, decoded);
                break;
            case 'hydrophone':
                topicMultipart.handlePartData('h', amqp, topicId, decoded);
                break;
        }
    });
});

/**
 * Handle the different ways an application can shutdown
 */

function handleAppExit (options, err) {
    if (err) {
        console.log('App Exit Error: ' + err.stack);
    }

    if (options.cleanup) {
        // Cleanup
    }

    if (options.exit) {
        process.exit();
    }
}

process.on('exit', handleAppExit.bind(null, {
    cleanup: true
}));

process.on('SIGINT', handleAppExit.bind(null, {
    exit: true
}));

process.on('uncaughtException', handleAppExit.bind(null, {
    exit: true
}));
