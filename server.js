'use strict';

require('./init')();

console.log('Version: ' + process.version);

let config = require('./config'),
    util = require('./lib/util'),
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

config.mqttoptions.clientId += '_' + process.pid + '.' + Math.floor(Math.random() * 1000);

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

    /*
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
    */

    client.subscribe([
        '+/v1/pressure',
        '+/v1/temperature',
        '+/v1/battery',
        '+/v1/reset',
        '+/v1/location',
        '+/v1/pressure-event',
        '+/v1/rssi',
        '+/v1/hydrophone',
        '+/v1/hydrophone-summary'
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

    util.log_debug(config.mqttoptions.clientId, 'Message received, topic is: ' + topic);

    // util.log_debug(config.mqttoptions.clientId, 'Message from topic ' + topicId + ' of type ' + type);

    let validTypes = ['pressure', 'temperature', 'battery','reset', 'location', 'pressure-event', 'rssi', 'hydrophone', 'hydrophone-summary'];

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
                topicSinglepart.handleData(amqp, data, topicId, config.mqttoptions.clientId);
                break;
            case 'temperature':
                data.sensorType = 2;
                data.min     = null;
                data.max     = null;
                data.avg     = null;
                data.point   = decoded.value;
                data.samples = null;
                topicSinglepart.handleData(amqp, data, topicId, config.mqttoptions.clientId);
                break;
            case 'battery':
                data.sensorType = 4;
                data.min     = null;
                data.max     = null;
                data.avg     = null;
                data.point   = decoded.value;
                data.samples = null;
                topicSinglepart.handleData(amqp, data, topicId, config.mqttoptions.clientId);
                break;
            case 'rssi':
                data.sensorType = 10;
                data.min     = decoded.min;
                data.max     = decoded.max;
                data.avg     = decoded.avg;
                data.point   = decoded.value;
                data.samples = decoded.n;
                topicSinglepart.handleData(amqp, data, topicId, config.mqttoptions.clientId);
                break;
            case 'hydrophone-summary':
                data.sensorType = 12;
                data.min     = decoded.min;
                data.max     = decoded.max;
                data.avg     = decoded.avg;
                data.point   = decoded.value;
                data.samples = decoded.n;
                topicSinglepart.handleData(amqp, data, topicId, config.mqttoptions.clientId);
                break;
            case 'reset':
                topicSinglepart.deviceResetLog(topicId, decoded, config.mqttoptions.clientId);
                break;
            case 'location':
                topicSinglepart.updateGeolocation(topicId, decoded.latitude, decoded.longitude, config.mqttoptions.clientId);
                break;
            case 'pressure-event':
                // util.log_debug(config.mqttoptions.clientId, 'Pressure event detected: ' + decoded.part[0] + ' of ' + decoded.part[1]);
                topicMultipart.handlePartData(redisClient, 'p', amqp, topicId, decoded, config.mqttoptions.clientId);
                break;
            case 'hydrophone':
                topicMultipart.handlePartData(redisClient, 'h', amqp, topicId, decoded, config.mqttoptions.clientId);
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
