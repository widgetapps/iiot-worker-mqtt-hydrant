'use strict';

require('./init')();

console.log('Node Version: ' + process.version);

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

// Generate unique MQTT Client ID for each server & CPU/instance
let appInstance = '';
if (process.env.NODE_APP_INSTANCE) {
    appInstance = '_' + process.env.NODE_APP_INSTANCE;
}
config.mqttoptions.clientId += '_' + process.env.IP + appInstance;

mongoose.Promise = global.Promise;
mongoose.connect(config.db, config.dbOptions, function(err) {
    if (err) {
        util.log_debug(config.mqttoptions.clientId, 'Error connecting to MongoDB.');
    } else {
        util.log_debug(config.mqttoptions.clientId, 'Connected to MongoDB');
    }
});

let client  = mqtt.connect(config.mqtt, config.mqttoptions);
let amqp = require('amqplib').connect(config.amqp);
let redisClient = redis.createClient(config.redis);

util.log_debug(config.mqttoptions.clientId, 'Started on IP ' + config.ip + '. NODE_ENV=' + process.env.NODE_ENV);

redisClient.on('connect', function() {
    util.log_debug(config.mqttoptions.clientId, 'Redis client connected');
});

redisClient.on('error', function (err) {
    util.log_debug(config.mqttoptions.clientId, 'Error connecting to Redis server: ' + err);
});

client.on('error', function (error) {
    util.log_debug(config.mqttoptions.clientId, 'Error connecting to MQTT Server with username ' + config.mqttoptions.username + ' - ' + error);
    process.exit(1);
});

client.on('connect', function (connack) {

    /*
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
    */

    util.log_debug(config.mqttoptions.clientId, 'Connected to MQTT server: ' + config.mqtt);

    //util.log_debug(config.mqttoptions.clientId, JSON.stringify(connack));

    // Subscribe to hydrant pubs, use $share/workers/ prefix to enable round robin/random shared subscription
    client.subscribe([
        '$share/workers_hydrant/+/v1/pressure',
        '$share/workers_hydrant/+/v1/temperature',
        '$share/workers_hydrant/+/v1/battery',
        '$share/workers_hydrant/+/v1/reset',
        '$share/workers_hydrant/+/v1/location',
        '$share/workers_hydrant/+/v1/pressure-event',
        '$share/workers_hydrant/+/v1/rssi',
        '$share/workers_hydrant/+/v1/hydrophone',
        '$share/workers_hydrant/+/v1/hydrophone-summary'
    ], {qos: 2});

});

client.on('reconnect', function () {
    util.log_debug(config.mqttoptions.clientId, 'Reconnecting to MQTT server...');
});

client.on('close', function () {
    util.log_debug(config.mqttoptions.clientId, 'MQTT connection closed.');
});

client.on('offline', function () {
    util.log_debug(config.mqttoptions.clientId, 'MQTT client went offline.');
});

client.on('message', function (topic, message) {
    let [topicId, version, type] = topic.split('/');

    // util.log_debug(config.mqttoptions.clientId, 'Message received, topic is: ' + topic);

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
                data.min     = null;
                data.max     = null;
                data.avg     = null;
                data.point   = decoded.rms;
                data.samples = decoded.n;
                topicSinglepart.handleData(amqp, data, topicId, config.mqttoptions.clientId);
                // let logDate = new Date();
                // console.log('[MQTT-RMS] ' + logDate.toUTCString() + ' ' + JSON.stringify(decoded));
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
    client.end();

    if (err) {
        console.log(Math.floor(Date.now()/1000) + ': App Exit Error: ' + JSON.stringify(err));
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
