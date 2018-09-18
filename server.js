'use strict';

let config = require('config'),
    mongoose = require('mongoose'),
    mqtt = require('mqtt');

mongoose.Promise = global.Promise;
mongoose.connect(config.db, function(err) {
    if (err) {
        console.log('Error connecting to MongoDB.');
    } else {
        console.log('Connected to MongoDB');
    }
});

let client  = mqtt.connect(config.mqtt, config.mqttoptions);

console.log('Started on IP ' + config.ip + '. NODE_ENV=' + process.env.NODE_ENV);

client.on('error', function (error) {
    console.log('Error connecting to MQTT Server with username ' + config.mqttoptions.username + ' - ' + error);
    process.exit(1);
});

client.on('connect', function () {
    console.log('Connected to MQTT server.');
    client.subscribe('telemetry');
});

client.on('message', function (topic, message) {
    console.log(topic);
    console.log(message);
});
