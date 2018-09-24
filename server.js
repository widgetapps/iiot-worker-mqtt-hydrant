'use strict';

require('./init')();

console.log('Version: ' + process.version);

let config = require('./config'),
    mongoose = require('mongoose'),
    _ = require('lodash'),
    mqtt = require('mqtt'),
    cbor = require('cbor'),
    Device = require('@terepac/terepac-models').Device,
    Asset = require('@terepac/terepac-models').Asset,
    Sensor = require('@terepac/terepac-models').Sensor;

mongoose.Promise = global.Promise;
mongoose.connect(config.db, function(err) {
    if (err) {
        console.log('Error connecting to MongoDB.');
    } else {
        console.log('Connected to MongoDB');
    }
});

let client  = mqtt.connect(config.mqtt, config.mqttoptions);
let amqp = require('amqplib').connect(config.amqp);

console.log('Started on IP ' + config.ip + '. NODE_ENV=' + process.env.NODE_ENV);

client.on('error', function (error) {
    console.log('Error connecting to MQTT Server with username ' + config.mqttoptions.username + ' - ' + error);
    process.exit(1);
});

client.on('connect', function () {
    console.log('Connected to MQTT server.');
    // Subscribe to hydrant pubs
    client.subscribe(['+/v1/pressure', '+/v1/temperature', '+/v1/battery', '+/v1/reset', '+/v1/location', '+/v1/pressure-event']);
});

client.on('message', function (topic, message) {
    let [deviceId, version, type] = topic.split('/');

    let validTypes = ['pressure', 'temperature', 'battery','reset', 'location', 'pressure-event'];

    if (!_.includes(validTypes, type)) {
        return;
    }

    cbor.decodeFirst(message, function(err, decoded) {

        if (err !== null) {
            console.log('Error decoding CBOR: ' + err);
            return;
        }

        let data = {
            date: new Date(decoded.date)
        };

        switch (type) {
            case 'pressure':
                data.sensorType = 1;
                break;
            case 'temperature':
                data.sensorType = 2;
                break;
            case 'battery':
                console.log('Device ID: ' + deviceId);
                console.log('Version: ' + version);
                console.log('Type: ' + type);

                console.log(JSON.stringify(decoded));
                data.sensorType = 4;
                data.min = decoded.value;
                data.max = decoded.value;
                data.avg = decoded.value;
                data.point = decoded.value;
                break;
            case 'reset':
                break;
            case 'location':
                break;
            case 'pressure-event':
                break;
        }

        var devicePromise = Device.findOne({ serialNumber: deviceId }).populate('client').exec();
        devicePromise.then(function (device){
            if (device === null) {
                console.log('Device not found, serialNumber ' + deviceId);
                return;
            }

            queueDatabase(amqp, device, data);
        });
    });
});

function queueDatabase(amqp, device, data) {
    amqp.then (function(conn) {
        console.log('AMQP connection established');
        return conn.createChannel();
    }).then (function(ch) {

        var assetPromise = Asset.findOne({ _id: device.asset }).populate('location').exec();

        assetPromise.then(function (asset) {
            onsole.log('Sending data to queue...');
            var q = 'telemetry';
            var ok = ch.assertQueue(q, {durable: true});
            return ok.then(function() {
                let document = buildMessage(asset, device, data);

                //ch.sendToQueue(q, new Buffer(JSON.stringify(document)), {persistent: true});
                console.log(JSON.stringify(document));

                return ch.close();
            }).catch(console.warn);
        }).catch(console.warn);

    }).catch(console.warn);
}

function buildMessage(asset, device, data) {

    var promise = Sensor.findOne({ type: data.sensorType }).exec();
    promise.then(function (sensor) {

        let document = {
            timestamp: data.timestamp,
            tag: {
                full: asset.location.tagCode + '_' + asset.tagCode + '_' + sensor.tagCode,
                clientTagCode: device.client.tagCode,
                locationTagCode: asset.location.tagCode,
                assetTagCode: asset.tagCode,
                sensorTagCode: sensor.tagCode
            },
            asset: {
                _id: asset._id,
                tagCode: asset.tagCode,
                name: asset.name,
                description: asset.description,
                location: {
                    tagCode: asset.location.tagCode,
                    description: asset.location.description,
                    geolocation: asset.location.geolocation.coordinates
                }
            },
            device: {
                _id: device._id,
                serialNumber: device.serialNumber,
                type: device.type,
                description: device.description
            },
            sensor: {
                type: sensor.type,
                typeString: sensor.typeString,
                description: sensor.description,
                unit: sensor.unit
            },
            client: device.client._id,
            data: {
                _id: sensor._id,
                unit: sensor.unit,
                values: {
                    min: data.min,
                    max: data.max,
                    average: data.avg,
                    point: data.point
                }
            }
        };

        return document;

    });

}