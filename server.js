'use strict';

require('./init')();

console.log('Version: ' + process.version);

let config = require('./config'),
    mongoose = require('mongoose'),
    _ = require('lodash'),
    mqtt = require('mqtt'),
    cbor = require('cbor'),
    Device   = require('@terepac/terepac-models').Device,
    Asset    = require('@terepac/terepac-models').Asset,
    Sensor   = require('@terepac/terepac-models').Sensor,
    Location = require('@terepac/terepac-models').Location;

mongoose.Promise = global.Promise;
mongoose.connect(config.db, config.dbOptions, function(err) {
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
            timestamp: new Date(decoded.date)
        };

        switch (type) {
            case 'pressure':
                data.sensorType = 1;
                data.min = decoded.value;
                data.max = decoded.value;
                data.avg = decoded.value;
                data.point = decoded.value;
                handleData(amqp, data, deviceId);
                break;
            case 'temperature':
                data.sensorType = 2;
                data.min = decoded.value;
                data.max = decoded.value;
                data.avg = decoded.value;
                data.point = decoded.value;
                handleData(amqp, data, deviceId);
                break;
            case 'battery':
                data.sensorType = 4;
                data.min = decoded.value;
                data.max = decoded.value;
                data.avg = decoded.value;
                data.point = decoded.value;
                handleData(amqp, data, deviceId);
                break;
            case 'reset':
                break;
            case 'location':
                updateGeolocation(deviceId, decoded.latitude, decoded.longitude);
                break;
            case 'pressure-event':
                break;
        }
    });
});

function handleData(amqp, data, deviceId) {
    //console.log('Querying the deviceId ' + deviceId);

    Device.findOne({ serialNumber: deviceId })
        .populate('client')
        .exec(function (err, device) {
            //console.log('Device queried: ' + deviceId);
            if (!device || err) {
                console.log('Device not found');
                return;
            }

            queueDatabase(amqp, device, data);
        });
}


function queueDatabase(amqp, device, data) {
    //console.log('Queueing data: ' + JSON.stringify(data));
    amqp.then (function(conn) {
        //console.log('AMQP connection established');
        return conn.createChannel();
    }).then (function(ch) {

        let assetPromise = Asset.findById(device.asset).populate('location').exec();

        assetPromise.then(function (asset) {
            //console.log('Sending data to queue...');
            let q = 'telemetry';
            let ok = ch.assertQueue(q, {durable: true});
            return ok.then(function() {
                buildMessage(asset, device, data, function(document){

                    ch.sendToQueue(q, new Buffer(JSON.stringify(document)), {persistent: true});
                    //console.log(JSON.stringify(document));

                    return ch.close();
                });
            }).catch(console.warn);
        }).catch(console.warn);

    }).catch(console.warn);
}

function buildMessage(asset, device, data, callback) {

    let promise = Sensor.findOne({ type: data.sensorType }).exec();
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

        callback(document);

    });

}

function updateGeolocation(deviceId, latitude, longitude) {

    Device.update(
        { serialNumber: deviceId },
        {
            $set: {
                'geolocation.coordinates': [latitude, longitude]
            }
        }, function(err, device) {
            if (!device || err) {
                console.log('No device found: ' + err);
                return;
            }
            Location.findByIdAndUpdate(device.location,
                {
                    $set: {
                        'geolocation.coordinates': [latitude, longitude]
                    }
                }, function (err, location) {
                    if (!location || err) {
                        console.log('Error updating the location coordinates: ' + err);
                    }
                }
            )
        }
    );
}

/**
 * Handle the different ways an application can shutdown
 */

function handleAppExit (options, err) {
    if (err) {
        console.log(err.stack);
    }

    if (options.cleanup) {
        // Cleamup
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
