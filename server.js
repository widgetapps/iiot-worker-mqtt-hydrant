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

let partBuffer = {'h': [], 'p': []};

console.log('Started on IP ' + config.ip + '. NODE_ENV=' + process.env.NODE_ENV);

client.on('error', function (error) {
    console.log('Error connecting to MQTT Server with username ' + config.mqttoptions.username + ' - ' + error);
    process.exit(1);
});

client.on('connect', function () {
    console.log('Connected to MQTT server.');
    // Subscribe to hydrant pubs
    client.subscribe(['+/v1/pressure', '+/v1/temperature', '+/v1/battery', '+/v1/reset', '+/v1/location', '+/v1/pressure-event', '+/v1/rssi', '+/v1/hydrophone']);
});

client.on('message', function (topic, message) {
    let [deviceId, version, type] = topic.split('/');

    let validTypes = ['pressure', 'temperature', 'battery','reset', 'location', 'pressure-event', 'rssi', 'hydrophone'];

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
                data.min     = decoded.min;
                data.max     = decoded.max;
                data.avg     = decoded.avg;
                data.point   = decoded.value;
                data.current = decoded.value;
                data.samples = decoded.n;
                handleData(amqp, data, deviceId);
                break;
            case 'temperature':
                data.sensorType = 2;
                data.min     = null;
                data.max     = null;
                data.avg     = null;
                data.point   = decoded.value;
                data.current = decoded.value;
                data.samples = null;
                handleData(amqp, data, deviceId);
                break;
            case 'battery':
                data.sensorType = 4;
                data.min     = null;
                data.max     = null;
                data.avg     = null;
                data.point   = decoded.value;
                data.current = decoded.value;
                data.samples = null;
                handleData(amqp, data, deviceId);
                break;
            case 'rssi':
                data.sensorType = 10;
                data.min     = decoded.min;
                data.max     = decoded.max;
                data.avg     = decoded.avg;
                data.point   = decoded.value;
                data.current = decoded.value;
                data.samples = decoded.n;
                handleData(amqp, data, deviceId);
                break;
            case 'reset':
                deviceResetLog(deviceId, decoded);
                break;
            case 'location':
                updateGeolocation(deviceId, decoded.latitude, decoded.longitude);
                break;
            case 'pressure-event':
                handlePartData('p', amqp, deviceId, decoded);
                break;
            case 'hydrophone':
                handlePartData('h', amqp, deviceId, decoded);
                break;
        }
    });
});

function handlePartData(type, amqp, deviceId, data) {
    let validTypes = ['h', 'p'];

    if (!validTypes.includes(type)) {
        return;
    }

    console.log('Part data of type ' + type + ' received. Part ' + data.part[0] + ' of ' + data.part[1];

    let date = new Date(data.date);
    let key = date.getTime() + deviceId;

    // Only one part, just process
    if (data.part[0] === 1 && data.part[1] === 1) {
        if (key in partBuffer[type]) {
            delete partBuffer[type][key];
        }

        partBuffer[type][key] = {
            parts: data.part[1],
            date: data.date,
            deviceId: deviceId,
            values: data.value
        };

        queuePartData(type, amqp, deviceId, key);

        return;
    }

    // If this is the first part, create the array element
    if (data.part[0] === 1) {
        console.log('First part received. Part ' + data.part[0] + ' with ' + data.value.length + ' values.');
        //console.log('KEY: ' + key);
        if (key in partBuffer[type]) {
            delete partBuffer[type][key];
        }

        partBuffer[type][key] = {
            parts: data.part[1],
            date: data.date,
            deviceId: deviceId,
            values: data.value
        };

        //console.log('Updated number of values: ' + pressureEventBuffer[key].values.length);

        return;
    }

    // If this is the last part, append and pub values
    if (data.part[0] === partBuffer[type][key].parts) {
        console.log('Last part received. Part ' + data.part[0] + ' with ' + data.value.length + ' values.');
        //console.log('KEY: ' + key);

        partBuffer[type][key].values = partBuffer[type][key].values.concat(data.value);

        //console.log('Updated number of values: ' + pressureEventBuffer[key].values.length);

        queuePartData(type, amqp, deviceId, key);

        return;
    }

    // If this is a middle part, just append
    if (data.part[0] < partBuffer[type][key].parts) {
        //console.log('Received part ' + data.part[0] + ' of ' + data.part[1] + ' parts with ' + data.value.length + ' values.');
        //console.log('KEY: ' + key);
        partBuffer[type][key].values = partBuffer[type][key].values.concat(data.value);

        //console.log('Updated number of values: ' + pressureEventBuffer[key].values.length);

        return;
    }
}

function queuePartData(type, amqp, deviceId, key) {
    // No key, do nothing
    if (!(key in partBuffer[type])) {
        return;
    }

    Device.findOne({ serialNumber: deviceId })
        .populate('client')
        .exec(function (err, device) {
            //console.log('Device queried: ' + deviceId);
            if (!device || err) {
                console.log('Device not found');
                return;
            }

            amqp.then (function(conn) {
                //console.log('AMQP connection established');
                return conn.createChannel();
            }).then (function(ch) {

                let assetPromise = Asset.findById(device.asset).populate('location').exec();

                assetPromise.then(function (asset) {
                    let docPromise = buildPartDocs(type, asset, device, key);

                    docPromise.then(function (documents) {
                        //console.log('Sending data to queue...');
                        let q = 'telemetry';
                        let ok = ch.assertQueue(q, {durable: true});
                        return ok.then(function() {

                            documents.forEach(function (document) {
                                ch.sendToQueue(q, Buffer.from(JSON.stringify(document)), {persistent: true});
                                //console.log(JSON.stringify(document));
                            });

                            // Done processing, delete the key
                            if (key in partBuffer[type]) {
                                delete partBuffer[type][key];
                            }

                            return ch.close();
                        }).catch(console.warn);
                    }).catch(console.warn);
                }).catch(console.warn);

            }).catch(console.warn);

        });
}

function buildPartDocs(type, asset, device, key) {

    let timestamps = Date.parse(partBuffer[type][key].date);

    let sampleRate;

    let sensorType = 1;
    if (type === 'h') {
        sensorType = 11;
    }

    if (isNaN(partBuffer[type][key].rate)) {
        sampleRate = 10;
    } else {
        sampleRate = partBuffer[type][key].rate;
    }

    let promise = Sensor.findOne({ type: sensorType }).exec();
    return promise.then(function (sensor) {

        let documents = [];
        let document;
        let timestamp;

        console.log('Building documents for buffer key ' + key);
        console.log('Number of documents to process: ' + partBuffer[type][key].values.length);

        for (let i=0; i < partBuffer[type][key].values.length; i++) {
            timestamp = new Date(timestamps);

            document = {
                timestamp: timestamp.toISOString(),
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
                        point: partBuffer[type][key].values[i],
                        current: partBuffer[type][key].values[i]
                    }
                }
            };

            documents.push(document);

            timestamps += sampleRate;
        }

        return documents;
    });
}

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

                    ch.sendToQueue(q, Buffer.from(JSON.stringify(document)), {persistent: true});
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
                    point: data.point,
                    current: data.point,
                    samples: data.samples
                }
            }
        };

        callback(document);

    });

}

function updateGeolocation(deviceId, latitude, longitude) {

    Device.findOneAndUpdate(
        { serialNumber: deviceId },
        {
            $set: {
                'geolocation.coordinates': [latitude, longitude]
            }
        },
        {new: true}, function(err, device) {
            if (!device || err) {
                console.log('No device found: ' + err);
                return;
            }

            if (device.location === null) return;

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

function deviceResetLog(deviceId, data) {
    Device.findOneAndUpdate(
        { serialNumber: deviceId },
        { $push: { resets: data  } },
        function (error, success) {
            if (error) {
                //console.log(error);
            } else {
                //console.log(success);
            }
        });
}

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
