'use strict';

require('./init')();

console.log('Version: ' + process.version);

let config = require('./config'),
    mongoose = require('mongoose'),
    _ = require('lodash'),
    mqtt = require('mqtt'),
    cbor = require('cbor'),
    microdate = require('./lib/microdate'),
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
    let [deviceId, version, type] = topic.split('/');

    // console.log('Message from device ' + deviceId + ' of type ' + type);

    let validTypes = ['pressure', 'temperature', 'battery','reset', 'location', 'pressure-event', 'rssi', 'hydrophone'];

    if (!_.includes(validTypes, type)) {
        return;
    }

    let decoder = cbor.Decoder({
        tags: { 30: (val) => {
                return [val[0], val[1]];
            }
        }
    });

    decoder.decodeFirst(message, function(err, decoded) {

        if (err !== null) {
            console.log('Error decoding CBOR: ' + err);
            return;
        }

        console.log('SAMPLE-RATE: ' + decoded['sample-rate']);

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
                handleData(amqp, data, deviceId);
                break;
            case 'temperature':
                data.sensorType = 2;
                data.min     = null;
                data.max     = null;
                data.avg     = null;
                data.point   = decoded.value;
                data.samples = null;
                handleData(amqp, data, deviceId);
                break;
            case 'battery':
                data.sensorType = 4;
                data.min     = null;
                data.max     = null;
                data.avg     = null;
                data.point   = decoded.value;
                data.samples = null;
                handleData(amqp, data, deviceId);
                break;
            case 'rssi':
                data.sensorType = 10;
                data.min     = decoded.min;
                data.max     = decoded.max;
                data.avg     = decoded.avg;
                data.point   = decoded.value;
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

    let timestamp = microdate.parseISOString(data.date.toISOString());

    // console.log('Part data of type ' + type + ' received. Part ' + data.part[0] + ' of ' + data.part[1]);

    // Convert date back to milliseconds to create new date, just for creating the part key
    let date = new Date(timestamp / 1000);
    let key = (date.getTime()) + deviceId;

    // Only one part, just process
    if (data.part[0] === 1 && data.part[1] === 1) {
        if (key in partBuffer[type]) {
            delete partBuffer[type][key];
        }

        partBuffer[type][key] = {
            parts: data.part[1],
            timestamp: timestamp,
            deviceId: deviceId,
            values: data.value
        };

        queuePartData(type, amqp, deviceId, key);

        return;
    }

    // If this is the first part, create the array element
    if (data.part[0] === 1) {
        // console.log('First part received. Part ' + data.part[0] + ' with ' + data.value.length + ' values.');
        //console.log('KEY: ' + key);
        if (key in partBuffer[type]) {
            delete partBuffer[type][key];
        }

        partBuffer[type][key] = {
            parts: data.part[1],
            timestamp: timestamp,
            deviceId: deviceId,
            values: data.value
        };

        //console.log('Updated number of values: ' + pressureEventBuffer[key].values.length);

        return;
    }

    // If this is the last part, append and pub values
    if (data.part[0] === partBuffer[type][key].parts) {
        // console.log('Last part received. Part ' + data.part[0] + ' with ' + data.value.length + ' values.');
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
                    let eventId = mongoose.Types.ObjectId();
                    let docPromise = buildPartDocs(type, asset, device, key, eventId);

                    docPromise.then(function (result) {
                        //console.log('Sending data to queue...');

                        let documents = result.documents;
                        let sensor = result.sensor;
                        let ex = 'telemetry';
                        let ok = ch.assertExchange(ex, 'direct', {durable: true});
                        return ok.then(function() {

                            documents.forEach(function (document) {
                                ch.publish(ex, 'event_telemetry', Buffer.from(JSON.stringify(document)), {persistent: true});
                                // console.log(JSON.stringify(document));
                            });

                            let eventdoc = {
                                _id: eventId,
                                start: new Date(documents[0].timestamp / 1000),
                                end: new Date(documents[documents.length - 1].timestamp / 1000),
                                count: documents.length,
                                description: sensor.typeString + ' event',
                                tag: {
                                    full: asset.location.tagCode + '_' + asset.tagCode + '_' + sensor.tagCode,
                                    clientTagCode: device.client.tagCode,
                                    locationTagCode: asset.location.tagCode,
                                    assetTagCode: asset.tagCode,
                                    sensorTagCode: sensor.tagCode
                                },
                                asset: asset._id,
                                device: device._id,
                                sensor: sensor._id,
                                client: device.client._id
                            };
                            ch.publish(ex, 'event', Buffer.from(JSON.stringify(eventdoc)), {persistent: true});
                            // console.log(eventdoc);

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

function buildPartDocs(type, asset, device, key, eventId) {

    // partBuffer[type][key].timestamp is in microseconds
    let timestamp = partBuffer[type][key].timestamp;

    let sampleRate;

    // Default sensor type to pressure (1)
    let sensorType = 1;
    if (type === 'h') {
        sensorType = 11;
    }

    // TODO: Gotta fix this sample rate
    // Convert partBuffer[type][key]['sample-rate'] to microseconds
    if (partBuffer[type][key]['sample-rate'] && isNaN(partBuffer[type][key]['sample-rate'][0]) && isNaN(partBuffer[type][key]['sample-rate'][1])) {
        // Default to 100 milliseconds, sampleRate is in microseconds
        sampleRate = 100000;
    } else {
        sampleRate = (1000 / (partBuffer[type][key]['sample-rate'][0] / partBuffer[type][key]['sample-rate'][1])) * 1000;
    }

    let promise = Sensor.findOne({ type: sensorType }).exec();
    return promise.then(function (sensor) {

        let documents = [];
        let document;

        // console.log('Building documents for buffer key ' + key);
        // console.log('Number of documents to process: ' + partBuffer[type][key].values.length);

        for (let i=0; i < partBuffer[type][key].values.length; i++) {

            document = {
                timestamp: timestamp,
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
                    _id: sensor._id,
                    type: sensor.type,
                    typeString: sensor.typeString,
                    description: sensor.description,
                    unit: sensor.unit
                },
                client: device.client._id,
                event: eventId,
                data: {
                    unit: sensor.unit,
                    value: partBuffer[type][key].values[i]
                }
            };

            documents.push(document);

            timestamp += sampleRate;
        }

        return {documents: documents, sensor: sensor};
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

    // console.log('Queueing data: ' + JSON.stringify(data));

    amqp.then (function(conn) {
        //console.log('AMQP connection established');
        return conn.createChannel();
    }).then (function(ch) {

        let assetPromise = Asset.findById(device.asset).populate('location').exec();

        assetPromise.then(function (asset) {
            //console.log('Sending data to queue...');

            let ex = 'telemetry';
            let ok = ch.assertExchange(ex, 'direct', {durable: true});
            return ok.then(function() {
                buildMessage(asset, device, data, function(document){

                    ch.publish(ex, 'telemetry', Buffer.from(JSON.stringify(document)), {persistent: true});
                    // console.log(JSON.stringify(document));

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
            timestamp: new Date(data.timestamp / 1000),
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
                _id: sensor._id,
                type: sensor.type,
                typeString: sensor.typeString,
                description: sensor.description,
                unit: sensor.unit
            },
            client: device.client._id,
            data: {
                unit: sensor.unit,
                values: {
                    min: data.min,
                    max: data.max,
                    average: data.avg,
                    point: data.point,
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
