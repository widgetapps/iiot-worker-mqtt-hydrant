'use strict';

let util = require('./util'),
    mongoose = require('mongoose'),
    microdate = require('./microdate'),
    _ = require('lodash'),
    Device   = require('@terepac/terepac-models').Device,
    Asset    = require('@terepac/terepac-models').Asset,
    Sensor   = require('@terepac/terepac-models').Sensor;

const {promisify} = require('util');

exports.handlePartData = function (redisClient, type, amqp, topicId, data, clientId) {

    // util.log_debug(clientId, 'Part data of type ' + type + ' received. Part ' + data.part[0] + ' of ' + data.part[1]);

    let timestamp = microdate.parseISOString(data.date.toISOString());

    // Convert date back to milliseconds to create new date, just for creating the part key
    let date = new Date(timestamp / 1000);
    let key = date.getTime().toString() + topicId;
    // console.log('BUILDING KEY FOR PART '+ data.part[0] + ' OF ' + data.part[1] + ': ' + date.getTime().toString() + ' ' + deviceId + ' -- ' + key);

    let dataHeader = {
        samplerate: data['sample-rate'],
        timestamp: timestamp
    };

    redisClient.hexists(key, 'header', function (err, header) {
        // util.log_debug(clientId, 'Does the header exist for key ' + key + '? ' + header);
        if (header === 0) {
            redisClient.hset(key, 'header', JSON.stringify(dataHeader));
        }

        redisClient.hset(key, 'field_' + data.part[0], JSON.stringify(data.value));

        redisClient.hlen(key, function (err, length) {
            if (length === data.part[1]) {
                //util.log_debug(clientId, 'All parts in REDIS key ' + key + ', time to process...');
                queuePartData(redisClient, length, key, type, topicId, amqp, clientId);
            }
        });
    });
};

function queuePartData (redisClient, hashLength, key, type, topicId, amqp, clientId) {

    // TODO: Update this to use the deviceId field and not serialNumber
    Device.findOne({ topicId: topicId })
        .populate('client')
        .exec(function (err, device) {
            // util.log_debug(clientId, 'Device topicId queried: ' + topicId);
            if (!device || err) {
                console.log('Device not found');
                return;
            }

            amqp.then (function(conn) {
                return conn.createChannel();
            }).then (function(ch) {

                let assetPromise = Asset.findById(device.asset).populate('location').exec();

                assetPromise.then(function (asset) {
                    let eventId = mongoose.Types.ObjectId();
                    let docPromise = buildPartDocs(redisClient, hashLength, key, type, asset, device, eventId, clientId);

                    docPromise.then(function (result) {

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

                            redisClient.hgetall(key, function (err, fields) {
                                _.forEach(fields, function (value, arrayKey) {
                                    redisClient.hdel(key, arrayKey);
                                });

                                return ch.close();
                            });
                        }).catch(console.warn);
                    }).catch(console.warn);
                }).catch(console.warn);

            }).catch(console.warn);

        });
}

function buildPartDocs (redisClient, hashLength, key, type, asset, device, eventId, clientId) {
    const hgetAsync = promisify(redisClient.hget).bind(redisClient);
    const hgetallAsync = promisify(redisClient.hgetall).bind(redisClient);

    let timestamp,
        sampleRate,
        values = [];
    let sensorType = 1;

    if (type === 'h') {
        sensorType = 11;
    }

    return hgetAsync(key, 'header').then(function (header) {
        // util.log_debug(clientId, 'Header found:' + header);
        header = JSON.parse(header);
        timestamp = header.timestamp;
        sampleRate = (1000 / (header.samplerate[0] / header.samplerate[1])) * 1000;

        let unorderedValues = {};
        let sortedValues = [];

        return hgetallAsync(key).then(function (data) {

            _.forEach(data, function (value, arrayKey) {
                if (arrayKey !== 'header') {
                    unorderedValues[arrayKey] = value;
                }
            });

            _.forEach(unorderedValues, function (value, key) {
                let index = parseInt(key.substr(6)) - 1;
                sortedValues[index] = JSON.parse(value);
            });

            for (let i = 0; i < sortedValues.length; i++) {
                values = values.concat(sortedValues[i]);
            }

            // util.log_debug(clientId, 'Final value array contains ' + values.length + ' elements and contains ' + values);

            let promise = Sensor.findOne({ type: sensorType }).exec();
            return promise.then(function (sensor) {

                // util.log_debug(clientId, 'Sensor found: ' + sensor._id);

                let documents = [];
                let document;

                for (let i=0; i < values.length; i++) {

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
                            value: values[i]
                        }
                    };

                    documents.push(document);

                    timestamp += sampleRate;
                }

                // util.log_debug(clientId, 'Number of documents to queue: ' + documents.length);

                return {documents: documents, sensor: sensor};
            });
        });
    });
}
