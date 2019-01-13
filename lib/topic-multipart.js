'use strict';

let util = require('./util'),
    mongoose = require('mongoose'),
    microdate = require('./microdate'),
    Device   = require('@terepac/terepac-models').Device,
    Asset    = require('@terepac/terepac-models').Asset,
    Sensor   = require('@terepac/terepac-models').Sensor;

exports.handlePartData = function (redisClient, type, amqp, topicId, data, clientId) {

    util.log_debug(clientId, 'Part data of type ' + type + ' received. Part ' + data.part[0] + ' of ' + data.part[1]);
    return;

    let timestamp = microdate.parseISOString(data.date.toISOString());

    // Convert date back to milliseconds to create new date, just for creating the part key
    let date = new Date(timestamp / 1000);
    let key = date.getTime().toString() + topicId;
    // console.log('BUILDING KEY FOR PART '+ data.part[0] + ' OF ' + data.part[1] + ': ' + date.getTime().toString() + ' ' + deviceId + ' -- ' + key);

    let header = {
        samplerate: data['sample-rate'],
        timestamp: timestamp
    };

    if (!redisClient.hexists(key, 'header')) {
        redisClient.hset(key, 'header', JSON.stringify(header));
    }
    redisClient.hset(key, 'field_' + data.part[0], data.value);

    if (redisClient.hlen(key) === data.part[1]) {
        queuePartData(redisClient, key, type, topicId, amqp, clientId);
    }
};

function queuePartData (redisClient, key, type, topicId, amqp, clientId) {

    // TODO: Update this to use the deviceId field and not serialNumber
    Device.findOne({ topicId: topicId })
        .populate('client')
        .exec(function (err, device) {
            console.log('Device topicId queried: ' + topicId);
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
                    let docPromise = buildPartDocs(redisClient, key, type, asset, device, eventId, clientId);

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

function buildPartDocs (redisClient, key, type, asset, device, eventId, clientId) {

    let timestamp,
        sampleRate,
        header,
        values = [];
    let sensorType = 1;

    if (type === 'h') {
        sensorType = 11;
    }

    header = JSON.parse(redisClient.hget(key, 'header'));
    timestamp = header.timestamp;
    sampleRate = (1000 / (header.samplerate[0] / header.samplerate[1])) * 1000;

    let hashLength = redisClient.hlen(key);

    for(let field = 1; field <= hashLength; field++) {
        values = values.concat(redisClient.hget(key, 'field_' + field));
    }

    let promise = Sensor.findOne({ type: sensorType }).exec();
    return promise.then(function (sensor) {

        let documents = [];
        let document;

        // console.log('Building documents for buffer key ' + key);
        // console.log('Number of documents to process: ' + partBuffer[type][key].values.length);

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

        return {documents: documents, sensor: sensor};
    });
}
