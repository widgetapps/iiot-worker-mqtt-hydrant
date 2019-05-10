'use strict';

let util = require('./util'),
    Device   = require('@terepac/terepac-models').Device,
    Asset    = require('@terepac/terepac-models').Asset,
    Sensor   = require('@terepac/terepac-models').Sensor;

exports.handleData = function(amqp, data, topicId, clientId) {
    // util.log_debug(clientId, 'Querying the deviceId ' + topicId);

    Device.findOne({ topicId: topicId })
        .populate('client')
        .exec(function (err, device) {
            // util.log_debug(clientId,'Device queried: ' + topicId);
            if (!device || err) {
                // console.log('Device not found');
                return;
            }

            // Save last transmission data to the  device.
            device.lastTransmission = {
                date: new Date(),
                data: data
            };

            device.save(function (err, savedDevice) {
                queueDatabase(amqp, device, data, clientId);
            });

        });
};

function queueDatabase (amqp, device, data, clientId) {

    // util.log_debug(clientId, 'Queueing data: ' + JSON.stringify(data));

    amqp.then (function(conn) {
        //console.log('AMQP connection established');
        return conn.createChannel();
    }).then (function(ch) {

        let assetPromise = Asset.findById(device.asset).populate('location').exec();

        assetPromise.then(function (asset) {
            //console.log('Sending data to queue...');

            if (!asset) {
                //console.log('Asset for device ' + device._id + ' not found: Asset ID' + device.asset);
                return;
            }

            let ex = 'telemetry';
            let ok = ch.assertExchange(ex, 'direct', {durable: true});
            return ok.then(function() {
                buildMessage(asset, device, data, clientId, function(document){

                    ch.publish(ex, 'telemetry', Buffer.from(JSON.stringify(document)), {persistent: true});
                    // util.log_debug(clientId, 'Data queued: ' + JSON.stringify(document));

                    return ch.close();
                });
            }).catch(console.warn);
        }).catch(console.warn);

    }).catch(console.warn);
}

function buildMessage (asset, device, data, clientId, callback) {

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

exports.updateGeolocation = function (topicId, latitude, longitude, clientId) {

    Device.findOneAndUpdate(
        { topicId: topicId },
        {
            $set: {
                updated: new Date(),
                'geolocation.coordinates': [latitude, longitude]
            }
        },
        {new: true}, function(err, device) {
            if (!device || err) {
                console.log('No device found: ' + err);
                return;
            }
        }
    );
};

exports.deviceResetLog = function (topicId, data, clientId) {
    // TODO: Need to decide what the max log entries will be. Maybe 10? Could be a setting.
    Device.findOneAndUpdate(
        { topicId: topicId },
        {
            updated: new Date(),
            $push: { resets: data  }
        },
        function (error, success) {
            if (error) {
                //console.log(error);
            } else {
                //console.log(success);
                // util.log_debug(clientId, 'Device reset data saved.');
            }
        });
};
