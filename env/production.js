'use strict';

module.exports = {
    db: 'mongodb://10.240.162.13,10.240.253.155/one-platform',
    dbOptions: { useNewUrlParser: true },
    mqtt: process.env.MQTT || 'mqtts://10.240.0.16:8883',
    mqttoptions: {
        clientId: 'worker_hydrant',
        username: 'worker',
        keepalive: 0,
        password: process.env.MQTT_PASSWORD || ''
    }
};
