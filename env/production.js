'use strict';

module.exports = {
    db: 'mongodb://10.240.162.13,10.240.253.155/one-platform',
    mqttoptions: {
        clientId: 'worker_primary',
        username: 'worker',
        password: process.env.MQTT_PASSWORD || ''
    }
};
