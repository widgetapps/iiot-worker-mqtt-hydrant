'use strict';

module.exports = {
    db: process.env.MONGO_STRING || 'mongodb://10.240.162.13,10.240.253.155/one-platform?replicaSet=rs0',
    dbOptions: { useNewUrlParser: true,  useCreateIndex: true, useFindAndModify: false },
    mqtt: process.env.MQTT || 'mqtts://10.240.0.16:8883',
    mqttoptions: {
        clientId: 'worker_hydrant',
        username: 'worker',
        keepalive: 60,
        clean: true,
        password: process.env.MQTT_PASSWORD || ''
    }
};
