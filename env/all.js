'use strict';

module.exports = {
    app: {
        title: 'terepac-worker-mqtt-hydrant',
        description: 'Full-Stack JavaScript with MongoDB, Express, AngularJS, and Node.js',
        keywords: 'MongoDB, Express, AngularJS, Node.js'
    },
    ip: process.env.IP || '127.0.0.1',
    mqtt: process.env.MQTT || 'mqtts://mqtt.terepac.one:8883',
    amqp: process.env.AMQP || 'amqp://localhost'
};