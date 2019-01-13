'use strict';

exports.log_debug = function (clientId, message) {
    let date = new Date();
    console.log(date.toISOString() + ' ' + clientId + ' ' + message);
};
