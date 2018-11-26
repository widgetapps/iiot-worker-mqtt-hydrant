'use strict';

exports.parseISOString = function (dateString) {
    let date = new Date(dateString);
    let milliseconds = date.getTime();

    // Normalize the provided date string to microseconds.
    let microseconds = milliseconds * 1000;

    let microSecondStringArray = dateString.match(/\.\d{3}(\d{1,3})Z$/);
    //console.log(microSecondStringArray);
    if (microSecondStringArray !== null) {
        switch (microSecondStringArray[1].length) {
            case 1:
                microseconds += parseInt(microSecondStringArray[1]) * 100;
                break;
            case 2:
                microseconds += parseInt(microSecondStringArray[1]) * 10;
                break;
            case 3:
                microseconds += parseInt(microSecondStringArray[1]);
                break;
        }
    }
    return microseconds;
};

exports.toISOString = function (microseconds) {
    let date = new Date(microseconds / 1000);
    let dateString = date.toISOString();

    // Cast ms to zero padded 3 character string.
    let msString = date.getMilliseconds().toString().padStart(3, '0');
    let microString = '.' + msString + microseconds.toString().substr(microseconds.toString().length - 3) + 'Z';

    return dateString.replace(/.\d(\d{0,3})Z$/, microString);
};
