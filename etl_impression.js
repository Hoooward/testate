'use strict';
const aws = require('aws-sdk');
const s3 = new aws.S3({ apiVersion: '2006-03-01' });
const moment = require('moment-timezone');
const numeral = require('numeral');


var etl = function(data) {

    //console.log(data.Body.toString());

    var dataBody = data.Body.toString().split('\n');

    //ETL the Data
    var dataSaveArray = [];

    for (var i = 0; i < dataBody.length; i++) {

        if (dataBody[i] == "")
            continue;

        var r;

        try {
            r = JSON.parse(dataBody[i]);
        } catch (e) {
            console.log(e);
            console.log(dataBody[i]);
            continue;
        }

        if (r['logtype'] == 'c') {
            var saveRecord = { idfa: r['params']['idfa'], ydofid: r['ydofid'], ydosid: r['ydosid'], ydckid: r['ydckid'], yodasc: r['yodasc'], ts: r['ts'] };
            dataSaveArray.push(saveRecord);
        }

    }

    return dataSaveArray;

}


var buildPath = function(time) {

    //console.log(time);

    var beijingTime = moment.tz(time, 'Asia/Shanghai').tz('Asia/Shanghai');

    //console.log(beijingTime.format());

    var mm = beijingTime.format('mm');// numeral(parseInt(parseInt(beijingTime.format('mm')) / 10) * 10).format('00');

    var path = beijingTime.format('YYYYMMDD/HH') + mm;

    //console.log(path);

    return path;

}

var buildBody = function(items) {

    var body = "";

    for (var item of items) {

        body = body + JSON.stringify(item) + '\n';
    }

    return body;

}




async function etlOnePiece(time) {

    console.log('ETL Time : ',time);

    var bucket = 'com.yodamob.adserver.track';

    var path = buildPath(time);

    var prefix = 'clicktrack/' + path + '/';


    var params_listObject = {
        Bucket: bucket,
        /* required */
        Prefix: prefix
    };

    var objectList = await s3.listObjects(params_listObject).promise();

    console.log("Object File Count from s3 : ",objectList.Contents.length);

    var items = [];

    for (var object of objectList.Contents) {

        var key = object.Key;

        const params_getObject = {
            Bucket: bucket,
            Key: key,
        };
        console.log('Geting Object And ETL : ',params_getObject);

        items = items.concat(etl(await s3.getObject(params_getObject).promise()));

        //console.log(items);

    }

    console.log('ETL Item Count',items.length);

    if (items && items.length > 0) {

        var body = buildBody(items);

        const params_putObject = { Bucket: bucket, Key: 'clicklog/' + path, Body: body };

        var rs = await s3.putObject(params_putObject).promise();

        console.log('ETL Saved To S3 : ',rs);
    }



};


function buildHourTimes(hour) {

    var times = [];

    for (var i = 0; i < 6; i++) {

        var t = hour + i + '0';
        times.push(moment.tz(t, 'YYYYMMDDHHmm', 'Asia/Shanghai'));

    }

    return times;

}

function buildDayTimes(day) {

    var times = [];

    for (var i = 0; i < 24; i++) {
        var hour =  day + numeral(i).format('00');

        times = times.concat(buildHourTimes(hour));

    }

    return times;

}

function buildPrePeriod() {


    var mm = moment().tz('Asia/Shanghai').format('mm');

    mm = numeral(parseInt((parseInt(parseInt(mm)/10 - 1))*10)).format('00');

    return moment().tz('Asia/Shanghai').format('YYYYMMDDHH') + mm;

}

function buildTimes(time) {

    var times = [];

    if (time.length == 12) {

        times.push(moment.tz(time, 'YYYYMMDDHHmm', 'Asia/Shanghai'));

    }


    if (time.length == 10) {

        times = times.concat(buildHourTimes(time));

    }

    if(time.length == 8){
        times = times.concat(buildDayTimes(time));

    }

    return times;

}





async function start(time) {


    console.log('ETL Running Time : ' , moment().tz('Asia/Shanghai').format());

    console.log('ETL Time : ' , time);

    var times = buildTimes(time);
    console.log(times.length);


    for (var time of times) {
        await etlOnePiece(time);
    }



}


var program = require('commander');

var time = buildPrePeriod();
 
program
  .version('0.0.1')
  .option('-t, --time [type]', 'ETL times eg. 20170427 or 2017042721 or 201704272120 ',time)
  .parse(process.argv);


start(program.time);
