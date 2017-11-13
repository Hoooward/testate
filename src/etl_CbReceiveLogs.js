/**
 * Created by chenruibin on 2017/6/8.
 */
/*
 From MongoDB 2 S3
 */
'use strict';
const aws = require('aws-sdk');
const s3 = new aws.S3({ apiVersion: '2006-03-01' });
const moment = require('moment-timezone');
const MongoClient = require('mongodb').MongoClient;
// Connection URL
var url = 'mongodb://172.31.0.97:27017/offer_manager';


var Collection;
var DB;

async function initDB() {

    console.log('Inital DB ...');

    DB = await MongoClient.connect(url);
    Collection = await DB.collection("CbReceiveLogs");

    console.log('DB Inital End');

}

async function closeDB() {

    console.log('close DB ...');

    await DB.close();

    console.log('DB closed!');

}


var etl = function(data,time) {

    //ETL the Data
    var dataSaveArray = [];
    var batchTimeValue = time.format('YYYYMMDDHHmm');

    for (var i = 0; i < data.length; i++) {

        var r = data[i];


        var saveRecord = {
            _id: r['_id'],
            ct: r['ct'],
            type: r['type'],
            yodaevent:r['yodaevent'],
            ydid:r['ydid'],
            data: r['data'],
            version: r['version'],
            batchTime: batchTimeValue
        };

        dataSaveArray.push(saveRecord);

    }

    return dataSaveArray;

}


var buildPath = function(time) {

    //console.log(time);

    var beijingTime = moment.tz(time, 'Asia/Shanghai').tz('Asia/Shanghai');

    //console.log(beijingTime.format());

    var mm = beijingTime.format('mm'); // numeral(parseInt(parseInt(beijingTime.format('mm')) / 10) * 10).format('00');

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

async function etlBatchExecute(time) {
    console.log('ETL Time : ', time);

    //time eg. 201704272120
    //得到这个
    var pieceTime = time.format('YYYY-MM-DDTHH:mm').substring(0, 15);

    //正则表达式 且不区分大小写
    var items = await Collection.find({ ct: eval("/" + pieceTime + "/i") }).toArray();

    items = etl(items,time);

    console.log('ETL Item Count', items.length);

    if (items && items.length > 0) {

        let body = buildBody(items);

        let bucket = 'com.yodamob.adserver.track';
        let path = buildPath(time);
        let fileName = `etl_CbReceiveLogs/dt=${path}`;


        let params_putObject = { Bucket: bucket, Key: fileName, Body: body };

        var rs = await s3.putObject(params_putObject).promise();

        console.log(`ETL Saved To S3 filename ${fileName} rs: `, rs);
    }

};

module.exports = {etlBatchExecute,initDB,closeDB};