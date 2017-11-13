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
const numeral = require('numeral');


/**
 *fake util,removing it when solt ready!
 */
const fakeSlotid =require('./testUtil/fakeTool').fakeSlotid;


const MongoClient = require('mongodb').MongoClient;
var ObjectID = require('mongodb').ObjectID;
// Connection URL
//var url = 'mongodb://52.221.173.123:27017/offer_manager';
var url = 'mongodb://172.31.0.97:27017/offer_manager';

var CbLogs;
var CbReceiveSendLogs;
// var CbChannels;
// var offerSnapshot;
var db;

async function initDB() {

    console.log('Inital DB ...');

    db = await MongoClient.connect(url);
    CbLogs = await db.collection("CbLogs");
    //CbReceiveSendLogs = await db.collection("CbReceiveSendLogs");
    // CbChannels = await db.collection("CbChannels");
    // offerSnapshot = await db.collection("offerSnapshot");


    console.log('DB Inital End');

}

async function closeDB() {

    console.log('close DB ...');

    await db.close();

    console.log('DB closed!');

}


var etl = function(data,time) {

    //ETL the Data
    var dataSaveArray = [];
    var batchTimeValue = time.format('YYYYMMDDHHmm');

    for (var i = 0; i < data.length; i++) {

        var r = data[i];

        // "_id" :  log ID,
        // "ydosid" : offer 快照ID,
        // "ydofid" : offer ID,
        // "idfa" : idfa,
        // "country" : 国家,
        // "ydscid" : 子渠道ID,
        // "ydogid" : offer组ID 也是gkey 也可以理解为Campaign,
        // "ydscgid" : 子渠道的自己广告组识别标志 Group,
        // "ofSrcPayout" : offer 的原始单价(广告主或者上游渠道放给我们的价格),
        // "premiumRate" : offer 加价,
        // "subPayout" : offer 放给子渠道的价格 不同的渠道折扣会不一样,
        // "ydCurrency" : 结算方式 "USD",
        // "eventtype" : event类型, // install login pay 需要考虑支持用户自定义类型
        // "payamount" : 支付金额,
        // "ct" : 创建时间,


        var saveRecord = {
            _id: r['_id'],
            ydofid: r['ydofid'],
            idfa: r['idfa'],
            country: r['icc'],
            ydscid: r['ydscid'],//
            ydogid: r['ydogid'], //
            ydckid: r['ydckid'], //adgroup
            ip: r['ip'],
            ua: r['ua'],
            ydid: r['ydid'],
            ydscgid: r['ydscgid'],
            app_name:r['offer']['app_name'],
            offerSource:r['offer']['offerSource'],
            ofSrcPayout: r['ofSrcPayout'],
            premiumRate: r['premiumRate'],
            subPayout: r['subPayout'],
            ydCurrency: r['ydCurrency'],
            eventtype: r['eventtype'],
            payamount: r['payamount'],
            ct: r['ct'],
            uct: moment.tz(r['ct'], 'Asia/Shanghai'),
            slotid:r['slotid'],///**fake soltid */,slotid:fakeSlotid()
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
    var items = await CbLogs.find({ ct: eval("/" + pieceTime + "/i") }).toArray();

    items = etl(items,time);

    console.log('ETL Item Count', items.length);


    if (items && items.length > 0) {

        var body = buildBody(items);

        var bucket = 'com.yodamob.adserver.track';
        var path = buildPath(time);

        const params_putObject = { Bucket: bucket, Key: `etl_event/dt=${path}`, Body: body };

        var rs = await s3.putObject(params_putObject).promise();

        console.log('ETL Saved To S3 : ',rs);
    }

};

module.exports = {etlBatchExecute,initDB,closeDB};