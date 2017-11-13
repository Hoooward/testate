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
const fakeSlotid =require('./src/testUtil/fakeTool').fakeSlotid;



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


var etl = function(data) {

    //ETL the Data
    var dataSaveArray = [];

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
            //ydosid: r['ydosid'],
            ydofid: r['ydofid'],
            idfa: r['idfa'],
            country: r['country'],
            ydscid: r['offerGroup']['scid'], //
            ydogid: r['offerGroup']['gkey'], //
            ydscgid: r['ydscgid'], //adgroup
            ofSrcPayout: r['ofSrcPayout'],
            premiumRate: r['premiumRate'],
            subPayout: r['subPayout'],
            ydCurrency: r['ydCurrency'],
            eventtype: r['eventtype'],
            payamount: r['payamount'],
            ct: r['ct'],
            uct: moment.tz(r['ct'], 'Asia/Shanghai'),
            slotid:r['slotid']///**fake soltid */,slotid:fakeSlotid()
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




async function etlOnePiece(time) {

    if (!db) {
        await initDB();
    }



    console.log('ETL Time : ', time);

    //time eg. 201704272120
    //得到这个
    var pieceTime = time.format('YYYY-MM-DDTHH:mm').substring(0, 15);

    //正则表达式 且不区分大小写
    var items = await CbLogs.find({ ct: eval("/" + pieceTime + "/i") }).toArray();

    items = etl(items);

    console.log('ETL Item Count', items.length);




    if (items && items.length > 0) {

        var body = buildBody(items);

        var bucket = 'com.yodamob.adserver.track';
        var path = buildPath(time);

        const params_putObject = { Bucket: bucket, Key: 'eventlog/' + path, Body: body };

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
        var hour = day + numeral(i).format('00');

        times = times.concat(buildHourTimes(hour));

    }

    return times;

}

function buildPrePeriod() {


    var time = moment().tz('Asia/Shanghai').subtract(10,'minute').format('YYYYMMDDHHmm');

    var mm = numeral(parseInt((parseInt(parseInt(time.substring(10,12))/10)) * 10)).format('00');

    return time.substring(0,10) + mm;

}

function buildMonthTimes(month) {

    var times = [];

    var m0 = moment(month, 'YYYYMM').tz('Asia/Shanghai');
    var m1 = moment(month, 'YYYYMM').tz('Asia/Shanghai').add(1,'M');

    for (var i = m0; i.isBefore(m1); i.add(1,'d')) {

        var day = i.format('YYYYMMDD');

        times = times.concat(buildDayTimes(day));

    }

    return times;

}

function buildTimes(time) {

    var times = [];

    if (time.length == 12) {

        times.push(moment.tz(time, 'YYYYMMDDHHmm', 'Asia/Shanghai'));

    }


    if (time.length == 10) {

        times = times.concat(buildHourTimes(time));

    }

    if (time.length == 8) {
        times = times.concat(buildDayTimes(time));

    }

    if (time.length == 6) {
        times = times.concat(buildMonthTimes(time));

    }

    return times;

}





async function start(time) {

    console.log('ETL Running Time : ', moment().tz('Asia/Shanghai').format());

    console.log('ETL Time : ', time);

    var times = buildTimes(time);
    console.log(times.length);


    for (var time of times) {
        await etlOnePiece(time);
    }

    if (db) {
        db.close();
    }

}


var program = require('commander');

var time = buildPrePeriod();

program
    .version('0.0.1')
    .option('-t, --time [type]', 'ETL times eg. 20170427 or 2017042721 or 201704272120 ', time)
    .parse(process.argv);


start(program.time);