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

const MongoClient = require('mongodb').MongoClient;
const _url = 'mongodb://172.31.0.97:27017';
const _bucket = 'com.yodamob.adserver.track';
const _batch = 1000;


var _collection;
var _db;
var _s3_prefix;

async function initDB(coll) {

    let dburl;
    if(coll === 'adSlot'){
        dburl = `${_url}/pubyoda`;
    }else{
        dburl = `${_url}/offer_manager`;
    }
    console.log(`Inital DB : ${dburl}`);
    console.log(`Inital Collection : ${coll}`);

    _db = await MongoClient.connect(dburl);
    _collection = await _db.collection(coll);

    console.log('DB Inital End');

}

async function closeDB() {

    console.log('close DB ...');

    await _db.close();

    console.log('DB closed!');

}


var buildBody = function(items) {

    var body = "";

    for (var item of items) {

        body = body + JSON.stringify(item) + '\n';
    }

    return body;

}

async function clearS3() {

    let oldObjects = await s3.listObjects({Bucket: _bucket, Prefix: _s3_prefix}).promise();

    console.log("oldObjects->",oldObjects);

    let values = await Promise.all(oldObjects.Contents.map(old => {
        return s3.deleteObject({Bucket: _bucket, Key: old.Key}).promise();
    }))

    console.log("clearS3 ->",values);

}

async function init(logtype) {
    let coll;
    if(logtype==='offer'){
        coll = "offers";
        _s3_prefix = "offers";
    }else if(logtype==='record'){
        coll = "offerRecord";
        _s3_prefix = "offerRecord";
    }else if(logtype==='cost'){
        coll = "SlotCost";
        _s3_prefix = "SlotCost";
    }else if(logtype==='adSlot'){
        coll = "adSlot";
        _s3_prefix = "adSlot";
    } else{
        throw new Error("inValid logtype!!!");
    }

    await initDB(coll);
}

async function etlBatchExecute(logtype) {
    console.log('----------------------OFFER ETL START-------------------------');

    await init(logtype);

    var total = await _collection.count();

    console.log(`offers total ${total} pages ${total / _batch}`);
    await clearS3();

    for(let i = 0;i < total / _batch; i ++){

        let items = await _collection.find({}).limit(_batch).skip(i*_batch).toArray();

        let body = buildBody(items);

        let params_putObject = { Bucket: _bucket, Key: `${_s3_prefix}/${i}-${items.length}`, Body: body };

        let rs = await s3.putObject(params_putObject).promise();
        console.log(`save offers page ${i} length ${items.length}`);
        console.log('offers Saved To S3 rs :  ',rs);
    }

    await closeDB();
    console.log('----------------------OFFER ETL OVER-------------------------');

};

module.exports = {etlBatchExecute};