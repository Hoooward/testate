const fakeSlotid =require('./testUtil/fakeTool').fakeSlotid;

const parseline = trace => {
    if (trace['logtype'] == 'rp' && trace['request']) {
        let request = trace['request'];
        return {
            logid: trace['logid'],
            reqid: trace['reqid'],
            plat: request['plat'],
            slotid:request['token'],///*fake soltid */slotid:fakeSlotid(),
            offer: trace['offer'],
            gaid: trace['gaid'],
            idfa: trace['idfa'],
            imei: trace['imei'],
            userid: trace['userid'],
            country: trace['icc'],
            ts: trace['ts']
        };
    }else{
        return null;
    }
}

exports.parseline = parseline;
exports.prefix = "adresponse";