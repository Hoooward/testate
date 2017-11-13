
const parseline = trace => {
    if (trace['logtype'] == 'rq') {
        return {
            logid: trace['logid'],
            reqid: trace['reqid'],
            userid: trace['userid'],
            plat: trace['plat'],
            slotid:trace['token'],
            gaid: trace['gaid'],
            idfa: trace['idfa'],
            imei: trace['imei'],
            country: trace['icc'],
            test: trace['test'],
            ts: trace['ts']
        };
    }else{
        return null;
    }
}

exports.parseline = parseline;
exports.prefix = "adrequest";