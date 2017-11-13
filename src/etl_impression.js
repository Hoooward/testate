
const parseline = trace => {
    if (trace['logtype'] == 's') {
        return {
            logid: trace['logid'],
            impid: trace['impid'],
            reqid: trace['reqid'],
            slotid:trace['slotid'],
            yodfid: trace['yodfid'],
            yodasc: trace['yodasc'],
            userid: trace['userid'],
            country: trace['icc'],
            ts: trace['ts']
        };
    }else{
        return null;
    }
}

exports.parseline = parseline;
exports.prefix = "imptrack";