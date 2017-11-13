/**
 * Created by chenruibin on 2017/8/24.
 */
/**
 * origin log: request,ip,os,num,slotid,ua,ts
 *
 * */

const parseline = trace => {
    let resLine = null;
    try{
        resLine = {
            ip: trace['ip'],
            os: trace['os'],
            num:trace['num'],//slotid:trace['slotid'],
            slotid: trace['slotid'],
            ua: trace['ua'],
            ts: trace['ts']
        }
    }catch (err){
        // console.error("sts parsline err ->",err)
    }
    return resLine;

}

exports.parseline = parseline;
exports.prefix = "sts/ruo";
