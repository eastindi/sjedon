var listify = function(context, event, callback)
{
    callback = callback || function() {};
    if (event) {
        if (event.params) {
            const Promise = require('bluebird');
            var starttime = new Date();
            var fn = Promise.promisifyAll(event.request);
            var params = Promise.map(event.params, function(param) {
                param = param.constructor.name == 'Array'?param:[param];
                if(context)
                {
                    param.unshift(context);
                    return fn[event.function + 'Async'].apply(null, param);
                }
                else
                    return fn[event.function + 'Async'].apply(null, param);
            });
            Promise.all(params).then(function(result) {
                if (result) {
                   return callback(null, JSON.stringify(result));
                }
            }).catch(function(err) {
                return callback(err);
            });
        }
    }
}

var batchify = function(context, event, callback)
{
    var result, starttime = new Date();
    var paramlist = [event.params];
    var generalhelper = require('pfhelper').general();
    var base64 =  require('base-64');
    try
    {
    event.batchoptions = event.options && event.options.batch ? event.options.batch : (process.env.batchoptions ? generalhelper.tryParse(base64.decode(process.env.batchoptions)) : null);
    var interval = event.batchoptions ? (event.batchoptions.delay ? event.batchoptions.delay : 0) : 0
    paramlist = event.batchoptions ? (event.batchoptions.batchsize ? generalhelper.splitlist(event.params, event.batchoptions.batchsize > 0 ? event.batchoptions.batchsize : event.params.length) : paramlist) : paramlist;
    }
    catch(e){};
    var Promise = require("bluebird");

    Promise.map(paramlist, function(param, index) {
        return new Promise(function(resolve, reject) {
            setTimeout(function(index) {
            listify(context, {
                    params: param,
                    request: event.request,
                    function: event.function
                }, function(err, data) {
                    if (err)
                        return callback(err);
                    else {
                        result = result ? (result.concat(data)).replace("][", ",") : data;
                        resolve('done');
                    }
                });
            }, interval * index, index);
        });
    }).then(() => {
        if (process.env.debug == true)
            console.log(new Date() - starttime + ' ms elapsed');
    }).done(() => {
        var general = require("pfhelper").general();
        result = general.tryParse(result);
        event.s3 = event.options ? (event.options.metadata ? (general.tryParse(event.options.metadata).output ? general.tryParse(event.options.metadata).output : null) : null) : null;
        if (event.s3 && event.s3.bucket) {
            var s3 = require("pfhelper").s3();
            Promise.promisifyAll(s3);
            var guid = general.generateGUID();
            return s3.putObjectAsync(
                event.s3.bucket,
                event.s3.filename ? event.s3.filename.replace('{guid}', guid) : event.s3.filename,
                event.s3.destpath,
                event.s3.filetype,
                event.s3.metadata ? event.s3.metadata : null,
                JSON.stringify(result),
                event.s3.encrypt ? event.s3.encrypt : false,
                function(err, data) {
                    if (err) 
                        return callback(err);
                    else {
                        if (process.env.debug == true)
                            console.log('File generated and uploaded to S3.');
                        return callback(null, data);
                    }
                });
        } else
            return callback(null, result);
    });
}
module.exports.listify = listify;
module.exports.batchify = batchify;