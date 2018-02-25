const general = require('./general.js');
module.exports.putObjectFull = function(bucketName, keyName, fullPath, contentType, metadata, body, filePermission, storageType, callback) {
    module.exports.putObjectFullEncrypted(bucketName, keyName, fullPath, contentType, metadata, body, filePermission, storageType, false, callback);
};

module.exports.putObjectFullEncrypted = function(bucketName, keyName, fullPath, contentType, metadata, body, filePermission, storageType, encrypt, callback) {
    callback = callback || function() {};

    var aws = require('aws-sdk'),
        s3 = new aws.S3({
            apiVersion: '2006-03-01',
            "signatureVersion": "v4"
        });

    if (general.isEmpty(bucketName) || general.isEmpty(keyName) || general.isEmpty(fullPath)) return callback(new Error('bucketName, keyName or fullPath missing'));

    var Key = fullPath + keyName;

    var params = {
        Bucket: bucketName,
        Key: Key.toLowerCase(),
        ACL: filePermission,
        Body: body,
        ContentType: contentType,
        Metadata: typeof(metadata) === "string" ? {
            'title': metadata
        } : metadata,
        StorageClass: storageType
    };

    if (encrypt) {
        params.ServerSideEncryption = 'aws:kms';
    }

    s3.putObject(params, function(err, data) {
        if (err) {
            return callback(err);
        } else {
            return callback(err, data);
        }
    });
};

module.exports.putObject = function(bucketName, keyName, fullPath, contentType, metadata, body, callback) {
    module.exports.putObjectFull(bucketName, keyName, fullPath, contentType, metadata, body, 'private', 'STANDARD', callback);
};

module.exports.putObject = function(bucketName, keyName, fullPath, contentType, metadata, body, encrypt, callback) {
    module.exports.putObjectFullEncrypted(bucketName, keyName, fullPath, contentType, metadata, body, 'private', 'STANDARD', encrypt, callback);
};

module.exports.getObject = function(bucketName, keyName, callback) {
    callback = callback || function() {};

    var aws = require('aws-sdk'),
        s3 = new aws.S3({
            apiVersion: '2006-03-01',
            "signatureVersion": "v4"
        }),
        general = require('./general.js');

    if (general.isEmpty(bucketName) || general.isEmpty(keyName)) return callback(new Error('bucketName or keyName missing'));

    var params = {
        Bucket: bucketName,
        Key: keyName.toLowerCase()
    };

    s3.getObject(params, function(err, data) {
        if (err) {
            return callback(err);
        } else {
            return callback(null, data);
        }
    });
};

module.exports.headObject = function(bucketName, keyName, callback) {
    callback = callback || function() {};

    var aws = require('aws-sdk'),
        s3 = new aws.S3({
            apiVersion: '2006-03-01',
            "signatureVersion": "v4"
        }),
        general = require('./general.js');

    if (general.isEmpty(bucketName) || general.isEmpty(keyName)) return callback(new Error('bucketName or keyName missing'));

    var params = {
        Bucket: bucketName,
        Key: keyName.toLowerCase()
    };

    s3.headObject(params, function(err, data) {
        if (err) {
            return callback(err);
        } else {
            return callback(null, data);
        }
    });
};

module.exports.deleteObject = function(bucket, key, callback) {
    var aws = require('aws-sdk'),
        s3 = new aws.S3({
            apiVersion: '2006-03-01',
            "signatureVersion": "v4"
        })
    var deleteParam = {Bucket: bucket, Key:key};
    s3.deleteObject(deleteParam, function(err, data) 
    {
       if (err) {
            return callback(err);
        } else {
            return callback(null, data);
        }
    });   
}
module.exports.listObjects = function (bucket,marker, callback)
{
     var aws = require('aws-sdk'),
        s3 = new aws.S3({
            apiVersion: '2006-03-01',
            "signatureVersion": "v4"
        })
    var getParam = {Bucket: bucket, Prefix:marker};
    s3.listObjects(getParam, function(err, data) 
    {
       if (err) {
            return callback(err);
        } else {
            return callback(null, data);
        }
    });   
}

module.exports.eventify = function(event, callback) {
    if(event)
    {
        if (event.Records)
        {
            var _record = event.Records[0];
            if (_record.eventSource === "aws:s3")
            {
                var fn = require("./s3");
                fn.getObject(_record.s3.bucket.name, _record.s3.object.key, function(err, data)
                {
                    if(err)
                        return callback(err);
                    else
                    {
                            var fileData = {};//general.tryParse(general.tryParse(data.Body.toString()));
                            fileData.data = general.tryParse(general.tryParse(data.Body.toString()));
                            fileData.bucket = _record.s3.bucket.name;
                            fileData.key = _record.s3.object.key;
                            gets3metadata(fileData.bucket, fileData.key, function(err, data)
                        {
                            if(data)
                                fileData.metadata = data;
                            return callback(err, fileData);
                        })
                    }
                });
            }
        }
    }
 }

 function gets3metadata(bucket, key, callback)
 {
    var fn = require("./s3");
    var dynamo =  require("./dynamo");
    var upath = require("upath");
    var scan = 
          {
            "TableName": "s3event",
            "FilterExpression": "#bucketname = :bucketname and #prefix = :prefix and #suffix = :suffix",
            "ExpressionAttributeNames": {
                      "#bucketname": "bucketname",
                      "#prefix": "prefix",
                      "#suffix": "suffix"
                  },
                  "ExpressionAttributeValues": {
                      ":bucketname": bucket,
                      ":prefix": upath.dirname(key),
                      ":suffix": upath.extname(key)
                  }
           
          };
    dynamo.getData(scan, function(err, data)
    {
        if (err || data.Items.length <= 0)
            fn.headObject(bucket, key, function(err, data)
            {
                if(err)
                    return callback(err);
                else
                {
                    return callback(null, data["Metadata"]);
                }
            });
        else
            return callback(err, data.Items[0]?data.Items[0].metadata:null);
    });
 }

