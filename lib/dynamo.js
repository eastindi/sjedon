var aws = require('aws-sdk');
aws.config.update({
    region: 'us-east-1'
});

var general = require('./general.js'),
    kms = require('./kms.js'),
    converter = require('dynamo-converters');

module.exports.getTableValue = function(tableName, keyName, name, attributeName, decryptValue, callback) {
    if (general.isEmpty(tableName) || general.isEmpty(keyName) || general.isEmpty(name) || general.isEmpty(attributeName)) return;

    callback = callback || function() {};

    var key = {
        'name': {
            S: name
        }
    };

    exports.queryTable(tableName, key, attributeName, function(err, data) {
        if (err) {
            callback(err, null);
        } else {
            var value = converter.itemToData(data.Item);
            if (value !== null) {
                if (decryptValue) {
                    if (general.isBase64(value)) {
                        kms.decryptFromBase64(value[attributeName], function(err, data) {
                            if (err) {
                                return callback(err, null);
                            } else {
                                if (data['Plaintext']) {
                                    var strValue = data['Plaintext'].toString();
                                    if (!general.isEmpty(strValue)) {
                                        if (general.isJson(strValue)) {
                                            return callback(null, JSON.parse(strValue));
                                        } else {
                                            return callback(null, strValue);
                                        }
                                    }                                    
                                } else {
                                    return callback(null, data);
                                }
                            }
                        });
                    } else {
                        kms.decrypt(value[attributeName], function(err, data) {
                            if (err) {
                                return callback(err, null);
                            } else {
                                if (data['Plaintext']) {
                                    var strValue = data['Plaintext'].toString();
                                    if (!general.isEmpty(strValue)) {
                                        if (general.isJson(strValue)) {
                                            return callback(null, JSON.parse(strValue));
                                        } else {
                                            return callback(null, strValue);
                                        }
                                    }                                    
                                } else {
                                    return callback(null, data);
                                }
                            }
                        });
                    }
                } else {
                    return callback(null, value[attributeName]);
                }
            }
        }
    });
};

module.exports.getTableValueFromBase64 = function(tableName, keyName, name, attributeName, decryptValue, callback) {
    if (general.isEmpty(tableName) || general.isEmpty(keyName) || general.isEmpty(name) || general.isEmpty(attributeName)) return;

    callback = callback || function() {};

    var key = {
        'name': {
            S: name
        }
    };

    exports.queryTable(tableName, key, attributeName, function(err, data) {
        if (err) {
            callback(err, null);
        } else {
            var value = converter.itemToData(data.Item);
            if (value !== null) {
                if (decryptValue) {
                    kms.decryptFromBase64(value[attributeName], function(err, data) {
                        if (err) {
                            return callback(err, null);
                        } else {
                            var connection = JSON.parse(data);
                            if (!general.isEmpty(connection)) {
                                return callback(null, connection);
                            }
                        }
                    });
                } else {
                    return callback(null, value[attributeName]);
                }
            }
        }
    });
};

module.exports.queryTable = function(tableName, key, attributeName, callback) {
    if (general.isEmpty(tableName) || general.isEmpty(key)) return;

    callback = callback || function() {};

    var dynamo = new aws.DynamoDB({
        apiVersion: '2012-08-10'
    });
    var params = {
        Key: key,
        TableName: tableName,
        AttributesToGet: [attributeName]
    };
    dynamo.getItem(params, function(err, data) {
        return callback(err, data);
    });
};

module.exports.getData = function(params, callback) {
    if (params == null) return;

    callback = callback || function() {};

    var docClient = new aws.DynamoDB.DocumentClient();

    var dynamo = new aws.DynamoDB({
        apiVersion: '2012-08-10'
    });

    docClient.scan(params, function(err, data) {
        return callback(err, data)
    });
};

module.exports.deleteItem = function(params, callback) {
    if (params == null) return;

    callback = callback || function() {};

    var docClient = new aws.DynamoDB.DocumentClient();

    var dynamo = new aws.DynamoDB({
        apiVersion: '2012-08-10'
    });

    docClient.delete(params, function(err, data) {
        return callback(err, data)
    });
};

module.exports.putItem = function(tableName, item, callback) {
    if (general.isEmpty(tableName) || general.isEmpty(item)) return;

    callback = callback || function() {};

    var dynamo = new aws.DynamoDB({
        apiVersion: '2012-08-10'
    });
    var params = {
        Item: item,
        TableName: tableName
    };
    dynamo.putItem(params, function(err, data) {
        return callback(err, data);
    });
};

module.exports.get = function(tableName,keyName, value, callback) {
    if (general.isEmpty(tableName) || general.isEmpty(keyName) || general.isEmpty(value) ) return;

    callback = callback || function() {};
    var dynamo = new aws.DynamoDB.DocumentClient()
    var key = {};
    key[keyName] = value;
    var params = {
        Key: key,
        TableName: tableName
    };
    dynamo.put(params, function(err, data) {
        return callback(err, data);
    });
};

module.exports.put = function(tableName, item, callback) {
    if (general.isEmpty(tableName) || general.isEmpty(item)) return;

    callback = callback || function() {};

    var dynamo = new aws.DynamoDB.DocumentClient()
    var params = {
        Item: item,
        TableName: tableName
    };
    dynamo.put(params, function(err, data) {
        return callback(err, data);
    });
};

module.exports.query = function( params,  callback) {
    if (general.isEmpty(params)) return;
    onQuery(params,null,callback)
};

var onQuery = function (params,retArray,callback) {
    
    retArray = retArray || [];
    callback = callback || function() {};

    var dynamo = new aws.DynamoDB.DocumentClient()
    dynamo.query(params, function(err, data) {
        // return callback(err, data);
        if(err) {
            return callback(err);
        }else{
            retArray = retArray.concat(data.Items);
            if(data.LastEvaluatedKey){
                params.ExclusiveStartKey = data.LastEvaluatedKey;
                onQuery(params,retArray,callback);
            }else{
                return callback(null,retArray);
            }
        }
    });
}
module.exports.scan = function(tableName, filterExpression, ExpressionAttributes, callback) {
    var params = {
        TableName: tableName
    }
    if(filterExpression && ExpressionAttributes) {
        params.FilterExpression = filterExpression.FilterExpression;
        params.ExpressionAttributeValues = ExpressionAttributes.ExpressionAttributeValues;
    }
    onScan(params,null,callback);
}

var onScan = function (params,retArray,callback) {
    
    retArray = retArray || [];
    callback = callback || function() {};

    var dynamo = new aws.DynamoDB.DocumentClient()
    dynamo.scan(params, function(err, data) {
        // return callback(err, data);
        if(err) {
            return callback(err);
        }else{
            retArray = retArray.concat(data.Items);
            if(data.LastEvaluatedKey){
                params.ExclusiveStartKey = data.LastEvaluatedKey;
                onScan(params,retArray,callback);
            }else{
                return callback(null,retArray);
            }
        }
    });
}

module.exports.update = function(params, callback){
    if (general.isEmpty(params)) return;
    
    callback = callback || function() {};

    var dynamo = new aws.DynamoDB.DocumentClient()
    
    dynamo.update(params, function(err, data) {
        return callback(err, data);
    });
}

module.exports.putJSONItem = function(tableName, json, callback) {
    if (general.isEmpty(tableName) || general.isEmpty(json)) return;

    callback = callback || function() {};

    var dynamo = new aws.DynamoDB({
        apiVersion: '2012-08-10'
    });
    var params = {
        TableName: tableName
    };
    dynamo.putItem(params, function(err, data) {
        return callback(err, data);
    });
};