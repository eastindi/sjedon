'use strict'
var Promise = require('bluebird')

function setupEnvironment () {
  if (typeof String.prototype.startsWith !== 'function') {
    String.prototype.startsWith = function (str) {
      return this.indexOf(str) === 0
    }
  }
  if (typeof String.prototype.endsWith !== 'function') {
    String.prototype.endsWith = function (suffix) {
      return this.indexOf(suffix, this.length - suffix.length) !== -1
    }
  }
  /*
  if (typeof String.prototype.toBase64 !== 'function') {
      String.prototype.toBase64 = function (value) {
          return new Buffer(value).toString('base64')
      }
  }
  if (typeof String.prototype.fromBase64 !== 'function') {
      String.prototype.fromBase64 = function (value) {
          return new Buffer(value, 'base64').toString('ascii')
      }
  }
  */
  if (typeof Date.prototype.dayOfYear !== 'function') {
    Date.prototype.dayOfYear = function () {
      var j1 = new Date(this)
      j1.setMonth(0, 0)
      return Math.round((this - j1) / 8.64e7)
    }
  }
  if (typeof Date.prototype.dayRef !== 'function') {
    Date.prototype.dayRef = function (filler) {
      var date = new Date(this)
      var yr = date.getFullYear()
      var dyr = date.dayOfYear()
      var hh = date.getHours()
      return yr.toString().substr(3, 1).concat(
        ('000' + dyr.toString()).slice(-3),
        ('00' + hh).slice(-2),
        ('000000' + filler).slice(-6)
      )
    }
  }
}

function isEmpty (val) {
  return (val === undefined || val === null || val.length <= 0) ? true : false
}

function lambdaContext (event, context) {
  //  http://docs.aws.amazon.com/apigateway/latest/developerguide/api-gateway-mapping-template-reference.html#context-variable-reference

  var out = {}
  if (!exports.isEmpty(context.functionName)) {
    out.ReceivedEvent = JSON.stringify(event, null, 2)
    out.RemainingTime = context.getRemainingTimeInMillis()
    out.FunctionName = context.functionName
    out.FunctionVersion = context.functionVersion
    out.InvokedFunctionArn = context.invokedFunctionArn
    out.AWSrequestID = context.awsRequestId
    out.LogGroupName = context.logGroupName
    out.LogStreamName = context.logStreamName
    out.ClientContext = context.clientContext
    // context.identity.sourceIp
    if (typeof context.identity !== 'undefined') {
      out.CognityIdentityID = context.identity.cognito_identity_id
      out.CognityPoolID = context.identity.cognito_identity_pool_id
    }
  }
  return out
}

function daydiff (datepart, fromdate, todate) {
  // datepart: 'y', 'm', 'w', 'd', 'h', 'n', 's'

  datepart = datepart.toLowerCase()
  var diff = todate - fromdate
  var divideBy = {
    w: 604800000,
    d: 86400000,
    h: 3600000,
    n: 60000,
    s: 1000
  }

  return Math.floor(diff / divideBy[datepart])
}

function generateGUID () {
  let uuid = require('node-uuid')
  return uuid.v4()
}

function lookupCallerEnv (apikey, lookupCallerEnvCallback) {
  var dynamo = require('./dynamo'),
    localapikey = apikey

  dynamo.getTableValue('configuration', 'name', 'payments', 'value', false, function (err, data) {
    if (err) {
      return lookupCallerEnvCallback(err)
    } else {
      if (data !== null) {
        var key = data.apiKeys[localapikey]
        return lookupCallerEnvCallback(null, key)
      } else {
        return lookupCallerEnvCallback(new Error('403.Failed to retrieve payments configuration'))
      }
    }
  })
}

function verifyCaller (event, verifyCallback) {
  var sourceip = event.sourceip
  lookupCallerEnv(event.headers['x-api-key'], function (err, key) {
    if (err) {
      return verifyCallback(err)
    } else {
      if (key) {
        var allowIPAddress = key.ipaddress.indexOf(sourceip) > -1 ? true : false
        if (!allowIPAddress) {
          console.log('403.IP Address ' + sourceip + ' not allowed')
          return verifyCallback(new Error('403.Invalid request--bad IP address'), null)
        } else {
          return verifyCallback(null, key)
        }
      } else {
        console.log('403.Api Key ' + event.headers['x-api-key'] + ' not allowed')
        return verifyCallback(new Error('403.Invalid request--bad Api Key'), null)
      }
    }
  })
}

function getBytes (str) {
  var buffer = require('Buffer')
  return Buffer.byteLength(str, 'utf8')
}

function splitlist (list, size) {
  var result = []
  while (list.length) {
    result.push(list.splice(0, size))
  }
  return result
}

function cleanupString (str, replaceStr) {
  var str1, str2, str3, k
  if (replaceStr) {
    var i = 1
    for (var item of replaceStr) {
      if (i > 1 && i % 2 == 0)
        str = str.replace(str1, item)
      else
        str1 = new RegExp(item, 'g')
      i++
    }
  }
  return str
}

function extractValue (result, path) {
  // console.log(JSON.stringify(result))
  var str, key
  str = result
  for (var item of path) {
    if (typeof (str[item]) === 'object') {
      str = str[item]
      key = item
    }
  }
  return str
}

function isS3Event (event) {
  if (event) {
    if (event.Records) {
      if (event.Records[0])
        if (event.Records[0].eventSource === 'aws:s3')
          return true
    }
  }
  return false
}

function jsontransform (data, event) {
  if (event) {
    var guid = generateGUID()
    var date = (new Date().toISOString()).replace(/T/, ' ').replace(/\..+/, '')
    event = tryParse(((JSON.stringify(event)).replace(/{guid}/g, guid)).replace(/{date}/g, date))
    var format = event.format ? event.format : 'json'
    var input = data
    var isArray = false
    if (event.format == 'xml') {
      var xml2js = require('xml2js')
      var parser = new xml2js.Parser({
        explicitArray: false
      })
      parser.parseString(data, function (err, result) {
        if (err)
          input = data
        else {
          input = result
        }
      })
    }

    input = input ? (event.path ? jsonpathextract(input, event.path) : input) : null
    if (input) {
      var map = event.map,
        add = event.add,
        keep = event.keep,
        merge = event.merge,
        list = event.list,
        concat = event.append,
        dateformat = event.dateformat

      var keys, maps, ret, adds, merges
      maps = map ? Object.keys(map) : null
      adds = add ? Object.keys(add) : null
      isArray = (input.constructor === Array)

      if (!isArray)
        input = [input]
      ret = keep ? input : Array.apply(null, Array(input.length)).map(function () {
        return { }
      })
      var k = 0
      for (var i of input) {
        keys = Object.keys(i)
        var item = keep ? i : {},
          j = 0
        if (maps) {
          maps.forEach(function (key) {
            if (!keep) {
              jsonpathcreate(item, map[key], jsonpathextract(i, key))
              j++
              if (j == maps.length)
                ret[k] = item
            } else {
              jsonpathcreate(i, map[key], jsonpathextract(i, key))
              delete jsonpathdelete(i, key)
            }
          })
        }
        if (adds) {
          adds.forEach(function (key) {
            jsonpathcreate(item, key, add[key])
          })
        }
        if (merge) {
          merge.forEach(function (key) {
            item[key] = toObject(item[key])
          })
        }
        if (list) {
          list.forEach(function (listitem) {
            var itemvalue = jsonpathextract(item, listitem)
            var listitems = Object.keys(itemvalue)
            var _tmp = []
            listitems.forEach(function (k) {
              _tmp.push((jsonpathextract(item, listitem))[k])
              jsonpathdelete(item, listitem + '/' + k)
            })
            jsonpathdelete(item, listitem)
            jsonpathcreate(item, listitem, _tmp)
          })
        }
        if (concat) {
          concat.forEach(function (listitem) {
            var itemvalue = jsonpathextract(item, listitem)
            var listitems = Object.keys(itemvalue)
            var _tmp = ''
            listitems.forEach(function (k) {
              _tmp += (jsonpathextract(item, listitem))[k]
            })
            jsonpathdelete(item, listitem)
            jsonpathcreate(item, listitem, _tmp)
          })
        }
        if (dateformat) {
          dateformat.forEach(function (listitem) {
            var itemvalue = jsonpathextract(item, listitem.field)
            var listitems = Object.keys(itemvalue)
            var _tmp = ''
            listitems.forEach(function (k) {
              _tmp += ((jsonpathextract(item, listitem.field))[k]).trim()
            })
            jsonpathdelete(item, listitem.field)

            var general = require('./general')
            jsonpathcreate(item, listitem.field, stringToDate(_tmp, listitem.format))
          })
        }
        k++
      }
      if (!isArray)
        ret = ret[0]
    }
  }

  // if (event.path && keep) {
  //     jsonpathcreate(input, event.path, ret)
  //     return ret
  // }
  // console.log(JSON.stringify(ret))
  return ret
}

function toObject (obj) {
  var item
  var merges = Object.keys(obj)
  var result = []
  var len = obj[merges[0]].length
  for (var i = 0; i < len; i++) {
    item = {}
    merges.forEach(function (key) {
      item[key] = obj[key][i]
    })
    result.push(item)
  }
  return result
}

function jsonpathextract (data, path) {
  // console.log(JSON.stringify(result))
  var str, key
  str = data
  path = path.split('/')
  for (var item of path) {
    if (str)
      str = str[item] ? str[item] : null
  }
  return str
}

function getValues (obj) {
  var keys = Object.keys(obj)
  var result = []
  for (var i = 0; i < keys.length; i++) {
    result.push(obj[keys[i]])
  }
  return result
}

function jsonpathget (data, path) {
  // console.log(JSON.stringify(result))
  var str, key
  str = data
  path = path.split('/')
  for (var item of path) {
    if (str)
      str = str[item] ? str[item] : null
  }
  return str
}

function jsonpathcreate (obj, keyPath, value) {
  if (obj) {
    if (keyPath.constructor === Array) {
      keyPath.forEach(function (kp) {
        jsonpathcreate(obj, kp, value)
      })
    } else {
      keyPath = keyPath.split('/')
      var lastKeyIndex = keyPath.length - 1
      for (var i = 0; i < lastKeyIndex; ++i) {
        var key = keyPath[i]
        if (!(key in obj))
          obj[key] = {}
        obj = obj[key]
      }
      if (obj[keyPath[lastKeyIndex]]) {
        obj[keyPath[lastKeyIndex]] = [obj[keyPath[lastKeyIndex]]]
        obj[keyPath[lastKeyIndex]].push(value)
      } else
        obj[keyPath[lastKeyIndex]] = value
    }
  }
}

function jsonpathdelete (obj, keyPath) {
  keyPath = keyPath.split('/')
  var lastKeyIndex = keyPath.length - 1
  for (var i = 0; i < lastKeyIndex; ++i) {
    var key = keyPath[i]
    if (!(key in obj))
      obj[key] = {}
    obj = obj[key]
  }
  delete obj[keyPath[lastKeyIndex]]
}

function tryParse (str) {
  try {
    var ret = JSON.parse(str)
    if (typeof ret == 'string' && isNaN(ret[key]) == true)
      return tryParse(ret)
    return ret
  } catch (e) {
    return str
  }
}

function deepParse (str) {
  try {
    var ret = tryParse(str)
    if (typeof ret == 'object') {
      Object.keys(ret).forEach(function (key) {
        {
          if (typeof ret[key] == 'string')
            ret[key] = tryParse(ret[key])
          return deepParse(ret[key])
        }
      })
    }else {
      return ret
    }
    return ret
  } catch (e) {
    return str
  }
}

function tryExtract (input, path) {
  try {
    return JSON.parse(str)
  } catch (e) {
    return str
  }
}

function callbackFunctions (functions, event, callback) {
  for (var i = 0; i < functions.length; i++) {
    if (functions[i].Payload && event) {
      functions[i].Payload.sourceEvent = event
    }
    if (typeof functions[i] == 'object')
      functions[i].Payload = JSON.stringify(functions[i].Payload)
  }
  callback = callback || {}
  var lstevent = {}
  var lstfy = require('./listify')
  lstevent.params = functions.filter(function (fun) { return (fun.enabled == undefined || fun.enabled == true); })
  lstevent.request = require('./lambda')
  lstevent.function = 'invoke'
  lstfy.listify(null, lstevent, function (err, data) {
    return (err, data)
  })
}

function maskstring (data, char) {
  if (data) {
    if (typeof data === 'string') {
      return new Array(data.length + 1).join(char)
    }
  }
  return null
}

function jsonmask (data, event) {
  if (event) {
    event.forEach(function (item) {
      var val = jsonpathextract(data, item)
      if (val) {
        val = maskstring(val, 'X')
        jsonpathdelete(data, item)
        jsonpathcreate(data, item, val)
      }
    })
  }
  return data
}

function jsonreplace (data, input) {
  // loop through and replace all instance of eg. {UserGUID} with params["UserGUID"], "{AccountGUID}" with params["AccountGUID"]
  var dataString = JSON.stringify(data)
  // console.log(dataString)
  Object.keys(input).forEach(function (key) {
    var replaceParamName = '{' + key + '}'
    if (dataString.indexOf(replaceParamName) >= 0)
      dataString = dataString.split(replaceParamName).join(input[key])
  // dataString = dataString.replace(replaceParamName, input[key])
  })
  // console.log(dataString)
  return JSON.parse(dataString)
}

function isBase64 (value) {
  var base64Type = /^([A-Za-z0-9+/]{4})*/
  return base64Type.test(value)
}

function isJson (str) {
  try {
    JSON.parse(str)
  } catch (e) {
    return false
  }
  return true
}

function stringToDate (value, format) {
  var newValue = format

  for (var i = 0; i < value.length; i++) {
    newValue = newValue.replace('x', value[i])
  }

  var dateFormat = require('dateformat')

  return dateFormat(newValue, 'isoDateTime')
}

function generateRandom (min, max) {
  var min = Math.ceil(min)
  var max = Math.floor(max)

  return Math.floor(Math.random() * (max - min + 1)) + min
}

function replace (str, replacer) {
  replacer = (replacer.constructor === Array) ? replacer : [replacer]
  replacer.forEach(function (key) {
    var str1 = new RegExp(key.replace, 'g')
    str = str.replace(str1, key.with)
  })
  return str
}

function fixcase (input, template) {
  if (input && template && Object.keys(template)) {
    var isArray = (input.constructor.name === 'Array')
    var tmp
    if (!isArray)
      input = [input]
    input.forEach(function (item) {
      if (Object.keys(item)) {
        Object.keys(template).forEach(function (key) {
          Object.keys(item).forEach(function (itemkey) {
            if (key.toLowerCase() === itemkey.toLowerCase()) {
              tmp = item[itemkey]
              delete item[itemkey]
              item[key] = tmp
            }
          })
        })
      }
    })
    return isArray ? input : input[0]
  } else
    return input
}

function subset (input, template) {
  var root, tmp
  if (input && template) {
    var isInputArray = (input.constructor.name === 'Array')
    var isTemplateArray = (template.constructor.name === 'Array')
    if (!isTemplateArray)
      template = [template]
    if (!isInputArray)
      input = [input]
    for (var i = 0; i < input.length; i++) {
      tmp = {}
      template.forEach(function (tItem) {
        root = tItem.substr(0, tItem.lastIndexOf('/'))

        var val = jsonpathextract(input[i], tItem)
        if (val) {
          jsonpathcreate(tmp, tItem, val)
        }
      })
      if (root.length > 0)
        jsonpathset(input[i], root, tmp[root])
      else
        input[i] = tmp
    }
  }
  return isInputArray == true ? input : input[0]
}

function jsonpathset (data, path, value) {
  // console.log(JSON.stringify(result))
  path = path.split('/')
  for (var i = 0; i < path.length; i++) {
    if (data)
      if (i < path.length - 1)
        data = data[path[i]]
      else
        data[path[i]] = value
  }
  return data
}

function generateResponse (event, statusCode, body, callback) {
  callback = callback || function () {}

  let isAPIGateway = (!isEmpty(event) && !isEmpty(event.requestContext) && !isEmpty(event.requestContext.apiId) && event.requestContext.apiId.length > 0) ? true : false
  if (statusCode == 200) {
    if (isAPIGateway) {
      let response = {
        statusCode: statusCode,
        body: JSON.stringify(body)
      }
      return callback(null, response)
    } else {
      return callback(null, body)
    }
  } else {
    //   console.log('event', event)
    if (isAPIGateway) {
      let response = {
        statusCode: statusCode,
        body: JSON.stringify({
          'message': body
        })
      }
      return callback(null, response)
    } else {
      return callback(body)
    }
  }
}

function lookupValue (event, name) {
  let value = null
  try {
    value = event[name]
    if (isEmpty(value)) {
      value = process.env[name]
    }
  } catch (err) {}
  return value
}

module.exports.callbackFunctions = callbackFunctions
module.exports.setupEnvironment = setupEnvironment
module.exports.lambdaContext = lambdaContext
module.exports.daydiff = daydiff
module.exports.isS3Event = isS3Event
module.exports.extractValue = extractValue
module.exports.cleanupString = cleanupString
module.exports.splitlist = splitlist
module.exports.getBytes = getBytes
module.exports.verifyCaller = verifyCaller
module.exports.lookupCallerEnv = lookupCallerEnv
module.exports.isEmpty = isEmpty
module.exports.generateResponse = generateResponse
module.exports.lookupValue = lookupValue
module.exports.jsonpathextract = jsonpathextract
module.exports.jsonpathcreate = jsonpathcreate
module.exports.tryExtract = tryExtract
module.exports.tryParse = tryParse
module.exports.jsonpathdelete = jsonpathdelete
module.exports.toObject = toObject
module.exports.jsontransform = jsontransform
module.exports.generateGUID = generateGUID
module.exports.jsonmask = jsonmask
module.exports.jsonreplace = jsonreplace
module.exports.isBase64 = isBase64
module.exports.isJson = isJson
module.exports.stringToDate = stringToDate
module.exports.generateRandom = generateRandom
module.exports.replace = replace
module.exports.fixcase = fixcase
module.exports.subset = subset
module.exports.deepParse = deepParse
