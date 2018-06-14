var express = require('express');
var api = require('nodetsdb-api');
var Datastore = require('@google-cloud/datastore');
// actual datastore
var datastore;

var config = {
};

var backend = {};

var lpad = function (input, char, length) {
    while (input.length < length) {
        input = char + input;
    }
    return input;
}

var uidKind = function(type, callback) {
    var kind;
    switch (type) {
        case "metric": kind = "metric_uid"; break;
        case "tagk": kind = "tagk_uid"; break;
        case "tagv": kind = "tagv_uid"; break;
        default:
            callback(null, 'Unsupported type: '+type);
            return;
    }
    callback(kind);
};

var uidMetaFrom = function (type, identifier, callback) {
    uidKind(type, function(kind, err) {
        if (err) {
            callback(null, err);
            return;
        }

        datastore.get(datastore.key({namespace:config.namespace,path:[kind, identifier]}), function(err, entity) {
            if (err) {
                callback(null, err);
            }
            else {
                callback(entity, null);
            }
        });
    });

};



var multiUidMetaFrom = function (type, identifiers, callback) {
    uidKind(type, function(kind, err) {
        if (err) {
            callback(null, err);
            return;
        }

        if (identifiers.length === 0) {
            callback([]);
            return;
        }

        console.log("Loading "+kind+" identifiers: "+JSON.stringify(identifiers));
        var dsIdentifiers = identifiers.map(function(identifier){return datastore.key({namespace:config.namespace,path:[kind, identifier]});});
        datastore.get(dsIdentifiers, function(err, entities) {
            if (err) {
                callback(null, err);
            }
            else {
                var ret = {};
                var foundANull = false;
                for (var i=0; i<identifiers.length; i++) {
                    var uidmeta = entities[i];
                    ret[uidmeta.uid] = uidmeta;
                    if (entities[i] === undefined) {
                        foundANull = true;
                    }
                }
                callback(ret, foundANull ? "At least one lookup failed" : null);
            }
        });
    });
};

var uidMetaFromName = function(type, name, callback) {
    uidMetaFrom(type, "name:"+name, callback);
};

// on backend api
backend.uidMetaFromUid = function(type, uid, callback) {
    uidMetaFrom(type, "id:"+uid, callback);
};

var multiUidMetaFromUid = function(type, uids, callback) {
    multiUidMetaFrom(type, uids.map(function(uid) { return "id:"+uid; }), callback);
};

var nextUid = function(txn, kind, callback) {
    var uidSequenceKey = datastore.key({namespace:config.namespace,path:["uid_sequence", "kind:"+kind]});

    txn.get(uidSequenceKey, function(err, entity) {
        if (err) {
            callback(null, "Error loading data entity: " + err);
            return;
        }

        var data;
        if (entity === undefined) {
            // create it
            entity = {
                key: uidSequenceKey,
                data: {
                    nextUid: 1
                }
            };
            data = entity.data;
        }
        else {
            data = entity;
            entity = {
                key: uidSequenceKey,
                data: data
            };
        }

        var uid = data.nextUid;
        data.nextUid++;

        txn.save(entity);

        callback(uid);
    });
};

var assignUidIfNecessary = function(type, name, callback) {
    uidKind(type, function(kind, err) {
        if (err) {
            callback(null, err);
            return;
        }

        var txn = datastore.transaction();

        txn.run(function (err) {

            if (err) {
                callback(null, 'Error creating transaction: '+ err);
                return;
            }

            var byNameKey = datastore.key({namespace:config.namespace,path:[kind, "name:"+name]});

            txn.get(byNameKey, function(err, entity) {
                if (err) {
                    callback(null, "Error loading data entity: "+ err);
                    return;
                }
                else if (entity !== undefined) {
                    callback(entity.uid);
                    return;
                }

                nextUid(txn, kind, function(uid, err) {
                    if (err) {
                        callback(null, 'failed to assign uid: '+ err);
                        return;
                    }

                    var uidString = lpad(uid.toString(16), '0', config[type+"_uid_bytes"]*2);
                    var byIdKey = datastore.key({namespace:config.namespace,path:[kind, "id:"+uidString]});

                    var byNameEntity = {
                        key: byNameKey,
                        data: {
                            uid: uidString,
                            name: name
                        }
                    };
                    var byIdEntity = {
                        key: byIdKey,
                        data: {
                            uid: uidString,
                            name: name
                        }
                    };

                    txn.save(byNameEntity);
                    txn.save(byIdEntity);

                    txn.commit(function (err) {
                        if (err) {
                            callback(null, 'failed to assign uid: '+ err);
                        }
                        else {
                            callback(uidString);
                        }
                    });
                });
            });
        });
    });
};

var suggest = function(entity, prefix, max, callback) {
    if (!prefix) {
        prefix = "";
    }
    var query = datastore
        .createQuery(config.namespace, entity);
    if (prefix !== "") {
        query = query
            .filter("__key__", ">=", datastore.key({namespace:config.namespace,path:[entity, "name:"+prefix]}))
            // todo: fudgeroo!! would be better to increment the last character by one
            .filter("__key__", "<=", datastore.key({namespace:config.namespace,path:[entity, "name:"+prefix+"zzzzzzzzzzzzzzzzzz"]}))
    }
    else {
        // exclude id: keys
        query = query
            .filter("__key__", ">=", datastore.key({namespace:config.namespace,path:[entity, "name:"]}))
    }
    query = query.order("__key__");

    if (max) {
        query = query.limit(max);
    }

    datastore
        .runQuery(query, function(err, entities) {
            if (err) {
                callback(null, err);
            }
            else {
                if (entities.length > 0) {
                    var ret = entities.map(function (uid) {
                        return uid.name;
                    });
                    callback(ret);
                }
                else {
                    callback([]);
                }
            }
        });
};

// on backend api
backend.suggestMetrics = function(prefix, max, callback) {
    suggest("metric_uid", prefix, max, callback);
};

// on backend api
backend.suggestTagKeys = function(prefix, max, callback) {
    suggest("tagk_uid", prefix, max, callback);
};

// on backend api
backend.suggestTagValues = function(prefix, max, callback) {
    suggest("tagv_uid", prefix, max, callback);
};

var withMetricAndTagUids = function(txn, metric, incomingTags, callback) {
    if (config.verbose) {
        console.log("withMetricAndTagUids(txn, \"" + metric + "\", " + JSON.stringify(incomingTags) + ", callback)");
    }
    var metricUidCallback = function(metricUid, err) {
        if (config.verbose) {
            console.log("metricUidCallback: " + JSON.stringify(metricUid));
        }
        if (err) {
            callback(null, null, null, err);
        }
        else {
            // now for tags
            var tagks = [];
            for (var tagk in incomingTags) {
                if (incomingTags.hasOwnProperty(tagk)) {
                    tagks.push(tagk);
                }
            }

            var tagkUids = [];
            var tags = {};
            var hadError = false;
            var processNextTag = function (t) {
                if (t < tagks.length && !hadError) {
                    var tagk = tagks[t];
                    var tagv = incomingTags[tagk];

                    var tagvCallback = function (tagk_uid, tagv_uid, err) {
                        if (err) {
                            hadError = true;
                            callback(null, null, null, err);
                        }
                        else {
                            tagkUids.push(tagk_uid);
                            tags[tagk_uid] = tagv_uid;
                            if (!hadError) {
                                processNextTag(t + 1);
                            }
                        }
                    };
                    var tagkCallback = function (tagk_uid, err) {
                        if (err) {
                            hadError = true;
                            callback(null, null, null, err);
                        }
                        else {
                            assignUidIfNecessary("tagv", tagv, function(tagv_uid, err) {
                                tagvCallback(tagk_uid, tagv_uid, err);
                            });
                        }
                    };
                    assignUidIfNecessary("tagk", tagk, tagkCallback);
                }
                else {
                    if (!hadError) {
                        tagkUids.sort();
                        var tagUidString = "";
                        var tagUidArray = [];
                        for (var t = 0; t < tagkUids.length; t++) {
                            var tagk_uid = tagkUids[t];
                            tagUidString += tagk_uid + tags[tagk_uid];
                            tagUidArray.push(tagk_uid);
                            tagUidArray.push(tags[tagk_uid]);
                        }

                        callback(metricUid, tagUidString, tagUidArray, null);
                    }
                }
            };
            processNextTag(0);
        }
    };
    assignUidIfNecessary("metric", metric, metricUidCallback);
};

var dataRowKey = function(metricUid, hour, tagUidString) {
    var hString = lpad(hour.toString(16), ' ', 4);
    return metricUid + hString + tagUidString;
};

// on backend api
backend.storePoints = function(points, storePointsCallback) {
    // start simple, process in series, single transaction for each point, read entity for timeseries/hour, update entity, write down
    var errors = new Array(points.length);
    if (points.length === 0) {
        storePointsCallback([]);
    }
    var storePoint = function(pointIndex) {
        var point = points[pointIndex];
        var errorMessage = undefined;

        if (config.verbose) {
            console.log("Processing point write " + pointIndex + ": " + JSON.stringify(point));
        }
        if (pointIndex >= points.length) {
            throw "wat?"
        }

        var txn = datastore.transaction();
        txn.run(function (err) {
            if (err) {
                errors[pointIndex] = err;
                if (pointIndex >= points.length - 1) {
                    storePointsCallback(errors);
                }
                else {
                    storePoint(pointIndex + 1);
                }
            }
            else {
                var timestamp = point.timestamp;
                var ms = timestamp > 10000000000;
                var hour = Math.floor(ms ? timestamp / 86400000 : timestamp / 86400)
                // force offset to ms
                var offsetFromHour = ms ? timestamp % 86400000 : (timestamp % 86400) * 1000;

                var uidCallback = function(metricUid, tagUidString, tagUidArray, err) {
                    if (config.verbose) {
                        console.log("uidCallback("+metricUid+","+tagUidString+","+JSON.stringify(tagUidArray)+","+err+")");
                    }
                    if (err) {
                        errors.push(errorMessage);
                        return;
                    }
                    else {
                        var rowId = dataRowKey(metricUid, hour, tagUidString);
                    }
                    var rowKey = datastore.key({namespace:config.namespace,path:["data", rowId]});
                    var row = undefined;

                    var processCommitResult = function(err) {
                        if (err) {
                            errorMessage = "Error saving entity: " + err;
                        }
                        errors[pointIndex] = errorMessage;
                        if (pointIndex >= points.length - 1) {
                            storePointsCallback(errors);
                        }
                        else {
                            storePoint(pointIndex + 1);
                        }
                    };

                    txn.get(rowKey, function (err, entity, info) {
                        if (err) {
                            processCommitResult("Error loading data entity: " + err);
                            return;
                        }
                        row = entity;

                        if (config.verbose) {
                            console.log("New data row? "+(row === undefined));
                        }
                        var data;
                        if (row === undefined) {
                            // create it
                            row = {
                                key: rowKey,
                                data: {
                                    tags: tagUidArray
                                }
                            };
                            data = row.data;
                        }
                        else {
                            data = row;
                            row = {
                                key: rowKey,
                                data: data
                            };

                        }

                        data[offsetFromHour.toString()] = Number(point.value);

                        txn.save(row);

                        try {
                            if (config.verbose) {
                                console.log("committing write on "+rowKey);
                            }
                            txn.commit(processCommitResult);
                        }
                        catch (err) {
                            processCommitResult(err);
                        }
                    });
                };
                withMetricAndTagUids(txn, point.metric, point.tags, uidCallback);
            }
        });
    };
    storePoint(0);
};

// on backend api
backend.searchLookupImpl = function(query, limit, useMeta, callback) {
    /*
    if (useMeta) {
        callback([]); // todo: not supported yet
    }
    */
    // so we need to look through all the row keys for the given metric (or all row keys if no metric given)
    // for each on we find the tags and then filter against the list of tag in the query
    var tagks = {};
    var disconnectedTagvs = {};
    for (var i=0; i<query.tags.length; i++) {
        if (query.tags[i].key !== "*") {
            var arr = tagks[query.tags[i].key];
            if (arr == null) {
                arr = [];
                tagks[query.tags[i].key] = arr;
            }
            if (query.tags[i].value === "*") {
                tagks[query.tags[i].key] = ["*"];
            }
            else {
                if (tagks[query.tags[i].key].length === 0 || tagks[query.tags[i].key][0] !== "*") {
                    tagks[query.tags[i].key].push(query.tags[i].value);
                }
            }
        }
        else {
            disconnectedTagvs[query.tags[i].value] = query.tags[i].value;
        }
    }

    var findTimeSeries = function(metricUid, callback) {
        var startRowKey = metricUid != null ? dataRowKey(metricUid.uid, 0, "") : dataRowKey(lpad("", "0", config.metric_uid_bytes*2), 0, "");
        var endRowKey = metricUid != null ? dataRowKey(metricUid.uid, Math.pow(256, config.metric_uid_bytes)-1, "") : dataRowKey(lpad("", "z", config.metric_uid_bytes*2), 0, "");

        var query = datastore
            .createQuery(config.namespace, "data")
            .filter("__key__", ">=", datastore.key({namespace:config.namespace,path:["data", startRowKey]}))
            .filter("__key__", "<=", datastore.key({namespace:config.namespace,path:["data", endRowKey]}));
        if (limit) {
            query = query.limit(limit);
        }

        var processRawRows = function(rawRows) {
            // now we need to find all the metricUids, all the tagk uids and all the tagv uids, convert them all to names and then remap everything
            var metric_uid_set = {};
            var metric_uids = [];
            var tagk_uids = [];
            var tagk_uid_set = {};
            var tagv_uids = [];
            var tagv_uid_set = {};

            for (var r=0; r<rawRows.length; r++) {
                var metric_uid = rawRows[r].metric;
                if (!(metric_uid in metric_uid_set)) {
                    metric_uid_set[metric_uid] = metric_uid;
                    metric_uids.push(metric_uid);
                }

                for (var t=0; t<rawRows[r].tags.length; t+=2) {
                    var tagk_uid = rawRows[r].tags[t];
                    var tagv_uid = rawRows[r].tags[t+1];

                    if (!(tagk_uid in tagk_uid_set)) {
                        tagk_uid_set[tagk_uid] = tagk_uid;
                        tagk_uids.push(tagk_uid);
                    }

                    if (!(tagv_uid in tagv_uid_set)) {
                        tagv_uid_set[tagv_uid] = tagv_uid;
                        tagv_uids.push(tagv_uid);
                    }
                }
            }

            reverseMapUids(metric_uids, tagk_uids, tagv_uids, function(metricUidMetaByUidMap, tagkUidMetaByUidMap, tagvUidMetaByUidMap, err) {
                if (err) {
                    callback(null, err);
                }
                for (var r=0; r<rawRows.length; r++) {
                    var metric_uid = rawRows[r].metric;
                    rawRows[r].metric = metricUidMetaByUidMap[metric_uid].name;

                    var newTags = {};
                    for (var t=0; t<rawRows[r].tags.length; t+=2) {
                        var tagk_uid = rawRows[r].tags[t];
                        var tagv_uid = rawRows[r].tags[t+1];
                        newTags[tagkUidMetaByUidMap[tagk_uid].name] = tagvUidMetaByUidMap[tagv_uid].name;
                    }
                    rawRows[r].tags = newTags;
                }

                // now we have all the rows with their bits resolved, now we need to filter based on any tagk/tagv in the query
                var filteredRows = [];
                for (var r=0; r<rawRows.length; r++) {
                    var row = rawRows[r];

                    var removeRow = false;
                    for (var k in tagks) {
                        if (tagks.hasOwnProperty(k)) {
                            // todo
                        }
                    }
                    for (var v in disconnectedTagvs) {
                        if (disconnectedTagvs.hasOwnProperty(v)) {
                            var foundAMatch = false;
                            for (var rk in row.tags) {
                                if (row.tags.hasOwnProperty(rk)) {
                                    var rv = row.tags[rk];
                                    if (rv === v) {
                                        foundAMatch = true;
                                    }
                                }
                            }
                            if (!foundAMatch) {
                                removeRow = true;
                            }
                        }
                    }

                    if (!removeRow) {
                        filteredRows.push(row);
                    }
                }

                callback(filteredRows, null);
            });
        };


        datastore
            .runQuery(query, function(err, entities) {
                if (err) {
                    callback(null, err);
                }
                else {
                    if (entities.length > 0) {
                        var seenTsuids = {};
                        var rowsForDistinctTsuids = [];

                        for (var r=0; r<entities.length; r++) {
                            var row = entities[r];

                            var key = row[Datastore.KEY].name;
                            if (config.verbose) {
                                console.log("ROW KEY: "+key+" (Namespace: "+row[Datastore.KEY].namespace+")");
                            }

                            var decomposedKey = decomposeRowKey(key);

                            if (!seenTsuids.hasOwnProperty(decomposedKey.tsuid)) {
                                // {metric:'some.metric',tags:{host:"host1"}, tsuid: "000001000001000001"}
                                rowsForDistinctTsuids.push({
                                    metric: decomposedKey.metricUid,
                                    tags: row.tags,
                                    tsuid: decomposedKey.tsuid
                                });
                                seenTsuids[decomposedKey.tsuid] = decomposedKey.tsuid;
                            }
                        }
                        processRawRows(rowsForDistinctTsuids);
                    }
                    else {
                        callback([]);
                    }
                }
            });

    };

    if (query.metric) {
        uidMetaFrom("metric", "name:"+query.metric, function(uid, err) {
            if (err) {
                callback(null, err);
            }
            else {
                findTimeSeries(uid, callback);
            }

        });
    }
    else {
        findTimeSeries(null, callback);
    }



};

// on backend api
backend.storeAnnotations = function(annotations, storeAnnotationsCallback) {
    // start simple, process in series, single transaction for each, read entity for timeseries/hour, update entity, write down
    var errors = new Array(annotations.length);
    if (annotations.length === 0) {
        storeAnnotationsCallback([]);
    }
    var storeAnnotation = function(annotationIndex) {
        var annotation = annotations[annotationIndex];
        var errorMessage = undefined;

        if (config.verbose) {
            console.log("Processing annotation " + annotationIndex + ": " + JSON.stringify(annotation));
        }
        if (annotationIndex >= annotations.length) {
            throw "wat?"
        }

        var txn = datastore.transaction();
        txn.run(function (err) {
            if (err) {
                errors[annotationIndex] = err;
                if (annotationIndex >= annotations.length - 1) {
                    storeAnnotationsCallback(errors);
                }
                else {
                    storeAnnotation(annotationIndex + 1);
                }
            }
            else {
                var timestamp = annotation.startTime;
                var ms = timestamp > 10000000000;
                var hour = Math.floor(ms ? timestamp / 86400000 : timestamp / 86400);
                // force offset to ms
                var offsetFromHour = ms ? timestamp % 86400000 : (timestamp % 86400) * 1000;

                var tagk_string_len = config.tagk_uid_bytes*2;
                var tagv_string_len = config.tagv_uid_bytes*2;
                var tsuid = annotation.tsuid;
                var metricUid = lpad("", "0", config.metric_uid_bytes*2), tagUidString = "", tagUidArray = [];
                if (tsuid) {
                    metricUid = tsuid.substring(0, config.metric_uid_bytes*2);
                    tagUidString = tsuid.substring(metricUid.length);
                    for (var i=0; i<tagUidString.length; i+=(tagk_string_len+tagv_string_len)) {
                        var kString = tagUidString.substring(i,i+tagk_string_len);
                        var vString = tagUidString.substring(i+tagk_string_len, i+tagk_string_len+tagv_string_len);
                        tagUidArray.push(kString);
                        tagUidArray.push(vString);
                    }
                }

                var rowId = dataRowKey(metricUid, hour, tagUidString);
                var rowKey = datastore.key({namespace:config.namespace,path:["ann", rowId]});
                var row = undefined;

                var processCommitResult = function(err) {
                    if (err) {
                        errorMessage = "Error saving entity: " + err;
                    }
                    errors[annotationIndex] = errorMessage;
                    if (annotationIndex >= annotations.length - 1) {
                        storeAnnotationsCallback(errors);
                    }
                    else {
                        storeAnnotation(annotationIndex + 1);
                    }
                };

                txn.get(rowKey, function (err, entity, info) {
                    if (err) {
                        processCommitResult("Error loading data entity: " + err);
                        return;
                    }
                    row = entity;

                    if (config.verbose) {
                        console.log("New data row? "+(row === undefined));
                    }
                    var data;
                    if (row === undefined) {
                        // create it
                        row = {
                            key: rowKey,
                            data: {
                                tags: tagUidArray
                            }
                        };
                        data = row.data;
                    }
                    else {
                        data = row;
                        row = {
                            key: rowKey,
                            data: data
                        };

                    }

                    data[offsetFromHour.toString()] = annotation;

                    txn.save(row);

                    try {
                        txn.commit(processCommitResult);
                    }
                    catch (err) {
                        processCommitResult(err);
                    }
                });
            }
        });
    };
    storeAnnotation(0);
};

// on backend api
backend.deleteAnnotation = function(annotation, callback) {
    callback(null); // todo: not supported yet
};

// on backend api
backend.performAnnotationsQueries = function(startTime, endTime, downsampleSeconds, participatingTimeSeries, callback) {
    // first we can map the time series to a set of row keys for possible annotations
    var startHour = Math.floor(startTime.getTime() / 86400000);
    // var startOffset = startTime.getTime() % 86400000;
    var endHour = Math.floor(endTime.getTime() / 86400000);
    // var endOffset = endTime.getTime() % 86400000;

    var allKeys = [];
    for (var t=0; t<participatingTimeSeries.length; t++) {
        for (var h=startHour; h<=endHour; h++) {
            var metricUid = participatingTimeSeries[t].metric_uid;
            var keyId = dataRowKey(metricUid, h, participatingTimeSeries[t].tsuid.substring(metricUid.length));
            allKeys.push(datastore.key({namespace:config.namespace,path:["ann", keyId]}));
        }
    }

    datastore.get(allKeys, function(err, entities) {
        if (err) {
            callback(null, err);
        }
        else {

            // var metricUidLength = config.metric_uid_bytes*2;
            // var hourLength = 4;
            // var tagkUidLength = config.tagk_uid_bytes*2;
            // var tagvUidLength = config.tagv_uid_bytes*2;

            var ret = [];
            for (var i=0; i<entities.length; i++) {
                var entity = entities[i];
                if (config.verbose) {
                    var key = entity[Datastore.KEY].name;
                    console.log("ANN ROW: "+key+" = "+JSON.stringify(entity));
                }

                for (var offset in entity) {
                    if (entity.hasOwnProperty(offset) && offset !== "tags" && entity[offset].hasOwnProperty("startTime")) {
                        var annStartTime = entity[offset].startTime;
                        var ms = annStartTime > 10000000000;
                        var desiredStartTime = ms ? startTime.getTime() : startTime.getTime() / 1000;
                        var desiredEndTime = ms ? endTime.getTime() : endTime.getTime() / 1000;

                        if (annStartTime >= desiredStartTime && annStartTime <= desiredEndTime) {
                            ret.push(entity[offset]);
                        }
                    }
                }

            }
            callback(ret, null);
        }
    });


};

/*
data is Array of {
 *                     tsuid:String,
 *                     description:String,
 *                     notes:String,
 *                     custom:Map,
 *                     startTime:Date,
 *                     endTime:Date
 *                   }
 */

// on backend api
backend.performGlobalAnnotationsQuery = function(startTime, endTime, callback) {
    //function(startTime, endTime, downsampleSeconds, participatingTimeSeries, callback)
    var participatingTimeSeries = [
        {
            metric_uid: lpad("", "0", config.metric_uid_bytes*2),
            tsuid: ""
        }
    ];
    backend.performAnnotationsQueries(startTime, endTime, null, participatingTimeSeries, callback);
};

var decomposeRowKey = function(rowKey) {
    var metricUidLength = config.metric_uid_bytes*2;
    var hourLength = 4;
    // var tagkUidLength = config.tagk_uid_bytes*2;
    // var tagvUidLength = config.tagv_uid_bytes*2;

    var metricUidString = rowKey.substring(0, metricUidLength);
    var tagUidString = rowKey.substring(metricUidLength+hourLength);
    var hourString = rowKey.substring(metricUidLength, metricUidLength+hourLength);

    return {
        metricUid: metricUidString,
        tagUidString: tagUidString,
        tsuid: metricUidString + tagUidString,
        hour: parseInt(hourString, 16)
    };
};

// callback = function(metricUidMetaByUidMap, tagkUidMetaByUidMap, tagvUidMetaByUidMap, err)
var reverseMapUids = function(metric_uids, tagk_uids, tagv_uids, callback) {

    var tagvsCallback = function (metricMetas, tagkMetas, tagvMetas, err) {
        if (err) {
            callback(null, null, null, err);
            return;
        }
        if (config.verbose) {
            console.log("tagvMetas = " + JSON.stringify(tagvMetas));
        }

        callback(metricMetas, tagkMetas, tagvMetas, null);
    };

    var tagksCallback = function(metricMetas, tagkMetas, err) {
        if (err) {
            callback(null, null, null, err);
            return;
        }
        if (config.verbose) {
            console.log("tagkMetas = " + JSON.stringify(tagkMetas));
        }

        multiUidMetaFromUid("tagv", tagv_uids, function(tagvMetas, err) { tagvsCallback(metricMetas, tagkMetas, tagvMetas, err); });
    };

    var metricsCallback = function(metricMetas, err) {
        if (err) {
            callback(null, null, null, err);
            return;
        }
        if (config.verbose) {
            console.log("metricMetas = " + JSON.stringify(metricMetas));
        }

        multiUidMetaFromUid("tagk", tagk_uids, function(tagkMetas, err) { tagksCallback(metricMetas, tagkMetas, err); });
    };

    if (metric_uids.length > 0) {
        multiUidMetaFromUid("metric", metric_uids, metricsCallback);
    }
    else {
        multiUidMetaFromUid("tagk", tagk_uids, function(tagkMetas, err) { tagksCallback({}, tagkMetas, err); });
    }
};

// on backend api
backend.performBackendQueries = function(startTime, endTime, downsample, metric, filters, callback) {
    if (config.verbose) {
        console.log("performBackendQueries(" + startTime + "," + endTime + ", " + downsample + ", " + metric + ", " + filters + ")");
    }


    uidMetaFromName("metric", metric, function(metricUid, err) {
        if (err) {
            callback(null, err);
            return;
        }
        // metric not known
        if (metricUid === undefined) {
            callback(null, "Metric "+metric+" not known");
            return;
        }
        // now run a query from metricUid+startHour inclusive to metricUid+(endHour+1) exclusive
        var startHour = Math.floor(startTime.getTime() / 86400000);
        var startOffset = startTime.getTime() % 86400000;
        var endHour = Math.floor(endTime.getTime() / 86400000);
        var endOffset = endTime.getTime() % 86400000;

        var timeFilterFunction = function(h) {
            if (h !== startHour && h !== endHour) {
                return function() {return true;}
            }
            else if (h === startHour && h === endHour) {
                // filter both ends
                return function(offset) {
                    return offset >= startOffset && offset <= endOffset;
                }
            }
            else if (h === startHour) {
                return function(offset) {
                    return offset >= startOffset;
                }
            }
            else { //if (h == endHour) {
                return function(offset) {
                    return offset <= endOffset;
                }
            }
        };

        var startKey = datastore.key({namespace:config.namespace,path:["data", dataRowKey(metricUid.uid, startHour, "")]});
        var endKey = datastore.key({namespace:config.namespace,path:["data", dataRowKey(metricUid.uid, endHour+1, "")]});
        if (config.verbose) {
            console.log("Finding data rows where " + JSON.stringify(startKey) + " <= __key__ < " + JSON.stringify(endKey));
        }



        var processRawRows = function(rows) {
            // each item in rows has a tag string and some dps, should be in order so we can detect switches of tag string
            var allTagks = {};
            var allTagvs = {};
            for (var r=0; r<rows.length; r++) {
                for (var t=0; t<rows[r].tags.length; t+=2) {
                    var tagk = rows[r].tags[t];
                    var tagv = rows[r].tags[t+1];
                    allTagks[tagk] = tagk;
                    allTagvs[tagv] = tagv;
                }
            }

            var tagk_uids = [];
            for (var k in allTagks) {
                if (allTagks.hasOwnProperty(k)) {
                    tagk_uids.push(k);
                }
            }
            if (config.verbose) {
                console.log("tagk_uids = "+JSON.stringify(tagk_uids));
            }

            var tagv_uids = [];
            for (var v in allTagvs) {
                if (allTagvs.hasOwnProperty(v)) {
                    tagv_uids.push(v);
                }
            }
            if (config.verbose) {
                console.log("tagv_uids = "+JSON.stringify(tagv_uids));
            }

            reverseMapUids([], tagk_uids, tagv_uids, function(metricMetas, tagkMetas, tagvMetas, err) {
                var ret = [];

                var lastUidString = undefined;
                var currentDps = [];
                var currentTags = {};

                for (var r=0; r<rows.length; r++) {
                    if (lastUidString !== undefined && rows[r].tag_uids !== lastUidString) {
                        ret.push({
                            metric: metric,
                            metric_uid: metricUid.uid,
                            tags: currentTags,
                            tsuid: metricUid.uid+lastUidString,
                            dps: currentDps
                        });
                        lastUidString = undefined;
                    }

                    if (lastUidString === undefined) {
                        // reset
                        currentDps = [];
                        var tags = {};
                        for (var t=0; t<rows[r].tags.length; t+=2) {
                            var k_uid = rows[r].tags[t];
                            var v_uid = rows[r].tags[t+1];
                            if (config.verbose && !tagkMetas.hasOwnProperty(k_uid)) {
                                console.log("Couldn't find a tagk meta for uid: "+k_uid);
                            }
                            var k = tagkMetas[k_uid].name;
                            tags[k] = {
                                tagk: k,
                                tagk_uid: k_uid,
                                tagv: tagvMetas[v_uid].name,
                                tagv_uid: v_uid
                            };
                        }
                        currentTags = tags;
                        lastUidString = rows[r].tag_uids;
                    }

                    currentDps = currentDps.concat(rows[r].dps);
                }
                // push the last one
                ret.push({
                    metric: metric,
                    metric_uid: metricUid.uid,
                    tags: currentTags,
                    tsuid: metricUid.uid+lastUidString,
                    dps: currentDps
                });

                if (config.verbose) {
                    console.log("Calling back to API with ret = "+JSON.stringify(ret));
                }
                callback(ret, null);
            });
        };

        var runQuery = function(cursor, rows) {
            if (config.verbose) {
                console.log("Running query with start cursor: " + cursor);
            }
            var query = datastore
                .createQuery(config.namespace, "data")
                .filter("__key__", ">=", startKey)
                .filter("__key__", "<", endKey)
                .order("__key__");

            if (cursor) {
                query = query.start(cursor);
            }

            datastore
                .runQuery(query, function(err, entities, info) {
                    if (config.verbose) {
                        console.log("query result info: "+JSON.stringify(info));
                    }
                    if (err) {
                        callback(null, err);
                    }
                    else {
                        if (entities.length > 0) {
                            // entities found
                            var rawRows = entities;
                            if (config.verbose) {
                                console.log("Found "+rawRows.length+" raw rows");
                            }

                            for (var r=0; r<rawRows.length; r++) {
                                if (config.verbose) {
                                    console.log("ROW: "+JSON.stringify(rawRows[r]));
                                }
                                var row = rawRows[r];


                                var key = row[Datastore.KEY].name;
                                if (config.verbose) {
                                    console.log("ROW KEY: "+key+" (Namespace: "+row[Datastore.KEY].namespace+")");
                                }

                                var decomposedKey = decomposeRowKey(key);
                                var tagUidString = decomposedKey.tagUidString;
                                var hour = decomposedKey.hour;
                                var timeFilter = timeFilterFunction(hour);

                                var keys = [];
                                for (var k in row) {
                                    if (row.hasOwnProperty(k) && k !== "tags") {
                                        keys.push(Number(k));
                                    }
                                }
                                keys.sort();
                                var dps = [];
                                for (var i=0; i<keys.length; i++) {
                                    var offset = keys[i];
                                    // filter the data
                                    if (timeFilter(offset)) {
                                        var timestamp = (hour * 86400000) + offset;
                                        dps.push([timestamp, row[String(offset)]]);
                                    }
                                }
                                var toPush = {tags:row.tags, tag_uids:tagUidString, dps:dps};
                                if (config.verbose) {
                                    console.log("Pushing row: "+JSON.stringify(toPush));
                                }
                                rows.push(toPush);
                            }
                        }
                        else {
                            if (config.verbose) {
                                console.log("Found 0 raw rows");
                            }
                        }

                        if (info.moreResults !== Datastore.NO_MORE_RESULTS && info.endCursor !== cursor) {
                            if (config.verbose) {
                                console.log("old cursor: "+cursor+", new cursor: "+info.endCursor);
                            }
                            runQuery(info.endCursor, rows);
                        }
                        else {
                            console.log("out of rows, what now?");
                            processRawRows(rows);
                        }
                    }
                });
        };
        runQuery(null, []);
    });
};

var applyOverrides = function(from, to) {
    for (var k in from) {
        if (from.hasOwnProperty(k)) {
            if (to.hasOwnProperty(k)) {
                switch (typeof from[k]) {
                    case 'number':
                    case 'string':
                    case 'boolean':
                        to[k] = from[k];
                        continue;
                    default:
                        if (config.verbose) {
                            console.log("unhandled: "+(typeof from[k]));
                        }
                }
                applyOverrides(from[k], to[k]);
            }
            else {
                to[k] = from[k];
            }
        }
    }
}

var installApiWithBackend = function(app, incomingConfig) {
    var backend = setupBackend(incomingConfig);

    api.backend(backend);
    api.install(app, config);
};

var setupBackend = function(incomingConfig) {
    if (!incomingConfig) {
        incomingConfig = {};
    }

    var conf = {
        verbose: false,
        logRequests: true,
        version: "2.2.0",
        metric_uid_bytes: 3,
        tagk_uid_bytes: 3,
        tagv_uid_bytes: 3,
        projectId: 'opentsdb-cloud',
        dataStoreKeyFile: null,
        namespace: null
    };

    applyOverrides(incomingConfig, conf);

    config = conf;

    datastore = new Datastore({
        projectId: config.projectId,
        keyFile: config.dataStoreKeyFile
    });

    return backend;
};

module.exports = {
    install: installApiWithBackend,
    backend: setupBackend
};

var runServer = function(conf) {
    var app = express();
    installApiWithBackend(app, conf);

    var server = app.listen(config.port, function() {
        var host = server.address().address
        var port = server.address().port

        console.log('OpenTSDB/GCE running at http://%s:%s', host, port)
    });
    return server;
};

// command line running
/* istanbul ignore if */
if (require.main === module) {
    var conf = {
        port: 4242
    };

    var args = process.argv.slice(2);
    for (var i=0; i<args.length; i++) {
        switch (args[i]) {
            case '-p':
                conf.port = args[++i];
                break;
            case '-v':
                conf.verbose = true;
                break;
            case '-?':
            case '--help':
                console.log("Usage: node index.js [options]");
                console.log(" -p [port] : Specify the port to bind to")
                console.log(" -v        : Verbose logging")
                console.log(" -? --help : Show this help page")
                break;
            default:
                console.error("Unrecognised option: "+args[i]);
        }
    }

    runServer(conf);

}
