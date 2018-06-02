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
}

var uidMetaFrom = function (type, identifier, callback) {
    uidKind(type, function(kind, err) {
        if (err) {
            callback(null, err);
            return;
        }

        datastore.get(datastore.key([kind, identifier]), function(err, entity) {
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

        var dsIdentifiers = identifiers.map(function(identifier){return datastore.key([kind, identifier]);});
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
    var uidSequenceKey = datastore.key(["uid_sequence", "kind:"+kind]);

    txn.get(uidSequenceKey, function(err, entity) {
        if (err) {
            // console.log("callback: 1");
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

            var byNameKey = datastore.key([kind, "name:"+name]);

            txn.get(byNameKey, function(err, entity) {
                if (err) {
                    // console.log("callback: 1");
                    callback(null, "Error loading data entity: "+ err);
                    return;
                }
                else if (entity !== undefined) {
                    // console.log("callback: 2");
                    callback(entity.uid);
                    return;
                }

                nextUid(txn, kind, function(uid, err) {
                    if (err) {
                        // console.log("callback: 3");
                        callback(null, 'failed to assign uid: '+ err);
                        return;
                    }

                    var uidString = lpad(uid.toString(16), '0', config[type+"_uid_bytes"]*2);
                    var byIdKey = datastore.key([kind, "id:"+uidString]);

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
                            // console.log("callback: 3");
                            callback(null, 'failed to assign uid: '+ err);
                        }
                        else {
                            // console.log("callback: 4");
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
        .createQuery(entity);
    if (prefix !== "") {
        query = query
            .filter("__key__", ">=", datastore.key([entity, "name:"+prefix]))
            // todo: fudgeroo!!
            .filter("__key__", "<=", datastore.key([entity, "name:"+prefix+"zzzzzzzzzzzzzzzzzz"]))
    }
    else {
        // exclude id: keys
        query = query
            .filter("__key__", ">=", datastore.key([entity, "name:"]))
    }
    query = query.order("__key__");

    if (max) {
        query = query.limit(max);
    }

    datastore
        .runQuery(query, function(err, entities, info) {
            if (err) {
                callback(null, err);
            }
            else {
                if (entities.length > 0) {
                    // entities found
                    var uids = entities;

                    var ret = uids.map(function (uid) {
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
    console.log("withMetricAndTagUids(txn, \""+metric+"\", "+JSON.stringify(incomingTags)+", callback)");
    var metricUidCallback = function(metricUid, err) {
        console.log("metricUidCallback: "+metricUid);
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
                // console.log("Process next tag: "+t);
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
    var rowKey = metricUid + hString + tagUidString;
    return rowKey;
}

// on backend api
backend.storePoints = function(points, storePointsCallback) {
    //console.log("storePoints("+JSON.stringify(points)+")");
    // start simple, process in series, single transaction for each, read entity for timeseries/hour, update entity, write down
    var errors = new Array(points.length);
    if (points.length === 0) {
        storePointsCallback([]);
    }
    var f = function(pointIndex) {
        console.log("Processing point "+pointIndex+": "+JSON.stringify(points[pointIndex]));
        var errorMessage = undefined;
        var point = points[pointIndex];
        if (pointIndex >= points.length) {
            throw "wat"
        }
        var txn = datastore.transaction();
        txn.run(function (err) {
            if (err) {
                errors[pointIndex] = err;
                if (pointIndex >= points.length - 1) {
                    // console.log(pointIndex+": Sending response: " + JSON.stringify(errors));
                    storePointsCallback(errors);
                }
                else {
                    f(pointIndex + 1);
                }
            }
            else {
                var timestamp = point.timestamp;
                var ms = timestamp > 10000000000;
                var hour = Math.floor(ms ? timestamp / 86400000 : timestamp / 86400)
                // force offset to ms
                var offsetFromHour = ms ? timestamp % 86400000 : (timestamp % 86400) * 1000;

                var uidCallback = function(metricUid, tagUidString, tagUidArray, err) {
                    //console.log("uidCallback("+metricUid+","+tagUidString+","+JSON.stringify(tagUidArray)+","+err+")");
                    if (err) {
                        errors.push(errorMessage);
                        return;
                    }
                    else {
                        var rowId = dataRowKey(metricUid, hour, tagUidString);
                    }
                    var rowKey = datastore.key(["data", rowId]);
                    var row = undefined;

                    var processCommitResult = function(err) {
                        if (err) {
                            errorMessage = "Error saving entity: " + err;
                        }
                        errors[pointIndex] = errorMessage;
                        if (pointIndex >= points.length - 1) {
                            //console.log(pointIndex+": Sending response: " + JSON.stringify(errors));
                            storePointsCallback(errors);
                        }
                        else {
                            f(pointIndex + 1);
                        }
                    };

                    txn.get(rowKey, function (err, entity, info) {
                        if (err) {
                            processCommitResult("Error loading data entity: " + err);
                            return;
                        }
                        row = entity;

                        //console.log("New data row? "+(row === undefined));
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
                            console.log("committing write on "+rowKey);
                            txn.commit(processCommitResult);
                        }
                        catch (err) {
                            // console.log("ERR in commit");
                            processCommitResult(err);
                        }
                    });
                };
                withMetricAndTagUids(txn, point.metric, point.tags, uidCallback);
            }
        });
    };
    f(0);
};

// on backend api
backend.searchLookupImpl = function(metric, limit, useMeta, callback) {
    callback([]); // not supported yet
};

// on backend api
backend.storeAnnotations = function(annotations, storeAnnotationsCallback) {
    // start simple, process in series, single transaction for each, read entity for timeseries/hour, update entity, write down
    var errors = new Array(annotations.length);
    if (annotations.length === 0) {
        storeAnnotationsCallback([]);
    }
    var f = function(annotationIndex) {
        // console.log("f: "+pointIndex);
        var errorMessage = undefined;
        var point = annotations[annotationIndex];
        if (annotationIndex >= annotations.length) {
            throw "wat"
        }
        var txn = datastore.transaction();
        txn.run(function (err) {
            if (err) {
                errors[annotationIndex] = err;
                if (annotationIndex >= annotations.length - 1) {
                    // console.log(pointIndex+": Sending response: " + JSON.stringify(errors));
                    storeAnnotationsCallback(errors);
                }
                else {
                    f(annotationIndex + 1);
                }
            }
            else {
                var timestamp = annotations[annotationIndex].startTime;
                var ms = timestamp > 10000000000;
                var hour = Math.floor(ms ? timestamp / 86400000 : timestamp / 86400);
                // force offset to ms
                var offsetFromHour = ms ? timestamp % 86400000 : (timestamp % 86400) * 1000;

                var tagk_string_len = config.tagk_uid_bytes*2;
                var tagv_string_len = config.tagv_uid_bytes*2;
                var tsuid = annotations[annotationIndex].tsuid;
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
                var rowKey = datastore.key(["ann", rowId]);
                var row = undefined;

                var processCommitResult = function(err) {
                    if (err) {
                        errorMessage = "Error saving entity: " + err;
                    }
                    errors[annotationIndex] = errorMessage;
                    if (annotationIndex >= annotations.length - 1) {
                        // console.log(pointIndex+": Sending response: " + JSON.stringify(errors));
                        storeAnnotationsCallback(errors);
                    }
                    else {
                        f(annotationIndex + 1);
                    }
                };

                txn.get(rowKey, function (err, entity, info) {
                    if (err) {
                        processCommitResult("Error loading data entity: " + err);
                        return;
                    }
                    row = entity;

                    //console.log("New data row? "+(row === undefined));
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

                    data[offsetFromHour.toString()] = annotations[annotationIndex];

                    txn.save(row);

                    try {
                        txn.commit(processCommitResult);
                    }
                    catch (err) {
                        // console.log("ERR in commit");
                        processCommitResult(err);
                    }
                });
            }
        });
    };
    f(0);
};

// on backend api
backend.deleteAnnotation = function(annotation, callback) {
    callback(null); // todo
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
            allKeys.push(datastore.key(["ann", keyId]));
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
                // console.log("ANN ROW: "+JSON.stringify(entity));
                // var key = entity[Datastore.KEY].name;
                // console.log("ANN KEY: "+key);

                // var metricUid = key.substring(0, metricUidLength);
                // var hourString = key.substring(metricUidLength, metricUidLength+hourLength);
                // var tagString = key.substring(metricUidLength+hourLength);
                // var tsuid = metricUid+tagString;
                // var hour = parseInt(hourString, 16);

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

var loadData = function(startTime, endTime, keyFn, metricUidString, tagUidsString, callback) {

};

// on backend api
backend.performBackendQueries = function(startTime, endTime, downsample, metric, filters, callback) {
    console.log("performBackendQueries("+startTime+","+endTime+", "+downsample+", "+metric+", "+filters+")");
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
        // now run a query from metricUid+startHour to metricUid+endHour inclusive
        var startHour = Math.floor(startTime.getTime() / 86400000);
        var startOffset = startTime.getTime() % 86400000;
        var endHour = Math.floor(endTime.getTime() / 86400000);
        var endOffset = endTime.getTime() % 86400000;
        // 0000011558811
        // 00000115588 <= x <= 00000115589
        // todo: should be able to query all this in one hit, need to put hour into entity
        var loadRows = function(h, rows) {
            console.log("loadRows("+h+") where endHour = "+endHour)
            if (h<=endHour) {
                var timeFilter;
                if (h !== startHour && h !== endHour) {
                    timeFilter = function() {return true;}
                }
                else if (h === startHour && h === endHour) {
                    // filter both ends
                    timeFilter = function(offset) {
                        return offset >= startOffset && offset <= endOffset;
                    }
                }
                else if (h === startHour) {
                    timeFilter = function(offset) {
                        return offset >= startOffset;
                    }
                }
                else { //if (h == endHour) {
                    timeFilter = function(offset) {
                        return offset <= endOffset;
                    }
                }

                var startKey = datastore.key(["data", dataRowKey(metricUid.uid, h, "")]);
                var endKey = datastore.key(["data", dataRowKey(metricUid.uid, h+1, "")]);
                console.log("Finding data rows where "+JSON.stringify(startKey)+" <= __key__ <= "+JSON.stringify(endKey));

                var runQuery = function(cursor) {
                    console.log(h+": Running query with start cursor: "+cursor);
                    var query = datastore
                        .createQuery("data")
                        .filter("__key__", ">=", startKey)
                        .filter("__key__", "<=", endKey)
                        // .limit(2000000000)
                        .order("__key__");

                    if (cursor) {
                        query = query.start(cursor);
                    }

                    datastore
                        .runQuery(query, function(err, entities, info) {
                            console.log("query result info: "+JSON.stringify(info));
                            if (err) {
                                console.log("CCCC");
                                callback(null, err);
                            }
                            else {
                                if (entities.length > 0) {
                                    // entities found
                                    var rawRows = entities;
                                    console.log("Found "+rawRows.length+" raw rows");

                                    // todo: filter the data!
                                    for (var r=0; r<rawRows.length; r++) {
                                        console.log("ROW: "+JSON.stringify(rawRows[r]));
                                        var row = rawRows[r];

                                        var tagUidString = row.tags.join("");

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
                                            if (timeFilter(offset)) {
                                                var timestamp = (h * 86400000) + offset;
                                                dps.push([timestamp, row[String(offset)]]);
                                            }
                                        }
                                        var toPush = {tags:row.tags, tag_uids:tagUidString, dps:dps};
                                        console.log("Pushing row: "+JSON.stringify(toPush));
                                        rows.push(toPush);
                                    }
                                }
                                else {
                                    console.log("Found 0 raw rows");
                                }

                                if (info.moreResults !== Datastore.NO_MORE_RESULTS && info.endCursor !== cursor) {
                                    console.log("old cursor: "+cursor+", new cursor: "+info.endCursor);
                                    runQuery(info.endCursor);
                                }
                                else {
                                    loadRows(h + 1, rows);
                                }
                            }
                        });

                };
                runQuery(null);
            }
            else {
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
                console.log("tagk_uids = "+JSON.stringify(tagk_uids));

                var tagv_uids = [];
                for (var v in allTagvs) {
                    if (allTagvs.hasOwnProperty(v)) {
                        tagv_uids.push(v);
                    }
                }
                console.log("tagv_uids = "+JSON.stringify(tagv_uids));

                var tagksCallback = function(tagkMetas, err) {
                    if (err) {
                        console.log("AAAA");
                        callback(null, err);
                        return;
                    }
                    console.log("AAAA - ok");
                    console.log("tagkMetas = "+JSON.stringify(tagkMetas));

                    var tagvsCallback = function(tagvMetas, err) {
                        if (err) {
                            console.log("BBBB");
                            callback(null, err);
                            return;
                        }
                        console.log("BBBB - ok");
                        console.log("tagvMetas = "+JSON.stringify(tagvMetas));

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
                                    if (!tagkMetas.hasOwnProperty(k_uid)) {
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

                        console.log("Calling back to API with ret = "+JSON.stringify(ret));
                        callback(ret, null);
                    };
                    multiUidMetaFromUid("tagv", tagv_uids, tagvsCallback);
                };
                multiUidMetaFromUid("tagk", tagk_uids, tagksCallback);
            }
        };
        loadRows(startHour, []);

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
                        console.log("unhandled: "+(typeof from[k]));
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
        dataStoreKeyFile: null
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
