var express = require('express');
const api = require('nodetsdb-api');
require('seedrandom');
// Imports the Google Cloud client library
const Datastore = require('@google-cloud/datastore');
// Your Google Cloud Platform project ID
const projectId = 'opentsdb-cloud';

// Creates a client
const datastore = new Datastore({
    projectId: projectId
});

var log = console;


var config = {
    
};

var backend = {};

var lpad = function (input, char, length) {
    while (input.length < length) {
        input = char + input;
    }
    return input;
}

var uidMetaFrom = function (type, identifier, callback) {
    var kind;
    switch (type) {
        case "metric": kind = "metric_uid"; break;
        case "tagk": kind = "tagk_uid"; break;
        case "tagv": kind = "tagv_uid"; break;
        default:
            callback(null, 'Unsupported type: '+type);
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
};

var multiUidMetaFrom = function (type, identifiers, callback) {
    var kind;
    switch (type) {
        case "metric": kind = "metric_uid"; break;
        case "tagk": kind = "tagk_uid"; break;
        case "tagv": kind = "tagv_uid"; break;
        default:
            callback(null, 'Unsupported type: '+type);
            return;
    }

    if (identifiers.length === 0) {
        callback([]);
    }

    var dsIdentifiers = identifiers.map(function(identifier){return datastore.key([kind, identifier]);});
    datastore.get(dsIdentifiers, function(err, entities) {
        if (err) {
            callback(null, err);
        }
        else {
            var ret = {};
            for (var i=0; i<identifiers.length; i++) {
                ret[identifiers[i]] = entities[i];
            }
            callback(ret, null);
        }
    });
};

// on backend api
var uidMetaFromName = function(type, name, callback) {
    uidMetaFrom(type, "name:"+name, callback);
};

// on backend api
backend.uidMetaFromUid = function(type, uid, callback) {
    uidMetaFrom(type, "id:"+uid, callback);
};

var multiUidMetaFromUid = function(type, uids, callback) {
    var cb = function(metas, err) {
        if (err) {
            callback(null, err);
        }
        else {
            var newMap = {};
            for (var k in metas) {
                if (metas.hasOwnProperty(k)) {
                    newMap[k.substring(3)] = metas[k];
                }
            }
            callback(newMap);
        }
    };
    multiUidMetaFrom(type, uids.map(function(uid) { return "id:"+uid; }), cb);
};

var assignUidIfNecessary = function(type, name, callback) {
    // console.log("assignUidIfNecessary: "+type+", "+name);
    var kind;
    switch (type) {
        case "metric": kind = "metric_uid"; break;
        case "tagk": kind = "tagk_uid"; break;
        case "tagv": kind = "tagv_uid"; break;
        default:
            callback(null, 'Unsupported type: '+type);
            return;
    }

    var txn = datastore.transaction();

    txn.run(function (err) {
        // console.log("assignUidIfNecessary: in txn.run()")

        if (err) {
            callback(null, 'Error creating transaction: '+ err);
            return;
        }

        var allDone = false;
        const byNameKey = datastore.key([kind, "name:"+name]);

        txn.get(byNameKey, function(err, entity) {
            if (err) {
                // console.log("callback: 1");
                callback(null, "Error loading data entity: "+ err);
                allDone = true;
            }
            else if (entity !== undefined) {
                // console.log("callback: 2");
                callback(entity.uid);
                allDone = true;
            }


            if (allDone) {
                // console.log("callback: allDone");
                return;
            }

            var nextUid = 1; // todo: get this from db
            var uidString = lpad(nextUid.toString(16), '0', 6);
            const byIdKey = datastore.key([kind, "id:"+uidString]);

            const byNameEntity = {
                key: byNameKey,
                data: {
                    uid: uidString,
                    name: name
                }
            };
            const byIdEntity = {
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
                    callback(null, 'failed to assign uid', err);
                }
                else {
                    // console.log("callback: 4");
                    callback(uidString);
                }
            });
        });
    });
};

// on backend api
backend.suggestMetrics = function(prefix, max, callback) {
    if (!prefix) {
        prefix = "";
    }
    var query = datastore
        .createQuery("metric_uid")
        .filter("__key__", ">", datastore.key(["metric_uid", "name:"+prefix]))
        .order("__key__");

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
                    const uids = entities[0];

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
backend.suggestTagKeys = function(prefix, max, callback) {
    callback([]); // todo
};

// on backend api
backend.suggestTagValues = function(prefix, max, callback) {
    callback([]); // todo
};

var withMetricAndTagUids = function(txn, metric, incomingTags, callback) {
    var metricUidCallback = function(metricUid, err) {
        // console.log("metricUidCallback: "+metricUid);
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
                            callack(null, null, err);
                        }
                        else {
                            tagkUids.push(tagk_uid);
                            tags[tagk_uid] = tagv_uid;
                            t++;
                            if (!hadError) {
                                processNextTag(t + 1);
                            }
                        }
                    };
                    var tagkCallback = function (tagk_uid, err) {
                        if (err) {
                            hadError = true;
                            callack(null, null, err);
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

// on backend api
backend.storePoints = function(points, storePointsCallback) {
    // start simple, process in series, single transaction for each, read entity for timeseries/hour, update entity, write down
    var errors = new Array(points.length);
    if (points.length === 0) {
        storePointsCallback([]);
    }
    var f = function(pointIndex) {
        // console.log("f: "+pointIndex);
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
                var hour = Math.floor(ms ? timestamp / 86400000 : timestamp / 86400);
                // force offset to ms
                var offsetFromHour = ms ? timestamp % 86400000 : (timestamp % 86400) * 1000;

                var uidCallback = function(metricUid, tagUidString, tagUidArray, err) {
                    if (err) {
                        errors.push(errorMessage);
                        return;
                    }
                    else {
                        var rowId = metricUid + hour + tagUidString;
                    }
                    var rowKey = datastore.key(["data", rowId]);
                    var row = undefined;

                    var processCommitResult = function(err) {
                        if (err) {
                            errorMessage = "Error saving entity: " + err;
                        }
                        errors[pointIndex] = errorMessage;
                        if (pointIndex >= points.length - 1) {
                            // console.log(pointIndex+": Sending response: " + JSON.stringify(errors));
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

                        console.log("New data row? "+(row === undefined));
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
backend.performAnnotationsQueries = function(startTime, endTime, downsampleSeconds, participatingTimeSeries, callback) {
    callback([]); // not supported yet
};

// on backend api
backend.performGlobalAnnotationsQuery = function(startTime, endTime, callback) {
    callback([]); // not supported yet
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

                var startKey = datastore.key(["data", metricUid.uid+h]);
                var endKey = datastore.key(["data", metricUid.uid+(h+1)]);
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
                                    var rawRows = entities[0];
                                    if (!(rawRows instanceof Array)) {
                                        rawRows = [rawRows];
                                    }
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
                                        rows.push({tags:row.tags, tag_uids:tagUidString, dps:dps});
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

                var tagv_uids = [];
                for (var v in allTagvs) {
                    if (allTagvs.hasOwnProperty(v)) {
                        tagv_uids.push(v);
                    }
                }

                var tagksCallback = function(tagkMetas, err) {
                    if (err) {
                        console.log("AAAA");
                        callback(null, err);
                        return;
                    }

                    var tagvsCallback = function(tagvMetas, err) {
                        if (err) {
                            console.log("BBBB");
                            callback(null, err);
                            return;
                        }

                        var ret = [];

                        var lastUidString = undefined;
                        var currentDps = [];
                        var currentTags = {};

                        for (var r=0; r<rows.length; r++) {
                            if (lastUidString !== undefined && rows[r].tag_uids !== lastUidString) {
                                ret.push({
                                    metric: metric,
                                    metric_uid: metricUid,
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
                                    var k = tagkMetas[k_uid].name;
                                    tags[k] = tagvMetas[v_uid].name;
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

var uid = function(type, name) {
    var meta = backend.uidMetaFromName(type, name);
    if (meta != null) {
        return meta.uid;
    }
    return null;
};

var tsuid = function(metric, tags) {
    var ret = uid("metric", metric);
    for (var k in tags) {
        if (tags.hasOwnProperty(k)) {
            ret += uid("tagk", k) + uid("tagv", tags[k]);
        }
    }
    return ret;
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

var installBackend = function(app, incomingConfig) {
    if (!incomingConfig) {
        incomingConfig = {};
    }

    var conf = {
        verbose: false,
        logRequests: true,
        version: "2.2.0"
    };

    applyOverrides(incomingConfig, conf);

    config = conf;

    api.backend(backend);
    api.install(app, config);
}

module.exports = {
    install: installBackend
}

// command line running
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

    var app = express();
    installBackend(app, conf);

    var server = app.listen(config.port, function() {
        var host = server.address().address
        var port = server.address().port

        console.log('OpenTSDB/GCE running at http://%s:%s', host, port)
    });

}