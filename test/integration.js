var request = require('supertest')
     , util = require('util')
   , assert = require('assert')
   , rewire = require('rewire')
 , Emulator = require('google-datastore-emulator');

function errorExpectedActual(msg, expected, actual) {
    var err = new Error(msg);
    err.expected = expected;
    err.actual = actual;
    err.showDiff = true;
    return err;
}
function errorActual(msg, actual) {
    var err = new Error(msg);
    err.actual = actual;
    err.showDiff = true;
    return err;
}
function assertArrayContainsOnly(arrayDesc, expected, actual) {
    if (actual.length != 4) {
        return errorExpectedActual('expected '+arrayDesc+' of length '+expected.length+', got ' + actual.length, expected.length, actual.length);
    }
    for (var i=0; i<expected.length; i++) {
        var lookFor = expected[i];
        if (actual.indexOf(lookFor) < 0) {
            return errorActual('expected '+arrayDesc+' to contain '+JSON.stringify(lookFor)+', but was ' + JSON.stringify(actual), actual);
        }
    }
}

describe('NodeTSDB GCE Integration Testing', function () {
    var projectId = 'nodetsdb-gce-integration-testing';
    var emulator, server, nodetsdb;

    before(function () {
        this.timeout(120000);

        var options = {
            projectId: projectId,
            storeOnDisk: false,
            host: "localhost",
            port: 8082
        };

        emulator = new Emulator(options);

        // var success = emulator.start();

        nodetsdb = rewire('../index');
        var runServer = nodetsdb.__get__("runServer");
        server = runServer({port:4242,verbose:false,projectId:projectId});

        // return success;
    });

    after(function (done) {
        this.timeout(2000);
        // emulator.stop();
        server.close(done);
    });

    step('write single timeseries', function(done) {
        this.timeout(20000);
        request(server)
            .post('/api/put?summary')
            .send([
                {
                    timestamp: 1524450000,
                    metric: "cpu.percent",
                    value: 23,
                    tags: {
                        host: "host001",
                        type: "user"
                    }
                },
                {
                    timestamp: 1524450010,
                    metric: "cpu.percent",
                    value: 25,
                    tags: {
                        host: "host001",
                        type: "user"
                    }
                },
                {
                    timestamp: 1524450020,
                    metric: "cpu.percent",
                    value: 27,
                    tags: {
                        host: "host001",
                        type: "user"
                    }
                }
            ])
            .expect('Content-Type', /json/)
            .expect(200, {failed:0,success:3})
            .end(done);
    });

    step('query single timeseries', function(done) {
        request(server)
            .get('/api/query?start=1524450000&end=1524460000&m=sum:cpu.percent&arrays=true&show_tsuids=true')
            .expect('Content-Type', /json/)
            .expect(200, [
                {
                    metric: 'cpu.percent',
                    tags: {}, // todo: should be type and host, but this is caused by api code
                    aggregatedTags: [ 'host', 'type' ], // todo: should be empty, but again, is api code
                    dps: [
                        [ 1524450000, 23 ],
                        [ 1524450010, 25 ],
                        [ 1524450020, 27 ]
                    ],
                    tsuids: [
                        "000001000001000001000002000002"
                    ],
                    annotations: []
                }
            ])
            .end(done);
    });

    step('write multiple timeseries on single metric', function(done) {
        this.timeout(20000);
        request(server)
            .post('/api/put?summary')
            .send([
                {
                    timestamp: 1524450000,
                    metric: "disk.used.bytes",
                    value: 245333400,
                    tags: {
                        host: "host001",
                        volume: "/dev/sda"
                    }
                },
                {
                    timestamp: 1524450060,
                    metric: "disk.used.bytes",
                    value: 2453445677,
                    tags: {
                        host: "host001",
                        volume: "/dev/sda"
                    }
                },
                {
                    timestamp: 1524450120,
                    metric: "disk.used.bytes",
                    value: 2493453234,
                    tags: {
                        host: "host001",
                        volume: "/dev/sda"
                    }
                },
                {
                    timestamp: 1524450000,
                    metric: "disk.used.bytes",
                    value: 45333400,
                    tags: {
                        host: "host001",
                        volume: "/dev/sdb"
                    }
                },
                {
                    timestamp: 1524450060,
                    metric: "disk.used.bytes",
                    value: 453445677,
                    tags: {
                        host: "host001",
                        volume: "/dev/sdb"
                    }
                },
                {
                    timestamp: 1524450120,
                    metric: "disk.used.bytes",
                    value: 493453234,
                    tags: {
                        host: "host001",
                        volume: "/dev/sdb"
                    }
                },
            ])
            .expect('Content-Type', /json/)
            .expect(200, {failed:0,success:6})
            .end(done);

    });

    step('read aggregation of multiple timeseries', function(done) {
        request(server)
            .get('/api/query?start=1524450000&end=1524460000&m=sum:disk.used.bytes{host=host001}&arrays=true&show_tsuids=true')
            .expect('Content-Type', /json/)
            .expect(200, [
                {
                    metric: 'disk.used.bytes',
                    tags: {host:"host001"},
                    aggregatedTags: [ 'volume' ],
                    dps: [
                        [ 1524450000, 290666800 ],
                        [ 1524450060, 2906891354 ],
                        [ 1524450120, 2986906468 ]
                    ],
                    tsuids: [
                        "000002000001000001000003000003",
                        "000002000001000001000003000004"
                    ],
                    annotations: []
                }
            ])
            .end(done);
    });

    step('suggest with prefix', function(done) {
        request(server)
            .get('/api/suggest?type=metrics&q=disk')
            .expect('Content-Type', /json/)
            .expect(200, ["disk.used.bytes"])
            .end(done);
    });

    step('suggest all', function(done) {
        request(server)
            .get('/api/suggest?type=metrics&q=')
            .expect('Content-Type', /json/)
            .expect(200, ["cpu.percent","disk.used.bytes"])
            .end(done);
    });
});
