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
    var projectId = process.env.GCE_PROJECT_ID || 'nodetsdb-gce-integration-testing';
    var usingEmulator = process.env.DATASTORE_EMULATOR_HOST && true;
    var emulator, server, nodetsdb;
    var perTestTimeout = usingEmulator ? 20000 : 120000;

    before(function () {
        this.timeout(120000);

        if (usingEmulator) {
            var options = {
                projectId: projectId,
                storeOnDisk: false,
                host: "localhost",
                port: 8082
            };

            emulator = new Emulator(options);
        }

        // var success = emulator.start();

        nodetsdb = rewire('../index');
        var runServer = nodetsdb.__get__("runServer");

        server = runServer({port:4242,verbose:true,projectId:projectId});

        // return success;
    });

    after(function (done) {
        this.timeout(2000);
        if (usingEmulator) {
            // emulator.stop();
        }
        server.close(done);
    });

    var doTest = function(done) {
        request(server)
            .get('/api/query?start=1524450000&end=1524460000&m=sum:disk.used.bytes{host=host001}&arrays=true&show_tsuids=true&no_annotations=true&global_annotations=false')
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
                    ]
                }
            ])
            .end(done);
    };

    step('read aggregation of multiple timeseries', function(done) {
        this.timeout(perTestTimeout*20);
        var _t, _err, _res, _n = 0;
        var ownDone = function(t, err, res) {
            _t = t;
            _err = err;
            _res = res;

            _n++;
            if (_n <= 20 && !err) {
                doTest(ownDone);
            }
            else {
                done(t, err, res);
            }
        };

        doTest(ownDone);

    });
});
