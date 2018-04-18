var request = require('supertest')
     , util = require('util')
   , assert = require('assert');

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

describe('Inline FakeTSDB', function () {
    var server, faketsdb;

    beforeEach(function () {
        var app = require('express')();
        faketsdb = require('../faketsdb');
        faketsdb.reset();
        faketsdb.install(app, {logRequests:false});

        server = app.listen(4242);
    });

    afterEach(function (done) {
        server.close(done);
    });

    it('responds to GET  /api/query with consistent data each call', function(done) {
        this.timeout(12000);
        faketsdb.addTimeSeries("some.metric", {"host":"host1"}, "gauge");

        var timeToNext10s = 10000 - new Date().getTime() % 10000;

        setTimeout(function () {
            var firstBody = [];
            request(server)
                .get('/api/query?start=1m-ago&m=sum:10s-avg:some.metric{host=host1}&arrays=true')
                .expect('Content-Type', /json/)
                .expect(200)
                .expect(function(res) {
                    firstBody = res.body;
                })
                .end(function () {
                    var secondBody = [];
                    request(server)
                        .get('/api/query?start=1m-ago&m=sum:10s-avg:some.metric{host=host1}&arrays=true')
                        .expect('Content-Type', /json/)
                        .expect(200, firstBody)
                        .end(done);
                });
        }, timeToNext10s+500);
    });
});