var request = require('supertest')
   , rewire = require('rewire')
 , Emulator = require('google-datastore-emulator');

describe('NodeTSDB GCE Integration Testing', function () {
    var projectId = process.env.GCE_PROJECT_ID || 'nodetsdb-gce-integration-testing';
    var keyFile = process.env.GCE_KEY_FILE || null;
    var namespace = "node-gce-integration-test"
    var usingEmulator = process.env.DATASTORE_EMULATOR_HOST && true;
    var emulator, server, nodetsdb;
    var perTestTimeout = usingEmulator ? 20000 : 120000;

    var deleteEntity = function(datastore, entity, callback) {
        console.log("Selecting all keys for entity "+entity);
        var keysOnlyQuery = datastore.createQuery(namespace, entity).select('__key__');

        datastore.runQuery(keysOnlyQuery, function(err, entities) {
            var keys = entities.map(function(entity) {
                return entity[datastore.KEY];
            });

            console.log("Deleting all keys for entity  "+entity);
            datastore.delete(keys, callback);
        });


    };

    var deleteExistingData = function(datastore, callback) {
        var toDelete = ['ann','data','metric_uid','tagk_uid','tagv_uid','uid_sequence'];

        var deleteIndex = function(index) {
            if (index >= toDelete.length) {
                console.log("Done deleting data");
                callback(null);
                return;
            }

            deleteEntity(datastore, toDelete[index], function(err) {
                if (err) {
                    callback(err);
                    return;
                }

                deleteIndex(index+1);
            });
        };
        deleteIndex(0);
    };

    before(function (done) {
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

        server = runServer({port:4242,verbose:true,projectId:projectId,dataStoreKeyFile:keyFile,namespace:namespace});

        var datastore = nodetsdb.__get__("datastore");
        deleteExistingData(datastore, function(err) {
            if (err) {
                done(false);
                return;
            }

            done();
            // done(success);
        });
    });

    after(function (done) {
        this.timeout(2000);
        if (usingEmulator) {
            // emulator.stop();
        }
        if (server != null) {
            server.close(done);
        }
    });

    step('write single timeseries', function(done) {
        this.timeout(perTestTimeout)
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
                },
                {
                    timestamp: 1524536420,
                    metric: "cpu.percent",
                    value: 29,
                    tags: {
                        host: "host001",
                        type: "user"
                    }
                },
                {
                    timestamp: 1524709220,
                    metric: "cpu.percent",
                    value: 31,
                    tags: {
                        host: "host001",
                        type: "user"
                    }
                },
                {
                    timestamp: 1524795620,
                    metric: "cpu.percent",
                    value: 33,
                    tags: {
                        host: "host001",
                        type: "user"
                    }
                }
            ])
            .expect('Content-Type', /json/)
            .expect(200, {failed:0,success:6})
            .end(done);
    });

    step('query single timeseries', function(done) {
        this.timeout(perTestTimeout)
        request(server)
            .get('/api/query?start=1524450000&end=1524460000&m=sum:cpu.percent&arrays=true&show_tsuids=true&no_annotations=true&global_annotations=false')
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
                    ]
                }
            ])
            .end(done);
    });

    step('query single timeseries over multiple days', function(done) {
        this.timeout(perTestTimeout)
        request(server)
            .get('/api/query?start=1524450000&end=1524795610&m=sum:cpu.percent&arrays=true&show_tsuids=true&no_annotations=true&global_annotations=false')
            .expect('Content-Type', /json/)
            .expect(200, [
                {
                    metric: 'cpu.percent',
                    tags: {}, // todo: should be type and host, but this is caused by api code
                    aggregatedTags: [ 'host', 'type' ], // todo: should be empty, but again, is api code
                    dps: [
                        [ 1524450000, 23 ],
                        [ 1524450010, 25 ],
                        [ 1524450020, 27 ],
                        [ 1524536420, 29 ],
                        [ 1524709220, 31 ]
                    ],
                    tsuids: [
                        "000001000001000001000002000002"
                    ]
                }
            ])
            .end(done);
    });

    step('write multiple timeseries on single metric', function(done) {
        this.timeout(perTestTimeout)
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
        this.timeout(perTestTimeout);
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
    });

    step('search for timeseries under a metric', function(done) {
        this.timeout(perTestTimeout);
        request(server)
            .get('/api/search/lookup?m=disk.used.bytes&use_meta=false&limit=10')
            .expect('Content-Type', /json/)
            .expect(200, {
                "type": "LOOKUP",
                "metric": "disk.used.bytes",
                "limit": 10,
                "time": 1,
                "results": [
                    {
                        "tags": {
                            "host": "host001",
                            "volume": "/dev/sda"
                        },
                        "metric": "disk.used.bytes",
                        "tsuid": "000002000001000001000003000003"
                    },
                    {
                        "tags": {
                            "host": "host001",
                            "volume": "/dev/sdb"
                        },
                        "metric": "disk.used.bytes",
                        "tsuid": "000002000001000001000003000004"
                    }
                ],
                "startIndex": 0,
                "totalResults": 2
            })
            .end(done);
    });

    step('search for timeseries containing a tagk', function(done) {
        this.timeout(perTestTimeout);
        request(server)
            .get('/api/search/lookup?m={host=*}&use_meta=false&limit=10')
            .expect('Content-Type', /json/)
            .expect(200, {
                "type": "LOOKUP",
                "metric": "{host=*}",
                "limit": 10,
                "time": 1,
                "results": [
                    {
                        "tags": {
                            host: "host001",
                            type: "user"
                        },
                        "metric": "cpu.percent",
                        "tsuid": "000001000001000001000002000002"
                    },
                    {
                        "tags": {
                            "host": "host001",
                            "volume": "/dev/sda"
                        },
                        "metric": "disk.used.bytes",
                        "tsuid": "000002000001000001000003000003"
                    },
                    {
                        "tags": {
                            "host": "host001",
                            "volume": "/dev/sdb"
                        },
                        "metric": "disk.used.bytes",
                        "tsuid": "000002000001000001000003000004"
                    }
                ],
                "startIndex": 0,
                "totalResults": 3
            })
            .end(done);
    });

    step('search for timeseries containing a tagv', function(done) {
        this.timeout(perTestTimeout);
        request(server)
            .get('/api/search/lookup?m={*=/dev/sda}&use_meta=false&limit=10')
            .expect('Content-Type', /json/)
            .expect(200, {
                "type": "LOOKUP",
                "metric": "{*=/dev/sda}",
                "limit": 10,
                "time": 1,
                "results": [
                    {
                        "tags": {
                            "host": "host001",
                            "volume": "/dev/sda"
                        },
                        "metric": "disk.used.bytes",
                        "tsuid": "000002000001000001000003000003"
                    }
                ],
                "startIndex": 0,
                "totalResults": 1
            })
            .end(done);
    });

    step('suggest all metrics', function(done) {
        this.timeout(perTestTimeout)
        request(server)
            .get('/api/suggest?type=metrics&q=')
            .expect('Content-Type', /json/)
            .expect(200, ["cpu.percent", "disk.used.bytes"])
            .end(done);
    });

    step('suggest metrics with prefix', function(done) {
        this.timeout(perTestTimeout)
        request(server)
            .get('/api/suggest?type=metrics&q=disk')
            .expect('Content-Type', /json/)
            .expect(200, ["disk.used.bytes"])
            .end(done);
    });

    step('suggest all tag keys', function(done) {
        this.timeout(perTestTimeout)
        request(server)
            .get('/api/suggest?type=tagk&q=')
            .expect('Content-Type', /json/)
            .expect(200, ["host", "type", "volume"])
            .end(done);
    });

    step('suggest tag keys with prefix', function(done) {
        this.timeout(perTestTimeout)
        request(server)
            .get('/api/suggest?type=tagk&q=ho')
            .expect('Content-Type', /json/)
            .expect(200, ["host"])
            .end(done);
    });

    step('suggest all tag values', function(done) {
        this.timeout(perTestTimeout)
        request(server)
            .get('/api/suggest?type=tagv&q=')
            .expect('Content-Type', /json/)
            .expect(200, ["/dev/sda", "/dev/sdb", "host001", "user"])
            .end(done);
    });

    step('suggest tag values with prefix', function(done) {
        this.timeout(perTestTimeout)
        request(server)
            .get('/api/suggest?type=tagv&q=/dev')
            .expect('Content-Type', /json/)
            .expect(200, ["/dev/sda", "/dev/sdb"])
            .end(done);
    });

    step('write annotation on a time series', function(done) {
        this.timeout(perTestTimeout)
       request(server)
           .post('/api/annotation')
           .send({
               tsuid: "000002000001000001000003000003",
               description: "Some description 1",
               notes: "Some notes",
               custom: {
                   custom1a: "custom1b",
                   custom2a: "custom2b"
               },
               startTime: 1524450060,
               endTime: 1524450070
           })
           .expect('Content-Type', /json/)
           .expect(200, {
               tsuid: "000002000001000001000003000003",
               description: "Some description 1",
               notes: "Some notes",
               custom: {
                   custom1a: "custom1b",
                   custom2a: "custom2b"
               },
               startTime: 1524450060,
               endTime: 1524450070
           })
           .end(done);
    });

    step('bulk write annotations on some time series', function(done) {
        this.timeout(perTestTimeout)
       request(server)
           .post('/api/annotation/bulk')
           .send([
               {
                   tsuid: "000002000001000001000003000003",
                   description: "Some description 2",
                   notes: "Some notes",
                   custom: {
                       custom1a: "custom1c",
                       custom2a: "custom2c"
                   },
                   startTime: 1524450124,
                   endTime: null
               },
               {
                   tsuid: "000002000001000001000003000004",
                   description: "Some description 3",
                   notes: "Some notes",
                   custom: {
                       custom1a: "custom1d",
                       custom2a: "custom2d"
                   },
                   startTime: 1524450125,
                   endTime: null
               }
           ])
           .expect('Content-Type', /json/)
           .expect(200, [
               {
                   tsuid: "000002000001000001000003000003",
                   description: "Some description 2",
                   notes: "Some notes",
                   custom: {
                       custom1a: "custom1c",
                       custom2a: "custom2c"
                   },
                   startTime: 1524450124,
                   endTime: null
               },
               {
                   tsuid: "000002000001000001000003000004",
                   description: "Some description 3",
                   notes: "Some notes",
                   custom: {
                       custom1a: "custom1d",
                       custom2a: "custom2d"
                   },
                   startTime: 1524450125,
                   endTime: null
               }
           ])
           .end(done);
    });

    step('write global annotations', function(done) {
        this.timeout(perTestTimeout)
        request(server)
            .post('/api/annotation')
            .send({
                tsuid: 0,
                description: "Some description 4",
                notes: "Some notes",
                custom: {
                    custom1a: "custom1e",
                    custom2a: "custom2e"
                },
                startTime: 1524450060,
                endTime: 1524450070
            })
            .expect('Content-Type', /json/)
            .expect(200, {
                tsuid: 0,
                description: "Some description 4",
                notes: "Some notes",
                custom: {
                    custom1a: "custom1e",
                    custom2a: "custom2e"
                },
                startTime: 1524450060,
                endTime: 1524450070
            })
            .end(done);
    });

    step('query data with annotations on a single timeseries', function(done) {
        this.timeout(perTestTimeout)
        request(server)
            .get('/api/query?start=1524450000&end=1524460000&m=sum:disk.used.bytes{host=host001,volume=/dev/sda}&arrays=true&show_tsuids=true&no_annotations=false&global_annotations=false')
            .expect('Content-Type', /json/)
            .expect(200, [
                {
                    metric: 'disk.used.bytes',
                    tags: {host:"host001",volume:"/dev/sda"},
                    aggregatedTags: [  ],
                    dps: [
                        [ 1524450000, 245333400 ],
                        [ 1524450060, 2453445677 ],
                        [ 1524450120, 2493453234 ]
                    ],
                    tsuids: [
                        "000002000001000001000003000003"
                    ],
                    annotations: [
                        {
                            tsuid: "000002000001000001000003000003",
                            description: "Some description 1",
                            notes: "Some notes",
                            custom: {
                                custom1a: "custom1b",
                                custom2a: "custom2b"
                            },
                            startTime: 1524450060,
                            endTime: 1524450070
                        },
                        {
                            tsuid: "000002000001000001000003000003",
                            description: "Some description 2",
                            notes: "Some notes",
                            custom: {
                                custom1a: "custom1c",
                                custom2a: "custom2c"
                            },
                            startTime: 1524450124,
                            endTime: null
                        }
                    ]
                }
            ])
            .end(done);
    });

    step('query data with annotations on multiple timeseries', function(done) {
        this.timeout(perTestTimeout)
        request(server)
            .get('/api/query?start=1524450000&end=1524460000&m=sum:disk.used.bytes{host=host001}&arrays=true&show_tsuids=true&no_annotations=false&global_annotations=true')
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
                    annotations: [
                        {
                            tsuid: "000002000001000001000003000003",
                            description: "Some description 1",
                            notes: "Some notes",
                            custom: {
                                custom1a: "custom1b",
                                custom2a: "custom2b"
                            },
                            startTime: 1524450060,
                            endTime: 1524450070
                        },
                        {
                            tsuid: "000002000001000001000003000003",
                            description: "Some description 2",
                            notes: "Some notes",
                            custom: {
                                custom1a: "custom1c",
                                custom2a: "custom2c"
                            },
                            startTime: 1524450124,
                            endTime: null
                        },
                        {
                            tsuid: "000002000001000001000003000004",
                            description: "Some description 3",
                            notes: "Some notes",
                            custom: {
                                custom1a: "custom1d",
                                custom2a: "custom2d"
                            },
                            startTime: 1524450125,
                            endTime: null
                        }
                    ],
                    globalAnnotations: [
                        {
                            tsuid: 0,
                            description: "Some description 4",
                            notes: "Some notes",
                            custom: {
                                custom1a: "custom1e",
                                custom2a: "custom2e"
                            },
                            startTime: 1524450060,
                            endTime: 1524450070
                        }
                    ]
                }
            ])
            .end(done);
    });
});
