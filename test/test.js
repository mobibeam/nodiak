// (The MIT License)

// Copyright (c) 2012 Coradine Aviation Systems
// Copyright (c) 2012 Nathan Aschbacher

// Permission is hereby granted, free of charge, to any person obtaining
// a copy of this software and associated documentation files (the
// 'Software'), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so, subject to
// the following conditions:

// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
// IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
// CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
// TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
// SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

describe("Nodiak Riak Client Test Suite", function() {
    var TIMEOUT = process.env.TIMEOUT || 50000;
    var NUM_OBJECTS = parseInt(process.env.NUM_OBJECTS, 10) || 1000;
    var backend = process.env.NODIAK_BACKEND || 'http';
    var host = process.env.NODIAK_HOST || 'localhost';
    var port = process.env.NODIAK_PORT || '8098';
    var search_enabled = process.env.NODIAK_SEARCH && process.env.NODIAK_SEARCH != 'false' ? true : false;
    var twoi_enabled = process.env.NODIAK_2I && process.env.NODIAK_2I != 'false'? true : false;

    var riak = require('../index.js').getClient(backend, host, port);
    
    var async = require('async');
    var should = require('should');
    var Map = require("../lib/map");
    var Counter = require("../lib/counter");
    var Set = require("../lib/set");

    before(function(done){ // bootstrap settings and data for tests.
        this.timeout(TIMEOUT);
        riak.ping(function(err, response) {
            if(err) throw new Error(err.toString());
            else {
                var search_hook = search_enabled ? {precommit:[{mod:"riak_search_kv_hook",fun:"precommit"}]} : {precommit:[]};

                riak._bucket.save('nodiak_test', search_hook, function(err, result) {
                    if(err) throw new Error(err.toString());
                    else {
                        var data = { field1: "has been set" };
                        var metadata = {
                            index: {
                                strings: { bin: ['this', 'that', 'the', 'other'] },
                                numbers: { int: [1000,250,37,4234,5] }
                            },
                            meta: { details: "you might want to know" }
                        };
                        var created = [];
                        for(var i = 1; i <= NUM_OBJECTS; i++) {
                            riak._object.save('nodiak_test', i, data, metadata, function(err, obj) {
                                if(err) throw new Error(err.toString());
                                else {
                                    created.push(obj);
                                }

                                if(created.length == NUM_OBJECTS) {
                                    done();
                                }
                            });
                        }
                    }
                });
            }
        });
    });

    describe("Basic connection functionality", function() {
        it("should be able to ping the cluster via "+backend.toUpperCase(), function(done) {
            riak.ping(function(err, response) {
                should.not.exist(err);
                response.should.equal("OK");
                done();
            });
        });

        it("should be able to get stats via "+backend.toUpperCase(), function(done) {
            riak.stats(function(err, response) {
                should.not.exist(err);
                response.should.be.an.Object;
                done();
            });
        });

        it("should be able to list resources via "+backend.toUpperCase(), function(done) {
            riak.resources(function(err, response) {
                should.not.exist(err);
                response.should.be.an.Object;
                done();
            });
        });
    });

    describe("Using the base client to interact with buckets", function() {
        it("should be able to read bucket properties", function(done) {
            riak._bucket.props('random_bucket', function(err, props) {
                should.not.exist(err);
                props.should.be.an.Object.and.have.property('name', 'random_bucket');
                done();
            });
        });

        it("should be able to save properties to a bucket", function(done) {
            riak._bucket.props('random_bucket', function(err, props) {
                should.not.exist(err);

                var toggled = props.allow_mult ? false : true;
                props.allow_mult = toggled;

                riak._bucket.save('random_bucket', props, function(err, response) {
                    should.not.exist(err);
                    
                    riak._bucket.props('random_bucket', function(err, props) {
                        should.not.exist(err);
                        props.allow_mult.should.equal(toggled);
                        done();
                    });
                });
            });
        });

        it("should be able to list all buckets", function(done) {
            riak._bucket.list(function(err, buckets) {
                should.not.exist(err);
                buckets.should.be.an.instanceOf(Array);
                done();
            });
        });

        it("should be able to list all keys in a bucket", function(done) {
            riak._bucket.keys('nodiak_test').stream()
            .on('data', function(data) {
                data.should.be.an.instanceOf(Array);
            })
            .on('end', function() {
                done();
            })
            .on('error', function(err) {
                should.not.exist(err);
            });
        });
    });

    describe("Using the base client to interact with objects", function() {
        it("should be able to check existence of object", function(done) {
            riak._object.save('nodiak_test', 'this_ol_key', { "pointless": "data" }, null, function(err, obj) {
                should.not.exist(err);

                riak._object.exists('nodiak_test', 'this_ol_key', function(err, exists) {
                    should.not.exist(err);
                    exists.should.be.true;

                    riak._object.exists('nodiak_test', 'no_key_here', function(err, exists) {
                        should.not.exist(err);
                        exists.should.be.false;
                        done();
                    });
                });
            });
        });

        it("should be able to save an object", function(done) {
            riak._object.save('nodiak_test', 'this_ol_key', { "pointless": "data" }, null, function(err, obj) {
                should.not.exist(err);
                obj.should.be.an.Object.and.have.property('key', 'this_ol_key');
                obj.should.be.an.Object.and.have.property('data');
                obj.data.should.eql({ "pointless": "data" });
                done();
            });
        });

        it("should be able to get an object", function(done) {
            riak._object.get('nodiak_test', 'this_ol_key', function(err, obj) {
                should.not.exist(err);
                obj.should.be.an.Object.and.have.property('key', 'this_ol_key');
                obj.should.be.an.Object.and.have.property('data');
                obj.data.should.eql({ "pointless": "data" });
                obj.metadata.should.be.an.Object.and.have.property('vclock');
                done();
            });
        });

        it("should be able to delete an object", function(done) {
            riak._object.delete('nodiak_test', 'this_ol_key', function(err, obj) {
                should.not.exist(err);
                obj.metadata.status_code.should.equal(204);
                done();
            });
        });

        it("should be able to get sibling vtags when siblings exist", function(done) {
            riak._bucket.save('siblings_test', { allow_mult: true }, function(err, response) {
                should.not.exist(err);

                riak._object.save('siblings_test', 'this_ol_key', { "pointless": "data" }, { meta: { extra: "meta data goes here"} }, function(err, obj) {
                    should.not.exist(err);
                    obj.should.be.an.Object.and.have.property('key', 'this_ol_key');
                    obj.should.be.an.Object.and.have.property('data');
                    obj.data.should.eql({ "pointless": "data" });
                
                    riak._object.save('siblings_test', 'this_ol_key', { "pointless": "sibling" }, { meta: {extra: "meta data goes EVERYWHERE"} }, function(err, obj) {
                        should.not.exist(err);
                        obj.should.be.an.Object.and.have.property('key', 'this_ol_key');
                        obj.should.be.an.Object.and.have.property('data');
                        obj.data.should.eql({ "pointless": "sibling" });

                        riak._object.get('siblings_test', 'this_ol_key', function(err, obj) {
                            should.not.exist(err);
                            obj.should.be.an.Object.and.have.property('siblings');
                            obj.siblings.should.be.an.instanceof(Array);
                            obj.metadata.should.be.an.Object.and.have.property('vclock');
                            done();
                        });
                    });
                });
            });
        });
    });

    describe("Using the base client to perform 2i queries", function() {
        if(twoi_enabled) {
            it("should be able to perform ranged integer 2i searches", function(done) {
                riak._bucket.twoi('nodiak_test', [0,10000], 'numbers_int', {}, function(err, keys) {
                    should.not.exist(err);
                    keys.should.be.an.instanceOf(Array).with.lengthOf(NUM_OBJECTS * 5);
                    done();
                });
            });

            it("should be able to perform exact match integer 2i searches", function(done) {
                riak._bucket.twoi('nodiak_test', 1000, 'numbers_int', {}, function(err, keys) {
                    should.not.exist(err);
                    keys.should.be.an.instanceOf(Array).with.lengthOf(NUM_OBJECTS);
                    done();
                });
            });

            it("should be able to perform ranged binary 2i searches", function(done) {
                riak._bucket.twoi('nodiak_test', ['a','zzzzzzzzzzz'], 'strings_bin', {}, function(err, keys) {
                    should.not.exist(err);
                    keys.should.be.an.instanceOf(Array).with.lengthOf(NUM_OBJECTS * 4);
                    done();
                });
            });

            it("should be able to perform exact match binary 2i searches", function(done) {
                riak._bucket.twoi('nodiak_test', 'that', 'strings_bin', {}, function(err, keys) {
                    should.not.exist(err);
                    keys.should.be.an.instanceOf(Array).with.lengthOf(NUM_OBJECTS);
                    done();
                });
            });
        }
        else {
            return;
        }
    });

    describe("Using the base client to perform Riak Search queries", function() {
        if(search_enabled) {
            it("should be able to perform solr search on indexed bucket", function(done) {
                riak._bucket.solr('nodiak_test', { q: 'field1:been' }, function(err, obj) {
                    should.not.exist(null);
                    obj.data.should.have.property('response');
                    obj.data.response.should.have.property('numFound', NUM_OBJECTS);
                    obj.data.response.should.have.property('docs').with.lengthOf(10);
                    done();
                });
            });
        }
        else {
            return;
        }
    });

    describe("Performing MapReduce queries", function() {
        it("should be able to handle streamed results", function(done) {
            riak.mapred.inputs('nodiak_test')
                .map({
                    language: 'erlang',
                    module: 'riak_kv_mapreduce',
                    function: 'map_object_value',
                    arg: 'filter_notfound'})
                .reduce({
                    language: 'erlang',
                    module: 'riak_kv_mapreduce',
                    function: 'reduce_count_inputs'})
                .execute().stream()
                .on('data', function(result) {
                    result.should.be.an.Object;
                    result.data.should.be.an.instanceOf(Array);
                    result.data[0].should.equal(NUM_OBJECTS + 1);
                })
                .on('end', function() {
                    done();
                })
                .on('error', function(err) {
                    should.not.exist(err);
                });
        });

        it("should be able to handle non-streamed results", function(done) {
             riak.mapred.inputs('nodiak_test')
                .map({
                    language: 'erlang',
                    module: 'riak_kv_mapreduce',
                    function: 'map_object_value',
                    arg: 'filter_notfound'})
                .reduce({
                    language: 'erlang',
                    module: 'riak_kv_mapreduce',
                    function: 'reduce_count_inputs'})     
                .execute(function(err, result) {
                    should.not.exist(err);
                    result.should.be.an.Object;
                    result.data.should.be.an.instanceOf(Array);
                    result.data[0].should.equal(NUM_OBJECTS + 1);
                    done();
                }
            );
        });
    });

    describe("Using the 'Bucket' class to interact with buckets and objects", function() {
        it("should be able to get a Bucket instance from the base client", function(done) {
            var bucket = riak.bucket('some_bucket');

            bucket.should.have.property('constructor');
            bucket.constructor.should.have.property('name', 'Bucket');
            bucket.name.should.equal('some_bucket');
            done();
        });

        it("should be able to fetch props from Riak", function(done) {
            var bucket = riak._bucket.get('nodiak_test');

            bucket.getProps(function(err, props) {
                should.not.exist(err);

                props.should.have.property('name');
                props.name.should.equal('nodiak_test');
                bucket.props.name.should.eql(props.name);

                done();
            });
        });

        it("should be able to save its props to Riak", function(done) {
            var bucket = riak._bucket.get('nodiak_test');

            bucket.props.last_write_wins = true;

            bucket.saveProps(function(err, saved) {
                should.not.exist(err);

                bucket.props.should.have.property('last_write_wins', true);
                bucket.props.last_write_wins.should.equal(saved.props.last_write_wins);

                bucket.props.last_write_wins = false;

                bucket.saveProps(function(err, saved) {
                    should.not.exist(err);

                    bucket.props.should.have.property('last_write_wins', false);
                    bucket.props.last_write_wins.should.equal(saved.props.last_write_wins);

                    done();
                });
            });
        });

        it("should be able to get RObject w/ siblings fetched as async requests for vtags", function(done) {
            var bucket = riak.bucket('siblings_test');
            bucket.objects.get('this_ol_key', function(err, obj) {
                should.not.exist(err);
                obj.constructor.name.should.eql('RObject');
                obj.should.have.property('siblings');
                obj.siblings.should.be.an.instanceOf(Array).with.lengthOf(2);
                obj.siblings[0].constructor.name.should.eql('RObject');
            });
            done();
        });
        
        it("should be able to get RObject w/ siblings as one request for multipart/mixed objects", function(done) {
            var bucket = riak.bucket('siblings_test');
            bucket.getSiblingsSync = true;
            bucket.objects.get('this_ol_key', function(err, obj) {
                should.not.exist(err);
                obj.constructor.name.should.eql('RObject');
                obj.should.have.property('siblings');
                obj.siblings.should.be.an.instanceOf(Array).with.lengthOf(2);
                obj.siblings[0].constructor.name.should.eql('RObject');
            });
            done();
        });
    });

    describe("Using the 'Bucket' class to perform 2i queries", function() {
        if(twoi_enabled) {
            it("should be able to perform ranged 2i searches, stream results as keys", function(done) {
                var all_results = [];
                riak.bucket('nodiak_test').search.twoi([0,10000], 'numbers').stream()
                .on('data', function(key) {
                    key.constructor.name.should.eql('String');
                    all_results.push(key);
                })
                .on('error', function(err) {
                    should.not.exist(err);
                })
                .on('end', function() {
                    all_results.length.should.eql(NUM_OBJECTS);
                    done();
                });
            });

            it("should be able to perform ranged 2i searches, results as keys", function(done) {
                riak.bucket('nodiak_test').search.twoi([0,10000], 'numbers', function(err, response) {
                    should.not.exist(err);
                    response.should.be.an.instanceOf(Array).with.lengthOf(NUM_OBJECTS);

                    response[0].constructor.name.should.eql('String');
                    done();
                });
            });

            it("should be able to perform exact match 2i searches, stream results as keys", function(done) {
                var all_results = [];
                riak.bucket('nodiak_test').search.twoi('that', 'strings').stream()
                .on('data', function(key) {
                    key.constructor.name.should.eql('String');
                    all_results.push(key);
                })
                .on('error', function(err) {
                    should.not.exist(err);
                })
                .on('end', function() {
                    all_results.length.should.eql(NUM_OBJECTS);
                    done();
                });
            });

            it("should be able to perform exact match 2i searches, results as keys", function(done) {
                riak.bucket('nodiak_test').search.twoi('that', 'strings', function(err, response) {
                    should.not.exist(err);
                    response.should.be.an.instanceOf(Array).with.lengthOf(NUM_OBJECTS);

                    response[0].constructor.name.should.eql('String');
                    done();
                });
            });
        }
        else {
            return;
        }
    });

    describe("Using the 'Bucket' class to perform Riak Search queries", function() {
        if(search_enabled) {
            it("should be able to perform Solr search on indexed bucket, stream results as RObjects", function(done) {
                var all_results = [];
                riak.bucket('nodiak_test').search.solr({ q: 'field1:been' }).stream()
                .on('data', function(r_obj) {
                    r_obj.constructor.name.should.eql('RObject');
                    all_results.push(r_obj);
                })
                .on('error', function(err) {
                    should.not.exist(err);
                })
                .on('end', function() {
                    all_results.length.should.eql(10);
                    done();
                });
            });

            it("should be able to perform Solr search on indexed bucket", function(done) {
                riak.bucket('nodiak_test').search.solr({ q: 'field1:been' }, function(err, results) {
                    should.not.exist(err);
                    results.should.have.property('response');
                    results.response.should.have.property('numFound');
                    results.response.numFound.should.eql(NUM_OBJECTS);

                    results.response.should.have.property('docs');
                    results.response.docs.should.be.an.instanceOf(Array).with.lengthOf(10);
                    results.response.docs[0].constructor.name.should.eql('Object');
                    done();
                });
            });
        }
        else {
            return;
        }
    });

    describe("Using the 'RObject' class to perform fetch/save/delete operations", function() {
        var obj = riak.bucket('nodiak_test').object.new('1');

        it("should be able to hydrate an uninitialized RObject from Riak", function(done) {
            obj.fetch(function(err, result) {
                should.not.exist(err);
                done();
            });
        });

        it("should be able to save a hydrated RObject to Riak", function(done) {
            obj.save(function(err, result) {
                should.not.exist(err);
                done();
            });
        });

        it("should be able to delete a hydrate a RObject from Riak", function(done) {
            obj.delete(function(err, result) {
                should.not.exist(err);
                done();
            });
        });
    });

    describe("Using the 'Counter' class to perform CRDT Counter operations", function() {
        var startvalue;
        var bucket = riak.bucket('counter_test');
        bucket.props.allow_mult = true;

        it("should be able to set bucket properties for counter", function(done){
            bucket.saveProps(function(err, response) {
                should.not.exist(err);
                done();
            })
        });

        it("should be able to get the initial value of a counter", function(done) {
            riak.counter('counter_test', 'the_count').value(function(err, response) {
                var type = typeof(response);
                type.should.eql('number');
                if (!response){
                    startvalue = 0;
                }else{
                    startvalue = response;
                }
                done();
            });
        });


        it("should be able to add to a counter", function(done) {
            riak.counter('counter_test', 'the_count').add(4, function(err, response) {
                should.not.exist(err);
                done();
            });
        });

        it("should be able to subtract from a counter", function(done) {
            riak.counter('counter_test', 'the_count').subtract(2, function(err, response) {
                should.not.exist(err);
                done();
            });
        });

        it("should be able to get the value of a counter", function(done) {
            riak.counter('counter_test', 'the_count').value(function(err, response) {
                should.not.exist(err);
                var type = typeof(response);
                type.should.eql('number');
                response.should.eql(2+startvalue);
                done();
            });
        });

    });


    describe("Using the 'Set' class to perform CRDT Counter operations", function() {
        it("should be able to add array to set", function(done) {
            riak.set('set_test', 'the_set').add("str1", function(err, response) {
                should.not.exist(err);
                done();
            });
        });

        it("should be able to add array to set", function(done) {
            riak.set('set_test', 'the_set').add(["str2", "str3", "str4" ], function(err, response) {
                should.not.exist(err);
                done();
            });
        });

        it("should be able to remove an element from a set", function(done) {
            riak.set('set_test', 'the_set').remove("str2", function(err, response) {
                should.not.exist(err);
                done();
            });
        });

        it("should not be able to remove non-existing element from a set", function(done) {
            riak.set('set_test', 'the_set').remove("nonexisitnigstr2", function(err, response) {
                should.exist(err);
                //err.message.should.match(/nonexisitnigstr2/);
                done();
            });
        });

        it("should be able to get current vaue of the set and ", function(done) {
            riak.set('set_test', 'the_set').value(function(err, value, data) {
                should.not.exist(err);
                value.should.length(3);
                value.should.containEql("str1");
                value.should.containEql("str3");
                value.should.containEql("str4");
                value.should.not.containEql("str2");
                done();
            });
        });

    });


    describe("Using the 'Map' class to perform basic operations", function() {

        it("should be able to update a register within a map", function(done) {
            riak.map('map_test', 'the_map').register("field1", "value1", function(err, response) {
                should.not.exist(err);
                done();
            });
        });

        it("should be able to update a flag within a map - using string", function(done) {
            riak.map('map_test', 'the_map').flag("field2", "disable", function(err, response) {
                should.not.exist(err);
                done();
            });
        });

        it("should be able get value of the previously updated map", function(done) {
            riak.map('map_test', 'the_map').value(function(err, value, data) {
                should.not.exist(err);
                should.exist(value.field1);
                value.field1.should.equal("value1");
                value.field2.should.equal(false);
                done();
            });
        });

        it("should be able to update a register within a map - again", function(done) {
            riak.map('map_test', 'the_map').register("field1", "value2", function(err, response) {
                should.not.exist(err);
                done();
            });
        });


        it("should be able to update a flag within a map - using boolean", function(done) {
            riak.map('map_test', 'the_map').flag("field2", true, function(err, response) {
                should.not.exist(err);
                done();
            });
        });

        it("should be able get value of the previously updated map", function(done) {
            riak.map('map_test', 'the_map').value(function(err, value, data) {
                should.not.exist(err);
                should.exist(value.field1);
                value.field1.should.equal("value2");
                value.field2.should.equal(true);
                done();
            });
        });

        it("should be able to update field in one object by casacde of operations", function(done) {
            riak.map('map_test', 'the_map')
                .register("Reg1", "ValReg1")
                .register("Reg2", "ValReg2")
                .register("Reg3", "ValReg3")
                .flag("Flag1", false)
                .flag("Flag2", true, function(err, response) {
                    should.not.exist(err);
                        riak.map('map_test', 'the_map').value(function(err, value) {
                            should.not.exist(err);
                            value.Reg1.should.equal("ValReg1");
                            value.Reg2.should.equal("ValReg2");
                            value.Reg3.should.equal("ValReg3");
                            value.Flag1.should.equal(false);
                            value.Flag2.should.equal(true);
                            done();
                        });
            });
        });


        it("should be able to increement counter", function(done) {
            this.timeout(TIMEOUT);
            riak.map('map_test', 'the_map')
                .counter("counter1", 5, function(err, response) {
                    should.not.exist(err);
                    riak.map('map_test', 'the_map').value(function(err, value) {
                        should.not.exist(err);
                        value.counter1.should.instanceOf(Counter);
                        value.counter1.value(function(err,value){
                            should.not.exist(err);
                            (value % 5).should.equal(0);
                            done();
                        });

                });
            });
        });

    });

    describe("Using the 'Map' perform field removal operation", function() {

        it("should be able to update a register within a map", function(done) {
            riak.map('map_test', 'the_map_remove')
                .register("field1", "value1")
                .register("field2", "value2", function(err, response) {
                    should.not.exist(err);
                    riak.map("map_test", "the_map_remove").value(function (err, value) {
                        should.exist(value.field1);

                        should.exist(value.field2);
                        done();
                    });
            });
        });

        it("should be able to remove a register from previously creted map", function(done) {
            riak.map('map_test', 'the_map_remove')
                .register_remove("field2", function(err, response) {
                    should.not.exist(err);
                    riak.map("map_test", "the_map_remove").value(function (err, value){
                        should.not.exist(err);
                        value.field1.should.equal("value1");
                        should.not.exist(value.field2);
                        done();
                    });

                });
        });


    });


    describe("Using the 'Map' class with Set fields", function() {
        it("should be able to put a set field inside a Map", function (done) {
            riak.map('map_test', 'embedded_set_in_map')
                .register("Reg1", "Value1")
                .set("SetField")
                    .add("Element1")
                    .add("Element2")
                    .add("Element3", function (err, response) {
                        should.not.exist(err);

                        riak.map('map_test', 'embedded_set_in_map').value(function(err, value) {
                            should.not.exist(err);
                            value.SetField.should.instanceOf(Set);
                            value.SetField.value(function(err, value){
                                value.should.containEql("Element1");
                                value.should.containEql("Element2");
                                value.should.containEql("Element3");
                                done();
                            });
                        });
                });
        });


        it("should be able to remove elemnets from a set", function (done) {
            riak.map('map_test', 'embedded_set_in_map')
                .register("Reg1", "Value1")
                .set("SetField")
                    .add(["Element4","Element5","Element6" ])
                    .remove("Element1")
                    .remove("Element2", function (err, response) {
                        should.not.exist(err);

                        riak.map('map_test', 'embedded_set_in_map').value(function(err, value) {
                            should.not.exist(err);
                            value.SetField.should.instanceOf(Set);
                            value.SetField.value(function(err, value){
                                value.should.not.containEql("Element1");
                                value.should.not.containEql("Element2");
                                value.should.containEql("Element3");
                                value.should.containEql("Element4");
                                value.should.containEql("Element5");
                                value.should.containEql("Element6");
                                done();
                            });
                        });
                    });
        });

    });



    describe("Using the 'Map' class within another map to perform CRDT Counter operations", function() {
        it("should be able to put a complex map within a map", function (done) {
            riak.map('map_test', 'embedded_map')
                .register("Reg1", "Value1")
                .register("Reg2", "Value2")
                .register("Reg3", "Value3")
                .map("MapField")
                    .register("MF_Reg1", "ValueMFReg", function (err, response) {
                        should.not.exist(err);
                        done();
                    });
        });

        it("should be able to a complex map", function (done) {
            this.timeout(TIMEOUT);
            riak.map('map_test', 'embedded_map').value(function(err, value, data) {
                should.not.exist(err);
                value.Reg1.should.equal("Value1");
                value.Reg2.should.equal("Value2");
                value.Reg3.should.equal("Value3");
                value.MapField.should.instanceOf(Map);
                value.MapField.value( function(err, value) {
                    should.not.exist(err);
                    value["MF_Reg1"].should.equal("ValueMFReg");
                    done();
                });
            });
        });

        it("should be able update selectively fields in existing map", function (done) {
            riak.map('map_test', 'embedded_map')
                .register("Reg1", "Value1New") // changed value of the existing field
                .register("Reg4", "Value4")     // new field to be updated
                .map("MapField")
                    .register("MapFieldReg4", "Value MFR4")     // new field to be updated
                    .register("MF_Reg1", "ValueMFRegNew", function (err, response) {
                        should.not.exist(err);

                        // now we are fetching value of the entire map from the server
                        riak.map('map_test', 'embedded_map').value(function(err, value, data) {
                            should.not.exist(err);
                            value.Reg1.should.equal("Value1New");
                            value.Reg2.should.equal("Value2");
                            value.Reg3.should.equal("Value3");
                            value.Reg4.should.equal("Value4");
                            value.MapField.should.instanceOf(Map);
                            value.MapField.value( function(err, value) {
                                should.not.exist(err);
                                value["MF_Reg1"].should.equal("ValueMFRegNew");
                                done();
                            });
                        });
                });
        });

    });


    after(function(done) { // teardown pre-test setup.
        this.timeout(TIMEOUT);
        function delete_all(done) {
            async.parallel([
                function(next) {
                    var bucket = riak.bucket('nodiak_test');
                    bucket.objects.all(function(err, r_objs) {
                        bucket.objects.delete(r_objs, function(err, result) {
                            next(null, result);
                        });
                    });
                },
                function(next) {
                    var bucket = riak.bucket('siblings_test');
                    bucket.objects.all(function(err, r_objs) {
                        bucket.objects.delete(r_objs, function(err, result) {
                            next(null, result);
                        });
                    });
                },
                function(next) {
                    var bucket = riak.bucket('counter_test');
                    bucket.objects.all(function(err, r_objs) {
                        bucket.objects.delete(r_objs, function(err, result) {
                            next(null, result);
                        });
                    });
                },
                function(next) {
                    var bucket = riak.bucket('map_test');
                    bucket.objects.all(function(err, r_objs) {
                        bucket.objects.delete(r_objs, function(err, result) {
                            next(null, result);
                        });
                    });
                },
                function(next) {
                    var bucket = riak.bucket('map_test');
                    bucket.objects.all(function(err, r_objs) {
                        bucket.objects.delete(r_objs, function(err, result) {
                            next(null, result);
                        });
                    });
                },
                function(next) {
                    var bucket = riak.bucket('set_test');
                    bucket.objects.all(function(err, r_objs) {
                        bucket.objects.delete(r_objs, function(err, result) {
                            next(null, result);
                        });
                    });
                }
            ],
            function(err, results){
                if(err) throw err;
                done();
            });
        }

        delete_all(done);
    });
});
