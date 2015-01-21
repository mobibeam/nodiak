// (The MIT License)

// Copyright (c) 2013 Nathan Aschbacher
// Copyright (c) 2015 Lukasz Dutka

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

var RObject = require("./robject");

var Counter = function Counter(bucket, key, data, metadata, embeddedinmap) {
    RObject.call(this, bucket, key, data, metadata); // calling superclass constructor

    if (key !== null && typeof bucket === "object") {
        this.key = key;
        this.bucket = bucket;
    }else{
        this.key = bucket; // we are calling the constructor only with key name
    }

    if (bucket.namespace) {
        this.namespace = bucket.namespace;
    }

    this.counterupdate = null; // we keep here counter update
    this.embeddedinmap = embeddedinmap; // map where the document is embedded in

    this.load_from_objvalue(data);
};

//inherits methods from the super class
Counter.prototype = Object.create(RObject.prototype);

// Set the "constructor" property to refer to Set
Counter.prototype.constructor = Counter;

Counter.prototype.tomap = function(){
    return this.embeddedinmap;
}

Counter.prototype.query = function(query, op, _return){

    if (this.namespace){
        query.path = query.path = "/types/" + this.namespace + "/buckets/" +encodeURIComponent(this.bucket.name)+ "/datatypes/" +encodeURIComponent(this.key);
        query.headers = {"content-type": "application/json"}; // new way works only with json encoding
    }else{
        // use old fasion way to define counters
        query.path = this.bucket.client.defaults.resources.riak_kv_wm_counters + "/" +encodeURIComponent(this.bucket.name)+ "/counters/" +encodeURIComponent(this.key);
    }
    this.bucket.client[op](query, _return);
};

Counter.prototype.get_update_structure = function() {
    return this.counterupdate;
};

Counter.prototype.load_from_objvalue = function (value){
    var _this = this;
    _this.data = value;
};

Counter.prototype.add = function(amount, _return) {
    amount = parseInt(amount, 10);

    if (!this.embeddedinmap) { // direct update of the counter
        if (amount) {
            var query = {
                headers: {"content-type": "text/plain"},
                body: amount
            };
            this.query(query, "POST", _return);
        }
        else {
            var err = new Error("Value needs to be an integer (positive or negative)");
            err.data = amount;
            _return(err);
        }
    }else{
        this.counterupdate = amount;
        if(_return){
            _return(null);
        }
    }

    return this;
};

Counter.prototype.subtract = function(amount, _return) {
    amount = parseInt(amount * -1, 10);
    return this.add(amount, _return);
};

Counter.prototype.value = function(_return) {
    var _this = this;
    if (!this.embeddedinmap) {

        var query = {
            path: this.bucket.client.defaults.resources.riak_kv_wm_counters + "/" + encodeURIComponent(this.bucket.name) + "/counters/" + encodeURIComponent(this.key)
        };

        this.query(query, "GET", function(err, response) {
            if (_this.namespace) {
                _return(err, parseInt(response.data.value, 10));
            }else{ // old fasion response processing
                _return(err, parseInt(response.data, 10));
            }

        });
    }else{

        if (!this.data) {
            this.embeddedinmap.value(_return);
        }else{
            _return(null, this.data);
        }

    }
};

module.exports = Counter;

