// (The MIT License)

// Copyright (c) 2013 Nathan Aschbacher

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


var Counter = function Counter(bucket, name, embeddedinmap, load) {
    if (name !== null && typeof bucket === "object") {
        this.name = name;
        this.bucket = bucket;
    }else{
        this.name = bucket; // we are calling the constructor only with name
    }

    this.counterupdate = null; // we keep here counter update
    this.embeddedinmap = embeddedinmap; // map where the document is embedded in
    this.valueobj=null; // we keep here a fetched object

    this.load_from_objvalue(load);
};


Counter.prototype.query = function(query, op, _return){
    query.path = this.bucket.client.defaults.resources.riak_kv_wm_counters + "/" +encodeURIComponent(this.bucket.name)+ "/counters/" +encodeURIComponent(this.name);
    this.bucket.client[op](query, _return);
};


Counter.prototype.get_update_structure = function() {
    return this.counterupdate;
};


Counter.prototype.load_from_objvalue = function (value){
    var _this = this;
    _this.valueobj = value;
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
    if (!this.embeddedinmap) {

        var query = {
            path: this.bucket.client.defaults.resources.riak_kv_wm_counters + "/" + encodeURIComponent(this.bucket.name) + "/counters/" + encodeURIComponent(this.name)
        };

        this.query(query, "GET", function(err, response) {
            _return(err, parseInt(response.data, 10));
        });
    }else{

        if (!this.valueobj) {
            this.embeddedinmap.value(_return);
        }else{
            _return(null, this.valueobj);
        }

    }
};

module.exports = Counter;

