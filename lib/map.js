// (The MIT License)

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

var Set = require("./set");
var Counter = require("./counter");

var Map = function Map(bucket, name, embeddedinmap, load) {
    if (name !== null && typeof bucket === "object") {
        this.name = name;
        this.bucket = bucket;
    }else{
        this.name = bucket; // we are calling the constructor only with name
    }

    this.toadd = {}; // we keep here all updates
    this.toremove = []; // we keep here fields to be removed
    this.embeddedinmap = embeddedinmap; // map where the document is embedded in
    this.valueobj=null; // we keep here a fetched object

    this.load_from_objvalue(load);
};


Map.prototype.query = function(query, op, _return){
    query.path = this.bucket.client.defaults.resources.riak_kv_wm_maps + "/buckets/" +encodeURIComponent(this.bucket.name)+ "/datatypes/" +encodeURIComponent(this.name);
    this.bucket.client[op](query, _return);
};


Map.prototype.remove_field = function(field, _return) {
    this.toremove.push(field);
    if (typeof _return === 'function'){
        this.save(_return);
    }
    return this;
};


Map.prototype.register = function(field, value, _return) {
    field = field +"_register";
    this.toadd[field] = value;
    if (typeof _return === 'function'){
        this.save(_return);
    }
    return this;
};

Map.prototype.register_remove = function(field, _return) {
    return this.remove_field(field +"_register", _return);
};


Map.prototype.counter = function(field, amount, _return) {
    var _this=this;
    if ( amount instanceof Counter){
        _this.toadd[field] = amount;

        if (typeof _return === 'function'){
            _this.save(_return);
        }
    }else{
        field = field + "_counter";
        _this.toadd[field] = new Counter(this.bucket, field, _this);
        _this.toadd[field].add(amount, function(err) {
            if (typeof _return === 'function') {
                if(err){
                    _return(err);
                }else{
                    _this.save(_return);
                }
            }
        });
    }

    return this.toadd[field];
};


Map.prototype.counter_remove = function(field, _return) {
    return this.remove_field(field +"_counter", _return);
};



Map.prototype.flag = function(field, value, _return) {
    field = field +"_flag";

    if (typeof value === 'boolean'){
        value = value ? "enable" : "disable";
    }else{
        if (value === 'enable'){
            value = "enable";
        }else{
            value = "disable";
        }
    }

    this.toadd[field] = value;
    if (typeof _return === 'function'){
        this.save(_return);
    }
    return this;
};


Map.prototype.flag_remove = function(field, _return) {
    return this.remove_field(field +"_flag", _return);
};


Map.prototype.map = function(field, value, _return) {
    if (value instanceof Map){
        this.toadd[field] = value;
    }else{
        field = field + "_map";
        this.toadd[field] = new Map(this.bucket, field, this);
    }

    if (typeof _return === 'function'){
        this.save(_return);
    }
    return this.toadd[field];
};


Map.prototype.map_remove = function(field, _return) {
    return this.remove_field(field +"_map", _return);
};


Map.prototype.set = function(field, value, _return) {
    if (value instanceof Set){
        this.toadd[field] = value;
    }else{
        field = field + "_set";
        this.toadd[field] = new Set(this.bucket, field, this);
    }

    if (typeof _return === 'function'){
        this.save(_return);
    }
    return this.toadd[field];
};

Map.prototype.set_remove = function(field, _return) {
    return this.remove_field(field +"_set", _return);
};


Map.prototype.save = function(_return) {

    if (this.embeddedinmap) { // we need to call superior class if we are embedded in another set
        this.embeddedinmap.save(_return);
    } else{
        var query = {
            headers: {"content-type": "application/json"},
            body: {"update": {}}
        };
        query.body.update = this.get_update_structure();
        if (this.toremove.length>0){
            query.body.remove = this.toremove;
        }

        this.query(query, "POST", _return);
    }

    return this;
};



Map.prototype.get_update_structure = function() {
    var  updateobj = {};

    for (var key in this.toadd){
        var value = this.toadd[key];

        if (value instanceof Map){
            updateobj[key] = {
                update: value.get_update_structure()
            }
        }else if (value instanceof Counter) {
            updateobj[key] = value.get_update_structure();
        }else if (value instanceof Set){
            updateobj[key] = value.get_update_structure();
        } else{
            updateobj[key] = value;
        }
    }
    return updateobj;
};

Map.prototype.value = function(_return){
    if (this.valueobj){
        _return(null, this.valueobj);
    }else{
        if (this.embeddedinmap){ // we need to call upper object
            this.value(function (err, objvalue) {
                _return(err, objvalue);
            });
        }else{
            this.fetch_value(_return);
        }
    }
    return this;
};

Map.prototype.load_from_objvalue = function (value){
    var _this = this;
    if (typeof value === 'object') {
        if (Object.keys(value).length>0){
            _this.valueobj = {};
        }
        for (var key in value) {
            fieldobj = split_type_and_name(key);
            switch (fieldobj.type) {
                case "register":
                    _this.valueobj[fieldobj.name] = value[key];
                    break;
                case  "flag":
                    _this.valueobj[fieldobj.name] = ((value[key] === "enable" || value[key] === true) ? true : false );
                    break;
                case  "map":
                    _this.valueobj[fieldobj.name] = new Map(_this.bucket, fieldobj.name, _this, value[key]);
                    break;
                case  "counter":
                    _this.valueobj[fieldobj.name] = new Counter(_this.bucket, fieldobj.name, _this, value[key]);
                    break;
                case  "set":
                    _this.valueobj[fieldobj.name] = new Set(_this.bucket, fieldobj.name, _this, value[key]);
                    break;
            }
        }
    }
};


Map.prototype.fetch_value = function(_return) {
    var query = {
        path: this.path
    };
    var _this = this;

    this.query(query, "GET", function(err, response) {
        if (err){
            _return(err);
        }

        if (response.data.value){
            _this.load_from_objvalue(response.data.value);
            _return(err, _this.valueobj, response.data);
        }else{

            err = new Error("Returned unexpected result from database");
            _return(err);

        }

    });
};


module.exports = Map;


/* Library of supporting functions */

function split_type_and_name (field){
    var idx = field.lastIndexOf("_");
    var out = {};
    if (idx>=0) {
        out.name = field.substr(0, idx);
        out.type = field.substr(idx + 1);
    }else{
        out.name = field;
        out.type = null;
    }
    return out;
}