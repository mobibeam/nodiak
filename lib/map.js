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
var RObject = require("./robject");

var Map = function Map(bucket, key, data, metadata, embeddedinmap) {
    RObject.call(this, bucket, key, data, metadata); // calling superclass constructor

    if (key !== null && typeof bucket === "object") {
        this.key = key;
        this.bucket = bucket;
    }else{
        this.key = bucket; // we are calling the constructor only with key name
    }

    this.toadd = {}; // we keep here all updates
    this.toremove = []; // we keep here fields to be removed
    this.embeddedinmap = embeddedinmap; // map where the document is embedded in

    if (bucket.namespace) {
        this.namespace = bucket.namespace;
    }else{
        this.namespace = "maps";
    }

    this.options.datatypes = false;

    this.load_from_objvalue(data);
};

//inherits methods from the super class
Map.prototype = Object.create(RObject.prototype);

// Set the "constructor" property to refer to Set
Map.prototype.constructor = Map;

Map.prototype.tomap = function(){
    return this.embeddedinmap;
}

Map.prototype.query = function(query, op, _return){
    query.path = "/types/" + this.namespace + "/buckets/" +encodeURIComponent(this.bucket.name)+ "/datatypes/" +encodeURIComponent(this.key);
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

    if (typeof value === 'function'){
        _return = value;
        amount = null;
    }

    if ( amount instanceof Counter){
        _this.toadd[field] = amount;

        if (typeof _return === 'function'){
            _this.save(_return);
        }

        return _this;
    }else{
        field = field + "_counter";
        _this.toadd[field] = new Counter(this.bucket, field, undefined, undefined, _this);

        if(amount){
            _this.toadd[field].add(amount, function(err) {
                if (typeof _return === 'function') {
                    if(err){
                        _return(err);
                    }else{
                        _this.save(_return);
                    }
                }
            });
            return _this;
        }else{ // this case when we are likely doing operation on counter;
            if (typeof _return === 'function'){
                _this.save(_return);
            }

            return _this.toadd[field];
        }
    }
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

Map.prototype.define_map_by_object = function(obj){
    for(var element in obj){
        var value = obj[element];

        if (typeof value === "boolean"){ // we assume this is a flag
            this.flag(element, value);
            continue;
        }

        if (typeof value === "number"){ // we assume this is a registry but we need to convert it to string

            this.register(element, value.toString());
            continue;
        }

        if (Array.isArray(value)){ // we assume this will be a set and all values will be added to the set
            this.set(element, value);
            continue;
        }

        if (typeof value === "object") { // we assume it will be submap and all values will be added recursively
            var m = new Map(this.bucket, element, undefined, undefined, this);
            m.define_map_by_object(value);
            this.map(element, m);
            continue;
        }

        this.register(element, value);

    }

};

Map.prototype.map = function(field, value, _return) {

    if (typeof value === 'function'){
        _return = value;
        value = null;
    }


    if (value instanceof Map){
        field = field + "_map";
        this.toadd[field] = value;
    }else{
        field = field + "_map";
        this.toadd[field] = new Map(this.bucket, field, undefined, undefined, this);

        // creator expects automatic updates of all fields passed as a param
        if (typeof  value === "object"){
            this.toadd[field].define_map_by_object(value);
        }else{
            if (typeof _return === 'function'){
                this.save(_return);
            }
            return this.toadd[field];
        }
    }

    if (typeof _return === 'function'){
        this.save(_return);
    }
    return this;
};

Map.prototype.map_remove = function(field, _return) {
    return this.remove_field(field +"_map", _return);
};

Map.prototype.set = function(field, value, _return) {
    if (typeof value === 'function'){
        _return = value;
        value = null;
    }

    if (value instanceof Set){
        this.toadd[field] = value;
    }else{
        field = field + "_set";
        this.toadd[field] = new Set(this.bucket, field, undefined, undefined, this);
        if (value){
            this.toadd[field].add(value);
        }else{
            if (typeof _return === 'function'){
                this.save(_return);
            }
            return this.toadd[field];
        }
    }

    if (typeof _return === 'function'){
        this.save(_return);
    }
    return this;
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
    var _this = this;
    if (_this.data){
        _return(null, this.data);
    }else{
        if (_this.embeddedinmap){ // we need to call upper object
            _this.value(function (err, objvalue) {
                _return(err, objvalue);
            });
        }else{
            _this.fetch_value(function (err, value){
                if (err){
                    _return(err);
                }else{
                    _this.data = value;
                    _return(null, _this.data);
                }
            });
        }
    }
    return this;
};

Map.prototype.load_from_objvalue = function (value){
    var _this = this;
    if (typeof value === 'object') {
        if (Object.keys(value).length>0){
            _this.data = {};
        }
        for (var key in value) {
            fieldobj = split_type_and_name(key);
            switch (fieldobj.type) {
                case "register":
                    _this.data[fieldobj.key] = value[key];
                    break;
                case  "flag":
                    _this.data[fieldobj.key] = ((value[key] === "enable" || value[key] === true) ? true : false );
                    break;
                case  "map":
                    _this.data[fieldobj.key] = new Map(_this.bucket, fieldobj.key, value[key], undefined, _this);
                    break;
                case  "counter":
                    _this.data[fieldobj.key] = new Counter(_this.bucket, fieldobj.key, value[key], undefined, _this);
                    break;
                case  "set":
                    _this.data[fieldobj.key] = new Set(_this.bucket, fieldobj.key, value[key], undefined, _this);
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
            _return(err, _this.data, response.data);
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
        out.key = field.substr(0, idx);
        out.type = field.substr(idx + 1);
    }else{
        out.key = field;
        out.type = null;
    }
    return out;
}