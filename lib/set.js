// (The MIT License)

// Copyright (c) 2014 Lukasz Dutka

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


var Set = function Set(bucket, name, embeddedinmap, load, namespace) {
    if (name !== null && typeof bucket === "object") {
        this.name = name;
        this.bucket = bucket;
    }else{
        this.name = bucket; // we are calling the constructor only with name
    }

    this.toadd = []; // we keep here all elements to be add to the set
    this.toremove = []; // we keep here all elements to removed from the set
    this.embeddedinmap = embeddedinmap; // map where the document is embedded in
    this.valueobj=null; // we keep here a fetched object

    if (namespace) {
        this.namespace = namespace;
    }else{
        this.namespace = "sets";
    }


    this.load_from_objvalue(load);
};

Set.prototype.tomap = function(){
    return this.embeddedinmap;
}

Set.prototype.load_from_objvalue = function (value){
    this.valueobj = value;
};


Set.prototype.query = function(query, op, _return){
    query.path = "/types/" + this.namespace + "/buckets/" +encodeURIComponent(this.bucket.name)+ "/datatypes/" +encodeURIComponent(this.name);
    this.bucket.client[op](query, _return);
};


Set.prototype.operation = function(operation, element, _return) {
    var _this = this;
    var operon;

    if (operation == "add"){
        operon = "toadd";
    }else{
        operon = "toremove";
    }

    if (Array.isArray(element)){
        this[operon] = this[operon].concat(element);
    }else{
        this[operon].push(element);
    }


    if(typeof _return === 'function' ){ // we need to perform save at the end
        if (!_this.embeddedinmap) { // we need to execute query
            var query = {
                headers: {"content-type": "application/json"},
                body: {}
            };

            if (_this.toadd.length==1){
                query.body["add"] = _this.toadd[0];
            }

            if (_this.toadd.length>1){
                query.body["add_all"] = _this.toadd;
            }

            if (_this.toremove.length==1){
                query.body["remove"] = _this.toremove[0];
            }

            if (_this.toremove.length>1){
                query.body["remove_all"] = _this.toremove;
            }

            this.query(query, "POST", _return);
        }else{

            if (typeof _return === 'function'){
                _this.embeddedinmap.save(_return);
            }
            return this;
        }
    }

    return this;
};


Set.prototype.add = function(element, _return) {
    return this.operation("add", element, _return);
};


Set.prototype.remove = function(element, _return) {
    return this.operation("remove", element, _return);
};


Set.prototype.value = function(_return) {
    if (!this.embeddedinmap) {
        this.query({}, "GET", function(err, response) {
            var value = null;
            if (response.data.value){
                value = response.data.value;
            }
            _return(err, value, response.data);
        });
    }else{
        if (!this.valueobj) {
            this.embeddedinmap.value(_return);
        }else{
            _return(null, this.valueobj);
        }

    }
};


Set.prototype.get_update_structure = function() {
    var  updateobj = {};
    if (this.toadd.length>1){
        updateobj['add_all'] = this.toadd;
    }

    if (this.toadd.length==1){
        updateobj['add'] = this.toadd[0];
    }

    if (this.toremove.length>1){
        updateobj['remove_all'] = this.toremove;
    }

    if (this.toremove.length==1){
        updateobj['remove'] = this.toremove[0];
    }
    return updateobj;
};



module.exports = Set;