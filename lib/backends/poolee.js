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

var Pool = require("poolee"),
    http = require('http'), // this  is the actual agent sending HTTP request
    helpers = require('../helpers'),
    HTTPBackend = require('./http');

var AgentKeepAlive = require('agentkeepalive');

var PooleeBackend = function PooleeBackend(servers, options) {

    if (typeof servers === "string"){ // we try to explode by the name
        servers = servers.split(",");
    }

    for (var idx in servers){
        servers[idx] = servers[idx].trim();
    }

    this.poolservers = servers;
    if ( options == null || typeof options !== "object"){
        options = { maxPending: 1000,       // maximum number of outstanding request to allow
            maxSockets: 20,         // max sockets per endpoint Agent
            timeout: 60000,         // request timeout in ms
            resolution: 1000,       // timeout check interval (see below)
            keepAlive: true,        // use an alternate Agent that does http keep-alive properly
            ping: "/ping",          // default health check url. It expects 200 code
            pingTimeout: 2000,      // ping timeout in ms
            retryFilter: undefined, // see below
            retryDelay: 20,         // see below
            maxRetries: 5,          // see below
            name: undefined,        // optional string
            agentOptions: undefined// an object for passing options directly to the Http Agent
        };
    }

    this.pooloptions = options;

    this.pool = new Pool(http, this.poolservers, this.pooloptions);

    HTTPBackend.call(this);

}; helpers.inherits(PooleeBackend, HTTPBackend);


PooleeBackend.prototype.agent = AgentKeepAlive; // this is just for sake of compatibilty with HTTPBackend constructor. It wont' be used later on.


PooleeBackend.prototype.request = function(query, _return) {
    var _this = this;


    var req = this.pool.request({
            path: query.path,           // the request path (required)
            method: query.method,
            data: query.body,           // request body, may be a string, buffer, or stream
            headers: query.headers,     // extra http headers to send
            retryFilter: undefined,     // valid http responses aren't necessarily a "success". This function lets you check the response before calling the request callback
            attempts: this.pool.length, // or at least 2, at most options.maxRetries + 1
            retryDelay: 20,             // retries wait with exponential backoff times this number of ms
            timeout: 60000,             // ms to wait before timing out the request
            encoding: 'utf8',           // response body encoding
            stream: true                // stream instead of buffer response body
            },
            function(err, response) {
                if (err){
                    _return(err, null);
                }else{
                    _this.responseHandler(response, query, _return);
                }

            });
};


module.exports = PooleeBackend;