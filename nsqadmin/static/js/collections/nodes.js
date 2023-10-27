var Backbone = require('backbone');

var AppState = require('../app_state');

var NodeModel = require('../models/node');

var Nodes = Backbone.Collection.extend({
    model: NodeModel,

    comparator: 'id',

    constructor: function Nodes() {
        Backbone.Collection.prototype.constructor.apply(this, arguments);
    },

    url: function() {
        return AppState.apiPath('/nodes');
    },

    parse: function(resp) {
        resp['nodes'].forEach(function(n) {
            var jaddr = n['broadcast_address'];
            if (jaddr.includes(':')) {
                // ipv6 raw address contains ':'
                // it must be wrapped in '[ ]' when joined with port
                jaddr = '[' + jaddr + ']';
            }
            n['broadcast_address_http'] = jaddr + ':' + n['http_port'];
        });
        return resp['nodes'];
    }
});

module.exports = Nodes;
