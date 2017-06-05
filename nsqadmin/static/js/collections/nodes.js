var Backbone = require('backbone');

var AppState = require('../app_state');

var Node = require('../models/node'); //eslint-disable-line no-undef

var Nodes = Backbone.Collection.extend({
    model: Node,

    comparator: 'id',

    constructor: function Nodes() {
        Backbone.Collection.prototype.constructor.apply(this, arguments);
    },

    url: function() {
        return AppState.url('/nodes');
    },

    parse: function(resp) {
        return resp['nodes'];
    }
});

module.exports = Nodes;
