var AppState = require('../app_state');
var Backbone = require('backbone');

var Node = Backbone.Model.extend({ //eslint-disable-line no-undef
    idAttribute: 'name',

    constructor: function Node() {
        Backbone.Model.prototype.constructor.apply(this, arguments);
    },

    urlRoot: function() {
        return AppState.url('/nodes');
    },

    tombstoneTopic: function(topic) {
        return this.destroy({
            'data': JSON.stringify({'topic': topic}),
            'dataType': 'text'
        });
    }
});

module.exports = Node;
