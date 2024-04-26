var AppState = require('../app_state');
var Backbone = require('backbone');

var NodeModel = Backbone.Model.extend({
    idAttribute: 'name',

    constructor: function Node() {
        Backbone.Model.prototype.constructor.apply(this, arguments);
    },

    urlRoot: function() {
        return AppState.apiPath('/nodes');
    },

    tombstoneTopic: function(topic) {
        return this.destroy({
            'data': JSON.stringify({'topic': topic}),
            'dataType': 'text'
        });
    }
});

module.exports = NodeModel;
