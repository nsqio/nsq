var _ = require('underscore');
var Backbone = require('backbone');

var AppState = require('../app_state');

var Topic = require('../models/topic');

var Topics = Backbone.Collection.extend({
    model: Topic,

    comparator: 'id',

    constructor: function Topics() {
        Backbone.Collection.prototype.constructor.apply(this, arguments);
    },

    url: function() {
        return AppState.apiPath('/topics');
    },

    parse: function(resp) {
        var topics = _.map(resp['topics'], function(name) {
            return {'name': name};
        });
        return topics;
    }
});

module.exports = Topics;
