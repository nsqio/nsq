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
        return AppState.url('/topics');
    },

    parse: function(resp) {
        var topics = _.map(resp['topics'], function(topic) {
            return {
                'name':             topic['topic_name'],
                'extend_support':   topic['extend_support'],
                'ordered':          topic['ordered']
            };
        });
        return topics;
    }
});

module.exports = Topics;
