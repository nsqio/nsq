var _ = require('underscore');
var $ = require('jquery');

var AppState = require('../app_state');
var Pubsub = require('../lib/pubsub');
var BaseView = require('./base');

var Topic = require('../models/topic');
var Channel = require('../models/channel');

var LookupView = BaseView.extend({
    className: 'lookup container-fluid',

    template: require('./spinner.hbs'),

    events: {
        'click .hierarchy button': 'onCreateTopicChannel',
        'click .delete-topic-link': 'onDeleteTopic',
        'click .delete-channel-link': 'onDeleteChannel'
    },

    initialize: function() {
        BaseView.prototype.initialize.apply(this, arguments);
        var isAdmin = arguments[0]['isAdmin'];
        $.ajax(AppState.apiPath('/topics?inactive=true'))
            .done(function(data) {
                this.template = require('./lookup.hbs');
                this.render({
                    'topics': _.map(data['topics'], function(v, k) {
                        return {'name': k, 'channels': v};
                    }),
                    'message': data['message'],
                    'isAdmin': isAdmin
                });
            }.bind(this))
            .fail(this.handleViewError.bind(this))
            .always(Pubsub.trigger.bind(Pubsub, 'view:ready'));
    },

    onCreateTopicChannel: function(e) {
        e.preventDefault();
        e.stopPropagation();
        var topic = $(e.target.form.elements['topic']).val();
        var channel = $(e.target.form.elements['channel']).val();
        if (topic === '' && channel === '') {
            return;
        }
        $.post(AppState.apiPath('/topics'), JSON.stringify({
            'topic': topic,
            'channel': channel
        }))
            .done(function() { window.location.reload(true); })
            .fail(this.handleAJAXError.bind(this));
    },

    onDeleteTopic: function(e) {
        e.preventDefault();
        e.stopPropagation();
        var topic = new Topic({
            'name': $(e.target).data('topic')
        });
        topic.destroy({'dataType': 'text'})
            .done(function() { window.location.reload(true); })
            .fail(this.handleAJAXError.bind(this));
    },

    onDeleteChannel: function(e) {
        e.preventDefault();
        e.stopPropagation();
        var channel = new Channel({
            'topic': $(e.target).data('topic'),
            'name': $(e.target).data('channel')
        });
        channel.destroy({'dataType': 'text'})
            .done(function() { window.location.reload(true); })
            .fail(this.handleAJAXError.bind(this));
    }
});

module.exports = LookupView;
