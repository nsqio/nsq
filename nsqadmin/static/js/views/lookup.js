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
        $.ajax({
            url : AppState.url('/topics?inactive=true'),
        })
            .done(function(data) {
                this.template = require('./lookup.hbs');
                this.render({
                    'topics': _.map(data['topics'], function(v, k) {
                        return {'name': k, 'channels': v};
                    }),
                    'message': data['message']
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
        var partition_num = $(e.target.form.elements['partition_num']).val();
        var replicator = $(e.target.form.elements['replicator']).val();
        var retention_days = $(e.target.form.elements['retention_days']).val();
        var syncdisk = $(e.target.form.elements['syncdisk']).val();
        var orderedmulti = 'false'
        if($(e.target.form.elements['orderedmulti']).is(':checked')){
            orderedmulti = 'true'
        }
        var extend = 'false'
        if($(e.target.form.elements['extend']).is(':checked')){
            extend = 'true'
        }
        if (topic === '' || partition_num === '' || replicator === '' || channel === '') {
            return;
        }
        $.post(AppState.url('/topics'), JSON.stringify({
                'topic': topic,
                'channel': channel,
                'partition_num': partition_num,
                'replicator': replicator,
                'retention_days': retention_days,
                'syncdisk': syncdisk,
                'orderedmulti': orderedmulti,
                'extend': extend
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
