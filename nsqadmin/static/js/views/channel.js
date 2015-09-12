var $ = require('jquery');

window.jQuery = $;
var bootstrap = require('bootstrap'); //eslint-disable-line no-unused-vars
var bootbox = require('bootbox');

var Pubsub = require('../lib/pubsub');
var AppState = require('../app_state');

var BaseView = require('./base');

var ChannelView = BaseView.extend({
    className: 'channel container-fluid',

    template: require('./spinner.hbs'),

    events: {
        'click .channel-actions button': 'channelAction'
    },

    initialize: function() {
        BaseView.prototype.initialize.apply(this, arguments);
        this.listenTo(AppState, 'change:graph_interval', this.render);
        this.model.fetch()
            .done(function(data) {
                this.template = require('./channel.hbs');
                this.render({'message': data['message']});
            }.bind(this))
            .fail(this.handleViewError.bind(this))
            .always(Pubsub.trigger.bind(Pubsub, 'view:ready'));
    },

    channelAction: function(e) {
        e.preventDefault();
        e.stopPropagation();
        var action = $(e.currentTarget).data('action');
        var txt = 'Are you sure you want to <strong>' +
            action + '</strong> <em>' + this.model.get('topic') +
            '/' + this.model.get('name') + '</em>?';
        bootbox.confirm(txt, function(result) {
            if (result !== true) {
                return;
            }
            if (action === 'delete') {
                $.ajax(this.model.url(), {'method': 'DELETE'})
                    .done(function() {
                        window.location = '/topics/' + encodeURIComponent(this.model.get('topic'));
                    })
                    .fail(this.handleAJAXError.bind(this));
            } else {
                $.post(this.model.url(), JSON.stringify({'action': action}))
                    .done(function() { window.location.reload(true); })
                    .fail(this.handleAJAXError.bind(this));
            }
        }.bind(this));
    }
});

module.exports = ChannelView;
