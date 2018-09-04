var $ = require('jquery');

window.jQuery = $;
var bootstrap = require('bootstrap'); //eslint-disable-line no-unused-vars
var bootbox = require('bootbox');

var Pubsub = require('../lib/pubsub');
var AppState = require('../app_state');

var BaseView = require('./base');

var TopicView = BaseView.extend({
    className: 'topic container-fluid',

    template: require('./spinner.hbs'),

    events: {
        'click .topic-actions button': 'topicAction'
    },

    initialize: function() {
        BaseView.prototype.initialize.apply(this, arguments);
        this.listenTo(AppState, 'change:graph_interval', this.render);
        var isAdmin = this.model.get('isAdmin');
        this.model.fetch()
            .done(function(data) {
                this.template = require('./topic.hbs');
                this.render({'message': data['message'], 'isAdmin': isAdmin});
            }.bind(this))
            .fail(this.handleViewError.bind(this))
            .always(Pubsub.trigger.bind(Pubsub, 'view:ready'));
    },

    topicAction: function(e) {
        e.preventDefault();
        e.stopPropagation();
        var action = $(e.currentTarget).data('action');
        var txt = 'Are you sure you want to <strong>' +
            action + '</strong> <em>' + this.model.get('name') + '</em>?';
        bootbox.confirm(txt, function(result) {
            if (result !== true) {
                return;
            }
            if (action === 'delete') {
                $.ajax(this.model.url(), {'method': 'DELETE'})
                    .done(function() { window.location = AppState.basePath('/'); });
            } else {
                $.post(this.model.url(), JSON.stringify({'action': action}))
                    .done(function() { window.location.reload(true); })
                    .fail(this.handleAJAXError.bind(this));
            }
        }.bind(this));
    }
});

module.exports = TopicView;
