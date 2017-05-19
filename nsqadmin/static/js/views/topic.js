var $ = require('jquery');

window.jQuery = $;
var bootstrap = require('bootstrap'); //eslint-disable-line no-unused-vars
var bootbox = require('bootbox');

var Pubsub = require('../lib/pubsub');
var AppState = require('../app_state');

var BaseView = require('./base');

var click2Show=" >>>";
var click2Hide=" <<<";

var TopicView = BaseView.extend({
    className: 'topic container-fluid',

    template: require('./spinner.hbs'),

    events: {
        'click .topic-actions button': 'topicAction',
        'click .channel-action .hierarchy button': 'onCreateTopicChannel',
        'click .toggle h4': 'onToggle',
        'click .toggle h4 span a': 'onToggle',
    },

    initialize: function() {
        BaseView.prototype.initialize.apply(this, arguments);
        this.listenTo(AppState, 'change:graph_interval', this.render);
        this.model.fetch()
            .done(function(data) {
                this.template = require('./topic.hbs');
                this.render({'message': data['message']});
            }.bind(this))
            .fail(this.handleViewError.bind(this))
            .always(Pubsub.trigger.bind(Pubsub, 'view:ready'));
        $('[data-toggle="tooltip"]').tooltip({
                placement : 'top'
            });
    },

    onToggle: function(e) {
        e.preventDefault();
        e.stopPropagation();
        var canHideClass = "canHide";
        var parent = $(e.target).parents(".toggle").first();
        var divCanHide = parent.next();
        if(divCanHide != null && divCanHide.attr("class") != null && divCanHide.attr("class").indexOf(canHideClass) !== -1) {
            divCanHide.toggle(300);
            var anchor = parent.find("a").first();
            if(anchor.text().indexOf(click2Hide) !== -1) {
                anchor.text(click2Show);
            } else {
                anchor.text(click2Hide);
            }
        }
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
                    .done(function() { window.location = '/'; });
            } else {
                $.post(this.model.url(), JSON.stringify({'action': action}))
                    .done(function() { window.location.reload(true); })
                    .fail(this.handleAJAXError.bind(this));
            }
        }.bind(this));
    },

    onCreateTopicChannel: function(e) {
        e.preventDefault();
        e.stopPropagation();
        var topic = $(e.target.form.elements['topic']).val();
        var channel = $(e.target.form.elements['channel']).val();
        if (topic === '' || channel === '') {
            return;
        }
        $.post(AppState.url('/topics/' + topic + '/' + channel), JSON.stringify({
                'action': 'create'
            }))
            .done(function() { window.location.reload(true); })
            .fail(this.handleAJAXError.bind(this));
    }
});

module.exports = TopicView;
