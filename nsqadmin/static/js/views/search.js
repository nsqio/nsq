var $ = require('jquery');
var _ = require('underscore');
var Pubsub = require('../lib/pubsub');
var AppState = require('../app_state');
var BaseView = require('./base');

var SearchView = BaseView.extend({
    className: 'search container-fluid',

    template: require('./spinner.hbs'),
    template: require('./search.hbs'),
    events: {
        'click .search-trace button': 'onSearchTopicMessages'
    },

    initialize: function() {
        BaseView.prototype.initialize.apply(this, arguments);
     },

    onSearchTopicMessages: function(e) {
        e.preventDefault();
        e.stopPropagation();
        $('#loadingmessage').show();
        var topic = $(e.target.form.elements['topic']).val();
        var partition_id = $(e.target.form.elements['partition_id']).val();
        var msgid = $(e.target.form.elements['msgid']).val();
        var traceid = $(e.target.form.elements['traceid']).val();
        var hours = $(e.target.form.elements['hours']).val();
        var ishashed = $(e.target.form.elements['hashed']).is(':checked');
        $.post(AppState.url('/search/messages'), JSON.stringify({
                'topic': topic,
                'partition_id': partition_id,
                'msgid': msgid,
                'traceid': traceid,
                'ishashed': ishashed,
                'hours': hours
            }))
            .done(function(data) {
                this.template = require('./search.hbs');
                this.render({
                    'messages': data['logDataDtos'],
                    'total_cnt': data['totalCount'],
                    'request_msg': data['request_msg'],
                    'message': data['message']
                });
                $('#loadingmessage').hide();
            }.bind(this))
            .fail(this.handleViewError.bind(this))
            .always(Pubsub.trigger.bind(Pubsub, 'view:ready'));
    },

});

module.exports = SearchView;
