var Pubsub = require('../lib/pubsub');
var AppState = require('../app_state');

var BaseView = require('./base');
var Topics = require('../collections/topics');

var TopicsView = BaseView.extend({
    className: 'topics container-fluid',

    template: require('./spinner.hbs'),

    events: {
        'click .filters input': 'topicsFilterAction',
    },

    topicsFilterAction: function(e) {
            e.stopPropagation();

            $('.table tr').show();

            $(e.target).parent().find('input')
                .filter(function(){
                    if($(this).is(':checked')) {
                        return this;
                    }
                })
                .each(function() {
                    var filterValue = $(this).val();
                    $('.table tr')
                        .filter(
                            function(){
                                if ($(this)
                                    .find('td a#' + filterValue).length == 0){
                                    return this;
                                }
                            }
                        )
                        .hide();
                    }
                );
    },

    initialize: function() {
        BaseView.prototype.initialize.apply(this, arguments);
        this.listenTo(AppState, 'change:graph_interval', this.render);
        this.collection = new Topics();
        this.collection.fetch()
            .done(function(data) {
                this.template = require('./topics.hbs');
                this.render({'message': data['message']});
            }.bind(this))
            .fail(this.handleViewError.bind(this))
            .always(Pubsub.trigger.bind(Pubsub, 'view:ready'));
    }
});

module.exports = TopicsView;
