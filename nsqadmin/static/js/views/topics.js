var Pubsub = require('../lib/pubsub');
var AppState = require('../app_state');

var BaseView = require('./base');
var Topics = require('../collections/topics');

var TopicsView = BaseView.extend({
    className: 'topics container-fluid',

    template: require('./spinner.hbs'),

    initialize: function() {
        BaseView.prototype.initialize.apply(this, arguments);
        this.listenTo(AppState, 'change:graph_interval', this.render);
        this.collection = new Topics();
        this.collection.fetch().done(function() {
            this.template = require('./topics.hbs');
            this.render();
            Pubsub.trigger('view:ready');
        }.bind(this));
    }
});

module.exports = TopicsView;
