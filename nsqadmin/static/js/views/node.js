var Pubsub = require('../lib/pubsub');
var AppState = require('../app_state');

var BaseView = require('./base');

var NodeView = BaseView.extend({
    className: 'node container-fluid',

    template: require('./spinner.hbs'),

    initialize: function() {
        BaseView.prototype.initialize.apply(this, arguments);
        this.listenTo(AppState, 'change:graph_interval', this.render);
        this.model.fetch().done(function() {
            this.template = require('./node.hbs');
            this.render();
            Pubsub.trigger('view:ready');
        }.bind(this));
    }
});

module.exports = NodeView;
