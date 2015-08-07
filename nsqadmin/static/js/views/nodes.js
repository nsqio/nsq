var $ = require('jquery');

var Pubsub = require('../lib/pubsub');
var AppState = require('../app_state');

var BaseView = require('./base');

var Nodes = require('../collections/nodes');

var NodesView = BaseView.extend({
    className: 'nodes container-fluid',

    template: require('./spinner.hbs'),

    events: {
        'click .conn-count': 'onClickConnCount'
    },

    initialize: function() {
        BaseView.prototype.initialize.apply(this, arguments);
        this.listenTo(AppState, 'change:graph_interval', this.render);
        this.collection = new Nodes();
        this.collection.fetch().done(function() {
            this.template = require('./nodes.hbs');
            this.render();
            Pubsub.trigger('view:ready');
        }.bind(this));
    },

    onClickConnCount: function(e) {
        e.preventDefault();
        $(e.target).next().toggle();
    }
});

module.exports = NodesView;
