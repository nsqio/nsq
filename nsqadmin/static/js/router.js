var Backbone = require('backbone');

var Pubsub = require('./lib/pubsub');


var Router = Backbone.Router.extend({
    routes: {
        '': 'topics',
        'topics/(:topic)(/:channel)': 'topic',
        'lookup': 'lookup',
        'nodes(/:node)': 'nodes',
        'counter': 'counter'
    },

    defaultRoute: 'topics',

    initialize: function() {
        this.currentRoute = this.defaultRoute;
        this.listenTo(this, 'route', function(route, params) {
            this.currentRoute = route || this.defaultRoute;
            // console.log('Route: %o; params: %o', route, params);
        });
    },

    start: function() {
        Backbone.history.start({
            'pushState': true
        });
    },

    topics: function() {
        Pubsub.trigger('topics:show');
    },

    topic: function(topic, channel) {
        if (channel !== null) {
            Pubsub.trigger('channel:show', topic, channel);
            return;
        }
        Pubsub.trigger('topic:show', topic);
    },

    lookup: function() {
        Pubsub.trigger('lookup:show');
    },

    nodes: function(node) {
        if (node !== null) {
            Pubsub.trigger('node:show', node);
            return;
        }
        Pubsub.trigger('nodes:show');
    },

    counter: function() {
        Pubsub.trigger('counter:show');
    }
});


module.exports = new Router();
