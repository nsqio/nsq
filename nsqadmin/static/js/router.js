var Backbone = require('backbone');

var AppState = require('./app_state');
var Pubsub = require('./lib/pubsub');


var Router = Backbone.Router.extend({
    initialize: function() {
        var bp = function(p) {
            // remove leading slash
            return AppState.basePath(p).substring(1);
        };
        this.route(bp('/'), 'topics');
        this.route(bp('/topics/(:topic)(/:channel)'), 'topic');
        this.route(bp('/lookup'), 'lookup');
        this.route(bp('/nodes(/:node)'), 'nodes');
        this.route(bp('/counter'), 'counter');
        // this.listenTo(this, 'route', function(route, params) {
        //     console.log('Route: %o; params: %o', route, params);
        // });
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
