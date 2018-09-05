var $ = require('jquery');
var Backbone = require('backbone');

// var Pubsub = require('./lib/pubsub');
var Router = require('./router');

var AppView = require('./views/app');

// When using browserify, we need to tell Backbone what jQuery to use.
Backbone.$ = $;

// Side effects:
require('./lib/ajax_setup');
require('./lib/handlebars_helpers');

var start = function() {
    new AppView();
    Router.start();
};

// Pubsub.on('all', function() {
//     console.log.apply(console, arguments);
// });

start();
