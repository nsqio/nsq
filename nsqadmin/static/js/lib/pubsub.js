var _ = require('underscore');
var Backbone = require('backbone');

var Pubsub = _.clone(Backbone.Events);

// making this global to more easily trigger events from the console
window.Pubsub = Pubsub;

module.exports = Pubsub;
