var AppState = require('../app_state');
var Backbone = require('backbone');

var Topic = Backbone.Model.extend({
    idAttribute: 'name',

    constructor: function Topic() {
        Backbone.Model.prototype.constructor.apply(this, arguments);
    },

    url: function() {
        return AppState.url('/topics/' + encodeURIComponent(this.get('name')));
    }
});

module.exports = Topic;
