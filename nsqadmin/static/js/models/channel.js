var AppState = require('../app_state');
var Backbone = require('backbone');

var Channel = Backbone.Model.extend({
    idAttribute: 'name',

    constructor: function Channel() {
        Backbone.Model.prototype.constructor.apply(this, arguments);
    },

    url: function() {
        return AppState.url('/topics/' +
            encodeURIComponent(this.get('topic')) + '/' +
            encodeURIComponent(this.get('name')));
    }
});

module.exports = Channel;
