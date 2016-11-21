var $ = require('jquery');
var _ = require('underscore');
var Backbone = require('backbone');

var AppState = require('../app_state');

var Rank = require('../models/rank');

var Ranks = Backbone.Collection.extend({
    model: Rank,

    comparator: 'filter',

    constructor: function Ranks() {
        Backbone.Collection.prototype.constructor.apply(this, arguments);
    }
});

module.exports = Ranks;
