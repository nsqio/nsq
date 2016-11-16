var $ = require('jquery');
var _ = require('underscore');
var Pubsub = require('../lib/pubsub');
var AppState = require('../app_state');
var BaseView = require('./base');
var Rank = require('../models/rank');
var Ranks = require('../collections/ranks');

var StatisticsView = BaseView.extend({
    className: 'statistics container-fluid',

    template: require('./statistics.hbs'),

    initialize: function() {
        BaseView.prototype.initialize.apply(this, arguments);
        this.listenTo(AppState, 'change:graph_interval', this.render);
        var filters;
        this.collection = new Ranks();
        this.listenTo(this.collection, 'update', this.render);
        var viewCollection = this.collection;
        $.get(AppState.url("/statistics"), function(resp){
            filters = _.map(resp['filters'], function(name) {
                rank = new Rank({filter: name});
                rank.fetch()
                    .done(function(data) {
                        viewCollection.add(data);
                    });
                return name;
            });
        });
    }
});

module.exports = StatisticsView;
