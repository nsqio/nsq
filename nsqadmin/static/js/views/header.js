var _ = require('underscore');
var $ = require('jquery');

var AppState = require('../app_state');

var BaseView = require('./base');

var HeaderView = BaseView.extend({
    className: 'header',

    template: require('./header.hbs'),

    events: {
        'click .dropdown-menu li': 'onGraphIntervalClick'
    },

    initialize: function() {
        BaseView.prototype.initialize.apply(this, arguments);
        this.listenTo(AppState, 'change:graph_interval', this.render);
    },

    getRenderCtx: function() {
        return _.extend(BaseView.prototype.getRenderCtx.apply(this, arguments), {
            'graph_intervals': ['1h', '2h', '12h', '24h', '48h', '168h', 'off'],
            'graph_interval': AppState.get('graph_interval')
        });
    },

    onReset: function() {
        this.render();
        this.$('.dropdown-toggle').dropdown();
    },

    onGraphIntervalClick: function(e) {
        e.stopPropagation();
        AppState.set('graph_interval', $(e.target).text());
    }
});

module.exports = HeaderView;
