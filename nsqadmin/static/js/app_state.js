var Backbone = require('backbone');
var _ = require('underscore');

var AppState = Backbone.Model.extend({
    defaults: function() {
        return {
            'VERSION': VERSION,
            'GRAPHITE_URL': GRAPHITE_URL,
            'GRAPH_ENABLED': GRAPH_ENABLED,
            'STATSD_INTERVAL': STATSD_INTERVAL,
            'USE_STATSD_PREFIXES': USE_STATSD_PREFIXES,
            'STATSD_COUNTER_FORMAT': STATSD_COUNTER_FORMAT,
            'STATSD_GAUGE_FORMAT': STATSD_GAUGE_FORMAT,
            'STATSD_PREFIX': STATSD_PREFIX,
            'NSQLOOKUPD': NSQLOOKUPD,
            'graph_interval': '2h'
        };
    },

    initialize: function() {
        this.on('change:graph_interval', function(model, v) {
            localStorage.setItem('graph_interval', v);
        });

        var qp = _.object(_.compact(_.map(window.location.search.slice(1).split('&'),
            function(item) { if (item) { return item.split('='); } })));

        var def = this.get('GRAPH_ENABLED') ? '2h' : 'off';
        var interval = qp['t'] || localStorage.getItem('graph_interval') || def;
        this.set('graph_interval', interval);
    },

    url: function(url) {
        return '/api' + url;
    }
});

var appState = new AppState();

window.AppState = appState;

module.exports = appState;
