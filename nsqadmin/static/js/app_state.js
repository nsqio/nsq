var Backbone = require('backbone');

var AppState = Backbone.Model.extend({
    defaults: function() {
        return {
            'VERSION': VERSION,
            'GRAPHITE_URL': GRAPHITE_URL,
            'GRAPH_ENABLED': GRAPH_ENABLED,
            'STATSD_INTERVAL': STATSD_INTERVAL,
            'STATSD_PREFIX': STATSD_PREFIX,
            'NSQLOOKUPD': NSQLOOKUPD,
            'graph_interval': '2h'
        };
    },

    initialize: function() {
        this.on('change:graph_interval', function(model, v) {
            localStorage.setItem('graph_interval', v);
        });
        this.set('graph_interval', localStorage.getItem('graph_interval') || 'off');
    },

    url: function(url) {
        return '/api' + url;
    }
});

var appState = new AppState();

window.AppState = appState;

module.exports = appState;
