var _ = require('underscore');
var AppState = require('../app_state');
var Backbone = require('backbone');

var Topic = Backbone.Model.extend({
    idAttribute: 'name',

    constructor: function Topic() {
        Backbone.Model.prototype.constructor.apply(this, arguments);
    },

    url: function() {
        return AppState.url('/topics/' + encodeURIComponent(this.get('name')));
    },

    parse: function(response) {
        response['nodes'] = _.map(response['nodes'] || [], function(node) {
            var hourly_pub_size;
            var nodeAddr = node['node'];
            var nodeParts = node['node'].split(':');
            var port = nodeParts.pop();
            var address = nodeParts.join(':');
            var hostname = node['hostname'];
            node['show_broadcast_address'] = hostname.toLowerCase() !== address.toLowerCase();
            node['hostname_port'] = hostname + ':' + port;
            node['client_pub_stats'] =  _.map(node['client_pub_stats'] || [], function(pub_stats){
               var date = new Date(pub_stats['last_pub_ts']);
                pub_stats['last_pub_ts'] = date.toString();
                return pub_stats;
            });
            return node;
        });
        if(response['nodes'][0]){
            response['client_pub_stats'] = response['nodes'][0]['client_pub_stats'];
            response['msg_size_stats'] = response['nodes'][0]['msg_size_stats'];
            response['msg_write_latency_stats'] = response['nodes'][0]['msg_write_latency_stats'];
        }
        return response;
    }
});

module.exports = Topic;
