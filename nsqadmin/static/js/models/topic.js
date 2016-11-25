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

        var total_hourly_pubsize = new Array();
        _.each(response['nodes'], function(node, outIdx){
           _.each(node['partition_hourly_pubsize'], function(value, idx){
                if(total_hourly_pubsize[idx]) {
                    total_hourly_pubsize[idx] += value;
                }else{
                    total_hourly_pubsize[idx] = value;
                }
           });
        });
        response['total_partition_hourly_pubsize'] = total_hourly_pubsize;
        return response;
    }
});

module.exports = Topic;
