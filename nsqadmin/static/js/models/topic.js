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
        response['has_client_pub'] = false;
        response['nodes'] = _.map(response['nodes'] || [], function(node) {
            var nodeAddr = node['node'];
            var nodeParts = node['node'].split(':');
            var port = nodeParts.pop();
            var address = nodeParts.join(':');
            var hostname = node['hostname'];
            node['show_broadcast_address'] = hostname.toLowerCase() !== address.toLowerCase();
            node['hostname_port'] = hostname + ':' + port;
            node['client_pub_stats'] =  _.map(node['client_pub_stats'] || [], function(pub_stats){
               var date = new Date(pub_stats['last_pub_ts']*1000);
                pub_stats['last_pub_ts'] = date.toString();
                return pub_stats;
            });
            if(node['client_pub_stats'].length > 0) {
                response['has_client_pub'] = true;
            }
            if(node['partition_hourly_pubsize']){
                node['partition_hourly_pubsize'].reverse();
                node['hourly_pubsize'] = node['partition_hourly_pubsize'][0];
            }
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
        response['total_hourly_pubsize'] = response['total_partition_hourly_pubsize'][0];
        return response;
    }
});

module.exports = Topic;
