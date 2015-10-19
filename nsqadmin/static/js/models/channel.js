var _ = require('underscore');

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
    },

    parse: function(response) {
        response['nodes'] = _.map(response['nodes'] || [], function(node) {
            var nodeParts = node['node'].split(':');
            var port = nodeParts.pop();
            var address = nodeParts.join(':');
            var hostname = node['hostname'];
            node['show_broadcast_address'] = hostname.toLowerCase() !== address.toLowerCase();
            node['hostname_port'] = hostname + ':' + port;
            return node;
        });

        response['clients'] = _.map(response['clients'] || [], function(client) {
            var clientId = client['client_id'];
            var hostname = client['hostname'];
            var shortHostname = hostname.split('.')[0];

            // ignore client_id if it's duplicative
            client['show_client_id'] = (clientId.toLowerCase() !== shortHostname.toLowerCase()
                                        && clientId.toLowerCase() !== hostname.toLowerCase());

            var port = client['remote_address'].split(':').pop();
            client['hostname_port'] = hostname + ':' + port;

            return client;
        });

        return response;
    }
});

module.exports = Channel;
