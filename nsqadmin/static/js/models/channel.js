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

    initialize: function() {
      this.on('change:clients', this.calculateHostnamePort);
    },

    calculateHostnamePort: function() {
        this.get("clients").forEach(function(client){
            var client_id = client["client_id"];
            var hostname = client["hostname"];
            var remote_address = client["remote_address"];
  
            // ignore client_id if it's duplicative
            if (client_id === hostname.split(".")[0] || client_id === hostname) {
                client["client_id"] = "";
            }

            var port = remote_address.split(":").pop()
            client['hostname_port'] =  hostname + ":" + port;
        })
    }
});

module.exports = Channel;
