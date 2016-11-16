var _ = require('underscore');

var AppState = require('../app_state');
var Backbone = require('backbone');

var Rank = Backbone.Model.extend({
    idAttribute: 'filter',

    constructor: function Rank() {
        Backbone.Model.prototype.constructor.apply(this, arguments);
    },

    url: function() {
        return AppState.url('/statistics/' + encodeURIComponent(this.get('filter')));
    },

    parse: function(resp) {
        var filter = this.get('filter');
        resp['top10'] = _.map(resp['top10'], function(data){
            switch(filter){
                case 'channel-depth':
                    data['rank_value'] = data['total_channel_depth'];
                    break;
                case 'message-count':
                    data['rank_value'] = data['message_count'];
                    break;
                default:
                    data['rank_value'] = data['hourly_pubsize'];
            }
            return data;
        });
        return resp;
    }
});

module.exports = Rank;