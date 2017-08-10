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
                case 'hourly-pubsize':
                    data['rank_value'] = data['hourly_pubsize'];
                    break;
                case 'channel-requeue':
                    data['name'] = data['name'].replace(':', '/');
                    data['rank_value'] = data['requeue_count'];
                    break;
                case 'channel-timeout':
                    data['name'] = data['name'].replace(':', '/');
                    data['rank_value'] = data['timeout_count'];
                    break;
                default:
                    data['rank_value'] = data['message_count'];
            }
            return data;
        });
        resp['top10'] = _.filter(resp['top10'], function(data){
            return (null !== data['rank_value'] && data['rank_value'] > 0);
        });
        return resp;
    }
});

module.exports = Rank;