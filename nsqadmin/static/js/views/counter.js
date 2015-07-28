var _ = require('underscore');
var $ = require('jquery');

var AppState = require('../app_state');

var BaseView = require('./base');

var CounterView = BaseView.extend({
    className: 'counter container-fluid',

    template: require('./counter.hbs'),

    initialize: function() {
        BaseView.prototype.initialize.apply(this, arguments);
        this.listenTo(AppState, 'change:graph_interval', this.render);
        this.start();
    },

    remove: function() {
        clearTimeout(this.poller);
        clearTimeout(this.animator);
        BaseView.prototype.remove.apply(this, arguments);
    },

    start: function() {
        this.poller = null;
        this.animator = null;
        this.delta = 0;
        this.looping = false;
        this.targetPollInterval = 10000;
        this.currentNum = -1;
        this.updateStats();
        this.startLoop(100);
    },

    startLoop: function(i) {
        this.interval = i;
        this.poller = setTimeout(this.updateStats.bind(this), i);
    },

    updateStats: function() {
        $.get(AppState.url('/counter')).done(function(data) {
            var num = _.reduce(data, function(n, v) {
                return n + v['message_count'];
            }, 0);

            if (this.currentNum === -1) {
                // seed the display
                this.currentNum = num;
                this.writeCounts(this.currentNum);
            } else if (num > this.lastNum) {
                var delta = num - this.lastNum;
                this.delta = (delta / (this.interval / 1000)) / 50;
                if (!this.animator) {
                    this.displayFrame();
                }
            }
            this.currentNum = this.lastNum;
            this.lastNum = num;

            var newInterval = this.interval;
            if (newInterval < this.targetPollInterval) {
                newInterval = this.interval + 1000;
            }
            this.startLoop(newInterval);

            $('#fetcherror').hide();
        }.bind(this)).fail(function() {
            clearTimeout(this.animator);
            this.animator = null;

            this.startLoop(this.interval);

            $('#fetcherror').show().text('ERROR: unable to fetch stats, retrying in ' +
                (this.interval / 1000) + 's');
        }.bind(this));

        if ($('#big_graph').length) {
            $('#big_graph').attr('src', LARGE_GRAPH_URL + '&_uniq=' + Math.random() * 1000000);
        }
    },

    displayFrame: function() {
        this.currentNum += this.delta;
        this.writeCounts(this.currentNum);
        this.animator = setTimeout(this.displayFrame.bind(this), 1000 / 60);
    },

    writeCounts: function(c) {
        var text = parseInt(c, 10).toString();
        var node = $('.numbers')[0];
        var n = $('.numbers .number');
        for (var i = 0; i < text.length; i++) {
            var v = text.charAt(i);
            if (n.length > i) {
                var el = $(n[i]);
                el.show();
                el.find('.top').text(v);
                el.find('.bottom').text(v);
            } else {
                $(node).append('<span class="number"><span class="top">' + v +
                    '</span><span class="bottom">' + v + '</span></span>\n');
            }
        }
        $('.numbers .number').each(function(ii, vv) {
            if (ii >= text.length) {
                $(vv).hide();
            }
        });
    }
});

module.exports = CounterView;
