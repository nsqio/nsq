var _ = require('underscore');
var $ = require('jquery');

var AppState = require('../app_state');

var BaseView = require('./base');

var CounterView = BaseView.extend({
    className: 'counter container-fluid',

    template: require('./counter.hbs'),

    initialize: function() {
        BaseView.prototype.initialize.apply(this, arguments);
        this.listenTo(AppState, 'change:graph_interval', function() {
            clearTimeout(this.poller);
            clearTimeout(this.animator);
            this.render();
            this.start();
        });
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
        this.lastNum = 0;
        this.interval = 100;
        this.graphUrl = null;
        this.updateStats();
    },

    startLoop: function(i) {
        this.interval = i;
        this.poller = setTimeout(this.updateStats.bind(this), i);
    },

    updateStats: function() {
        $.get(AppState.apiPath('/counter')).done(function(data) {
            if (this.removed) {
                return;
            }

            var num = _.reduce(data['stats'], function(n, v) {
                return n + v['message_count'];
            }, 0);

            if (this.currentNum === -1) {
                // seed the display
                this.currentNum = num;
                this.lastNum = num;
                this.writeCounts(this.currentNum);
            } else if (num > this.lastNum) {
                var delta = num - this.lastNum;
                this.delta = (delta / (this.interval / 1000)) / 50;
                this.lastNum = num;

                if (!this.animator) {
                    this.displayFrame();
                }
            }

            var newInterval = this.interval;
            if (newInterval < this.targetPollInterval) {
                newInterval = this.interval + 1000;
            }
            this.startLoop(newInterval);

            $('#warning, #error').hide();
            if (data['message'] !== '') {
                $('#warning .alert').text(data['message']);
                $('#warning').show();
            }
        }.bind(this)).fail(function(jqXHR) {
            if (this.removed) {
                return;
            }

            clearTimeout(this.animator);
            this.animator = null;

            this.startLoop(10000);

            this.handleAJAXError(jqXHR);
        }.bind(this));

        if ($('#big_graph').length) {
            if (!this.graphUrl) {
                this.graphUrl = $('#big_graph').attr('src');
            }
            var uniq = Math.floor(Math.random() * 1000000);
            $('#big_graph').attr('src', this.graphUrl + '&_uniq=' + uniq);
        }
    },

    displayFrame: function() {
        this.currentNum = Math.min(this.currentNum + this.delta, this.lastNum);
        this.writeCounts(this.currentNum);
        if (this.currentNum < this.lastNum) {
            this.animator = setTimeout(this.displayFrame.bind(this), 1000 / 60);
        } else {
            this.animator = null;
        }
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
