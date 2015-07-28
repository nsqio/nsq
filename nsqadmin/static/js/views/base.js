var _ = require('underscore');
var Backbone = require('backbone');

var AppState = require('../app_state');

var BaseView = Backbone.View.extend({
    constructor: function(options) {
        // As of 1.10, Backbone no longer automatically attaches options passed
        // to the constructor as this.options, but that's often useful in some
        // cases, like a className function, that happen before initialize()
        // would have a chance to attach the same options.
        this.options = options || {};
        return Backbone.View.prototype.constructor.apply(this, arguments);
    },

    initialize: function() {
        this.subviews = [];
        this.rendered = false;
    },

    template: function() {},

    skippedRender: function() {},

    render: function(data) {
        if (this.renderOnce && this.rendered) {
            this.skippedRender();
            return this;
        }
        this.removeSubviews();
        var ctx = this.getRenderCtx(data);
        // console.log('render ctx: %o', ctx);
        var html = this.template(ctx);
        if (!this.removed) {
            this.$el.empty();
            this.$el.append(html);
            this.postRender(ctx);
        }
        this.rendered = true;
        return this;
    },

    getRenderCtx: function(data) {
        var ctx = {
            'graph_enabled': AppState.get('GRAPH_ENABLED'),
            'graph_interval': AppState.get('graph_interval'),
            'graph_active': AppState.get('GRAPH_ENABLED') &&
                AppState.get('graph_interval') !== 'off',
            'nsqlookupd': AppState.get('NSQLOOKUPD'),
            'version': AppState.get('VERSION')
        };
        if (this.model) {
            return _.extend(ctx, this.model.toJSON());
        } else if (this.collection) {
            return _.extend(ctx, {'collection': this.collection.toJSON()});
        } else if (data) {
            return _.extend(ctx, data);
        }
        return ctx;
    },

    postRender: function() {},

    appendSubview: function(subview, selector) {
        return this.appendSubviews([subview], selector);
    },

    appendSubviews: function(subviews, selector) {
        this.subviews.push.apply(this.subviews, subviews);
        var $el = selector ? this.$(selector) : this.$el;
        $el.append(subviews.map(function(subview) {
            return subview.render().delegateEvents().el;
        }));
    },

    removeSubviews: function() {
        while (this.subviews.length) {
            this.subviews.pop().remove();
        }
    },

    remove: function() {
        this.removed = true;
        this.removeSubviews();
        Backbone.View.prototype.remove.apply(this, arguments);
    }
});

module.exports = BaseView;
