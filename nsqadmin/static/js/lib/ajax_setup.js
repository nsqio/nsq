var $ = require('jquery');
var _ = require('underscore');

// Set up some headers and options for every request.
$.ajaxPrefilter(function(options) {
    options['headers'] = _.defaults(options['headers'] || {}, {
        'X-UserAgent': USER_AGENT,
        'Accept': 'application/vnd.nsq; version=1.0'
    });
    options['timeout'] = 20 * 1000;
    options['contentType'] = 'application/json';
});
