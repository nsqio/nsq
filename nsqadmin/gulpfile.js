var browserify = require('browserify');
var clean = require('gulp-clean');
var gulp = require('gulp');
var notify = require('gulp-notify');
var path = require('path');
var sass = require('gulp-sass');
var source = require('vinyl-source-stream');
var taskListing = require('gulp-task-listing');
var uglify = require('gulp-uglify');
var sourcemaps = require('gulp-sourcemaps');
var buffer = require('vinyl-buffer');


var ROOT = 'static';

var VENDOR_CONFIG = {
    'src': [
        'backbone',
        'jquery',
        'underscore',
        'bootbox',
    ],
    'target': 'vendor.js',
    'targetDir': './static/build/'
};

function excludeVendor(b) {
    VENDOR_CONFIG.src.forEach(function(vendorLib) {
        b.exclude(vendorLib);
    });
}

function bytesToKB(bytes) { return Math.floor(+bytes/1024); }

function logBundle(filename, watching) {
    return function (err, buf) {
        if (err) {
            console.error(err.toString());
            if (!watching) {
                process.exit(1);
            }
        }
        if (!watching) {
            console.log(filename + ' ' + bytesToKB(buf.length) + ' KB written');
        }
    }
}


function sassTask(root, inputFile) {
    return function() {
        var onError = function(err) {
            notify({'title': 'Sass Compile Error'}).write(err);
        };
        gulp.src(path.join(root, 'css', inputFile))
            .pipe(sass({
                'sourceComments': 'map',
                'onError': onError
            }))
            .pipe(gulp.dest(path.join(root, 'build/')));
    };
}


function browserifyTask(root, inputFile) {
    return function() {
        var onError = function() {
            var args = Array.prototype.slice.call(arguments);
            notify.onError({
                'title': 'JS Compile Error',
                'message': '<%= error.message %>'
            }).apply(this, args);
            // Keep gulp from hanging on this task
            this.emit('end');
        };

        // Browserify needs a node module to import as its arg, so we need to
        // force the leading "./" to be included.
        var b = browserify({
            entries: './' + path.join(root, 'js', inputFile),
            debug: true
        })

        excludeVendor(b);

        return b.bundle()
            .pipe(source(inputFile))
            .pipe(buffer())
            .pipe(sourcemaps.init({'loadMaps': true, 'debug': true}))
                // Add transformation tasks to the pipeline here.
                .pipe(uglify())
                .on('error', onError)
            .pipe(sourcemaps.write('./'))
            .pipe(gulp.dest(path.join(root, 'build/')));
    };
}


function watchTask(root) {
    return function() {
        gulp.watch(path.join(root, 'sass/**/*.scss'), ['sass']);
        gulp.watch([
            path.join(root, 'js/**/*.js'),
            path.join(root, 'js/**/*.hbs')
        ], ['browserify']);
        gulp.watch([
          path.join(root, 'html/**'),
          path.join(root, 'fonts/**')
        ], ['sync-static-assets'])
    };
}


function cleanTask(){
    var paths = Array.prototype.slice.apply(arguments);
    return function () {
        gulp.src(paths).pipe(clean());
    };
}

gulp.task('vendor-build-js', function() {
    var onError = function() {
        var args = Array.prototype.slice.call(arguments);
        notify.onError({
            'title': 'JS Compile Error',
            'message': '<%= error.message %>'
        }).apply(this, args);
        // Keep gulp from hanging on this task
        this.emit('end');
    };

    var b = browserify()
        .require(VENDOR_CONFIG.src);

    return b.bundle(logBundle(VENDOR_CONFIG.target))
        .pipe(source(VENDOR_CONFIG.target))
        .pipe(buffer())
            // Add transformation tasks to the pipeline here.
            .pipe(uglify())
            .on('error', onError)
        .pipe(gulp.dest(VENDOR_CONFIG.targetDir));
});

gulp.task('help', taskListing);

gulp.task('sync-static-assets', function() {
    return gulp.src([
        path.join(ROOT, 'html/**'),
        path.join(ROOT, 'fonts/**'),
        path.join(ROOT, 'img/**')
    ]).pipe(gulp.dest(path.join(ROOT, 'build')));
});

gulp.task('sass', sassTask(ROOT, '*.*css'));
gulp.task('browserify', browserifyTask(ROOT, 'main.js'));
gulp.task('build', ['sass', 'browserify', 'sync-static-assets', 'vendor-build-js']);
gulp.task('watch', ['build'], watchTask(ROOT));
gulp.task('clean', cleanTask(path.join(ROOT, 'build')));

gulp.task('default', ['help']);
