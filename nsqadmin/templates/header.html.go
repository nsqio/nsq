package templates

func init() {
	registerTemplate("header.html", `
<!DOCTYPE html>
<html lang="en">
    <head>
        <meta http-equiv="Content-type" content="text/html; charset=utf-8">
        <title>{{.Title}}</title>
        <!-- from http://www.bootstrapcdn.com/ -->

        <script src="/static/jquery-1.8.2.min.js"></script>
        <link rel="stylesheet" href="/static/twitter-bootstrap/2.2.1/css/bootstrap-combined.min.css" type="text/css" charset="utf-8">

        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <style type="text/css">
        .red {
            color: #c30;
        }
        .red:hover {
            color: #f30;
        }
        .bold {
            font-weight: bold;
        }
        .graph-row td {
            text-align: center;
        }
        .image-preview {
             display: none;
             position: absolute;
             z-index: 100;
             height: 240px;
             width: 480px;
        }
        </style>
    </head>
    <body>

<script>
$(document).ready(function () {
 // Get all the thumbnail
 $('td a[href^="/render"] img').mouseenter(function(e) {
 
  // Calculate the position of the image tooltip
  x = e.pageX - 25;
  y = e.pageY + 25;
  x = Math.min(x, $(window).width() - 510)
  if (y + 240 > $(window).height()) {
    y = e.pageY - 265;
  }
 
  // Set the z-index of the current item,
  // make sure it's greater than the rest of thumbnail items
  // Set the position and display the image tooltip
  var tooltip = $('.image-preview');

  tooltip.attr("src", $(this).parent().attr("href"));
  tooltip.stop().css({'top': y,'left': x,'display':'block','opacity':1});
    
 }).mouseleave(function() {
    
  // Reset the z-index and hide the image tooltip
  var tooltip = $('.image-preview');
  tooltip.animate({"opacity": "hide"}, "fast");
 });
 
});
</script>

<img class="image-preview img-polaroid">
        
<div class="navbar navbar-inverse">
  <div class="navbar-inner">
    <div class="container">
      <a class="brand" href="/">NSQ</a>
        <ul class="nav">
          <li><a href="/">Streams</a></li>
          <li><a href="/nodes">Nodes</a></li>
          <li><a href="/counter">Counter</a></li>
          <li><a href="/lookup">Lookup</a></li>
          <li class="divider-vertical"></li>
          {{template "graph_options.html" .}}
          <li class="divider-vertical"></li>
          <li><a href="https://github.com/bitly/nsq">NSQ on github</a></li>
        </ul>
        <div class="pull-right navbar-text"><span class="label label-info">v{{.Version}}</span></div>
    </div>
  </div>
</div>

<div class="container-fluid">
`)
}
