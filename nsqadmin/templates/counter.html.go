package templates

func init() {
	registerTemplate("counter.html", `
{{template "header.html" .}}
{{$g := .GraphOptions}}
{{$targ := .Target}}

<style>
body {
    background-color:#000;
}
/*
 * .numbers : The container for .number
 */

#numbers {
    text-align:center;
    margin-top:100px;
}

.numbers {
  font-family: 'Francois One', sans-serif;
  font-size: 70px;
  color: white;
  white-space: nowrap;
  position: relative;
  direction: ltr;
  vertical-align: middle;
  height: 70px;
}


/*
 * .number : The container for each number
 */

.number {
  width: 50px;
  height: 70px;
  position: relative;
  display: inline-block;
  margin: 2px;
  border-radius: 5px;
  box-shadow: #999 0 -1px 0px 0px, #444 0 1px 0px 0px;
}


/*
 * Little white stuffs that link the top and the bottom
 */

.number:before {
  content: '';
  display: block;
  width: 3px;
  height: 6px;
  background: white;
  position: absolute;
  left: 0;
  top: 30px;
  z-index: 2;
  box-shadow: inset rgb(130, 130, 130) 0 0 0px 1px;
  border-right: 1px solid black;
  border-top: 1px solid black;
  border-bottom: 1px solid black;
}

.number:after {
  content: '';
  display: block;
  width: 3px;
  height: 6px;
  background: rgb(200, 200, 200);
  position: absolute;
  right: 0;
  top: 30px;
  z-index: 2;
  box-shadow: inset rgb(130, 130, 130) 0 0 0px 1px;
  border-left: 1px solid black;
  border-top: 1px solid black;
  border-bottom: 1px solid black;  
}

/*
 * The panels
 */

.number .top, .number .bottom {
  display: block;
  height: 35px;
  width: 50px;
  text-align: center;
  overflow: hidden;
  border-radius: 3px;
  background: -webkit-linear-gradient(90deg, rgb(30, 30, 30), rgb(90, 90, 90));
  background: -o-linear-gradient(90deg, rgb(30, 30, 30), rgb(90, 90, 90));
  background: linear-gradient(0deg, rgb(30, 30, 30), rgb(90, 90, 90));
  background-size: 50px 70px;
}

.number .top {
  -moz-box-sizing: border-box;
  -webkit-box-sizing: border-box;
  -o-box-sizing: border-box;
  box-sizing: border-box;
  line-height: 70px;
  border-top-left-radius: 5px; 
  border-top-right-radius: 5px;
  background-position: 0px 0px;
  border-bottom: 1px solid black;
}

.number .bottom {
  line-height: 0px;
  border-bottom-left-radius: 5px; 
  border-bottom-right-radius: 5px;
  background-position: 0px 35px; 
}

/*
 * Panel animations
 */

/* The new top panel */
.number .top:nth-last-of-type(4) {
  position: absolute;
  z-index: 0;
}

/* The old top panel */
.number .top:nth-last-of-type(3) {
  animation-duration: 0.2s;
  animation-name: top;
  animation-fill-mode: forwards;
  animation-timing-function: ease-in;  
  z-index: 1;

  -moz-transform-origin: 0 100%;
  -webkit-transform-origin: 0 100%;
  -o-transform-origin: 0 100%;
  transform-origin: 0 100%;
}

@keyframes top {
  from {
    -moz-transform: scaleY(1);
    -webkit-transform: scaleY(1);
    -o-transform: scaleY(1);
    transform: scaleY(1);
  }
 
  to {
    -moz-transform: scaleY(0);
    -webkit-transform: scaleY(0);
    -o-transform: scaleY(0);
    transform: scaleY(0);
  }
}

/* The new bottom panel */
.number .bottom:nth-last-of-type(2) {
  position: absolute;
  z-index: 1;

  animation-duration: 0.2s;
  animation-name: bottom;
  animation-delay: 0.2s;
  animation-fill-mode: forwards;
  animation-timing-function: ease-out;

  -moz-transform: scaleY(0);
  -moz-transform-origin: 0 0;
}

@keyframes bottom {
  from {
    -moz-transform: scaleY(0);
  }
 
  to {
    -moz-transform: scaleY(1);
  }
}

/* The old bottom panel */
.number .top:nth-last-of-type(1) {
  z-index: -1;
}

.processed {
    font-color:#bbb;
    margin:50px;
    font-size:24pt;
    font-family:Helvetica;
}
.messagerate {
    font-color:#bbb;
    margin:25px;
    font-size:20pt;
    font-family:Helvetica;
}
</style>

<div class="alert alert-error" id="fetcherror" style="display:none;">
</div>

<div id="numbers">
<div class="numbers">
</div>
<p class="processed">Messages Processed</p>
<p class="messagerate"></p>
<script type="text/javascript">
var large_graph_url = "{{$g.LargeGraph $targ "message_count"}}";
</script>
{{if $g.Enabled}}<img id="big_graph" height="500" src=""/>{{end}}
</div>

{{template "js.html" .}}

<script>
var current_number = 0;
var statsID = '';

var pending_frames = 0;
var pending_count = 0;
var display_poll;
var looping = false;
var frame_rate = 50;
var target_poll_interval = 30000
function display(count, time) {
    if (count > 0) {
        pending_count += count
        pending_frames = parseInt(time / frame_rate) + 1 // frame rate
        if (looping === false) {
            display_frame()
        }
    }
}

// this isn't very smooth for smaller numbers
function display_frame() {
    var delta;
    if (pending_frames === 1) {
        delta = pending_count
    } else {
        delta = pending_count / pending_frames
    }
    pending_frames -= 1
    if (delta > 0) {
        pending_count -= delta
        current_number += delta
        writeCounts(current_number)
    }
    if (pending_count > 0 ) {
        looping = true;
        setTimeout(display_frame, frame_rate);
    } else {
        looping = false;
    }
}


// <span class='number'><span class='top'>..</span><span class='bottom'>..</span></span>
function writeCounts(c) {
    var text = parseInt(c).toString();
    var node = $(".numbers")[0]
    var n = $(".numbers .number")
    $.each(text, function(i, v){
        if (n.length > i) {
            var el = $(n[i]);
            el.show()
            el.find(".top").text(v);
            el.find(".bottom").text(v);
        } else {
            $(node).append('<span class="number"><span class="top">' + v + '</span><span class="bottom">'+ v + "</span></span>\n");
        }
    })
    $(".numbers .number").each(function(i, v) {
        if (i >= text.length) {
            $(v).hide()
        }
    });
}
function updateStats() {
    $.ajax({
      url: "/counter/data",
      data: {"id" : statsID},
      error : function() {
          // backoff our polling
          clearInterval(data_poll);
          tries += 1
          startLoop(tries * 15000)
          $("#fetcherror").show().text("Unable to get statistics. Retrying in " + (interval / 1000) + ' seconds')
      }
    }).done(function ( data ) {
        if (data && data.status_code === 200) {
            if (statsID === "") {
                // seed the display
                current_number = data.data.total_messages;
                writeCounts(current_number);
            } else if (data.data.new_messages !== 0) {
                display(data.data.new_messages, interval)
            }
            statsID = data.data.id;
            if (tries !== 0 || interval <= target_poll_interval) {
                new_interval = interval < target_poll_interval ? interval + 1000 : target_poll_interval
                // reset out of error state
                tries = 0;
                clearInterval(data_poll);
                startLoop(new_interval);
                $("#fetcherror").hide();
            }
        }
    });
    $('#big_graph').attr('src', large_graph_url + "&_uniq=" + Math.random() * 1000000);
}
var data_poll;
var interval;
var tries = 0;
function startLoop(i) {
    interval = i
    data_poll = setInterval(function(){
        updateStats();
    }, interval);
}


updateStats();
startLoop(1000);

</script>

{{template "footer.html" .}}
`)
}
