package templates

func init() {
	registerTemplate("topic.html", `
{{template "header.html" .}}
{{$g := .GraphOptions}}
{{$gts := .GlobalTopicStats}}
{{$firstTopic := .FirstTopic}}

<ul class="breadcrumb">
  <li><a href="/">Streams</a> <span class="divider">/</span></li>
  <li class="active">{{.Topic}}</li>
</ul>

<div class="row-fluid">
    <div class="span6">
        <blockquote>
            <p>Topic: <strong>{{.Topic}}</strong>
        </blockquote>
    </div>
</div>

<div class="row-fluid">
    <div class="span2">
        <form action="/empty_topic" method="POST">
            <input type="hidden" name="topic" value="{{.Topic}}">
            <button class="btn btn-medium btn-warning" type="submit">Empty Queue</button>
        </form>
    </div>
    <div class="span2">
        <form action="/delete_topic" method="POST">
            <input type="hidden" name="topic" value="{{.Topic}}">
            <button class="btn btn-medium btn-danger" type="submit">Delete Topic</button>
        </form>
    </div>
    <div class="span2">
        {{if .GlobalTopicStats.Paused}}
        <form action="/unpause_topic" method="POST">
            <input type="hidden" name="topic" value="{{.Topic}}">
            <button class="btn btn-medium btn-success" type="submit">UnPause Topic</button>
        </form>
        {{else}}
        <form action="/pause_topic" method="POST">
            <input type="hidden" name="topic" value="{{.Topic}}">
            <button class="btn btn-medium btn-inverse" type="submit">Pause Topic</button>
        </form>
        {{end}}
    </div>
</div>


<div class="row-fluid">
{{if not .TopicStats}}
<div class="span6">
<h4>Topic Message Queue</h4>
<div class="alert">
    <h4>Notice</h4> No producers exist for this topic.
    <p>See <a href="/lookup">Lookup</a> for more information.
</div>
{{else}}
<div class="span12">
<h4>Topic Message Queue</h4>
<table class="table table-bordered table-condensed">
    {{if $firstTopic.E2eProcessingLatency.Percentiles}}
      <tr>
          <th colspan="{{if $g.Enabled}}6{{else}}5{{end}}"></th>
          <th colspan="{{len $firstTopic.E2eProcessingLatency.Percentiles}}">E2E Processing Latency</th>
      </tr>
    {{end}}
    <tr>
        <th>NSQd Host</th>
        <th>Depth</th>
        <th>Memory + Disk</th>
        <th>Messages</th>
        {{if $g.Enabled}}<th>Rate</th>{{end}}
        <th>Channels</th>
        {{range $e2e := $firstTopic.E2eProcessingLatency.Percentiles}}
          <th>{{$e2e.quantile | floatToPercent}}<sup>{{$e2e.quantile | percSuffix}}</sup></th>
        {{end}}
    </tr>
    {{range $t := .TopicStats}}
    <tr>
        <td>
            <form class="form-inline" style="margin:0" action="/tombstone_topic_producer" method="POST">
                <input type="hidden" name="rd" value="/topic/{{.TopicName}}">
                <input type="hidden" name="topic" value="{{.TopicName}}">
                <input type="hidden" name="node" value="{{.HostAddress}}">
                <button class="btn btn-mini btn-link red" type="submit">✘</button> <a href="/node/{{.HostAddress}}">{{.HostAddress}}</a>
                {{if $t.Paused}} <span class="label label-important">paused</span>{{end}}
            </form>
        </td>
        <td>
            {{.Depth | commafy}}</td>
        <td>{{.MemoryDepth | commafy}} + {{.BackendDepth | commafy}}</td>
        <td>{{.MessageCount | commafy}}</td>
            {{if $g.Enabled}}<td class="bold rate" target="{{$g.Rate $t}}"></td> {{end}}
        <td>{{.ChannelCount}}</td>
        {{if $t.E2eProcessingLatency.Percentiles}}
            {{range $e2e := $t.E2eProcessingLatency.Percentiles}}
              <td>
                <span title="{{$e2e.quantile | floatToPercent}}: min = {{$e2e.min | nanotohuman}}, max = {{$e2e.max | nanotohuman}}">{{$e2e.average | nanotohuman}}</span>
              </td>
            {{end}}
        {{end}}
    </tr>
    {{if $g.Enabled}}
        <tr class="graph-row">
            <td></td>
            <td><a href="{{$g.LargeGraph $t "depth"}}"><img width="120" src="{{$g.Sparkline $t "depth"}}"></a></td>
            <td></td>
            <td><a href="{{$g.LargeGraph $t "message_count"}}"><img width="120" src="{{$g.Sparkline $t "message_count"}}"></a></td>
            <td></td>
            <td></td>
            {{if $t.E2eProcessingLatency.Percentiles}}
                <td colspan="{{len $t.E2eProcessingLatency.Percentiles}}">
                    <a href="{{$g.LargeGraph $t.E2eProcessingLatency "e2e_processing_latency"}}"><img width="120" height="20"  src="{{$g.Sparkline $t.E2eProcessingLatency "e2e_processing_latency"}}"></a>
                </td>
            {{end}}
        </tr>
    {{end}}

    {{end}}
    <tr class="info">
        <td>Total:</td>
        <td>
            {{$gts.Depth | commafy}}
        </td>
        <td>{{$gts.MemoryDepth | commafy}} + {{$gts.BackendDepth | commafy}}</td>
        <td>
            {{$gts.MessageCount | commafy}}
        </td>
        {{if $g.Enabled}}<td class="bold rate" target="{{$g.Rate $gts}}"></td> {{end}}
        <td>{{$gts.ChannelCount}}</td>
        {{if $gts.E2eProcessingLatency.Percentiles}}
            {{range $e2e := $gts.E2eProcessingLatency.Percentiles}}
              <td>
                <span title="{{$e2e.quantile | floatToPercent}}: min = {{$e2e.min | nanotohuman}}, max = {{$e2e.max | nanotohuman}}">{{$e2e.average | nanotohuman}}</span>
              </td>
            {{end}}
        {{end}}
    </tr>
    {{if $g.Enabled}}
        <tr class="graph-row">
            <td></td>
            <td><a href="{{$g.LargeGraph $gts "depth"}}"><img width="120" src="{{$g.Sparkline $gts "depth"}}"></a></td>
            <td></td>
            <td><a href="{{$g.LargeGraph $gts "message_count"}}"><img width="120" src="{{$g.Sparkline $gts "message_count"}}"></a></td>
            <td></td>
            <td></td>
            {{if $gts.E2eProcessingLatency.Percentiles}}
                <td colspan="{{len $gts.E2eProcessingLatency.Percentiles}}">
                    <a href="{{$g.LargeGraph $gts.E2eProcessingLatency "e2e_processing_latency"}}"><img width="120" height="20"  src="{{$g.Sparkline $gts.E2eProcessingLatency "e2e_processing_latency"}}"></a>
                </td>
            {{end}}
        </tr>
    {{end}}
</table>
{{end}}
</div></div>

<div class="row-fluid">
{{ if not .ChannelStats }}
<div class="span6">
<h4>Channel Message Queues</h4>
<div class="alert">
    <h4>Notice</h4> No channels exist for this topic.
    <p>Messages will queue at the topic until a channel is created.
</div>
{{else}}
<div class="span12">
<h4>Channel Message Queues</h4>
<table class="table table-bordered table-condensed">
    {{if $firstTopic.E2eProcessingLatency.Percentiles}}
      <tr>
          <th colspan="{{if $g.Enabled}}10{{else}}9{{end}}"></th>
          <th colspan="{{len $firstTopic.E2eProcessingLatency.Percentiles}}">E2E Processing Latency</th>
      </tr>
    {{end}}
    <tr>
        <th>Channel</th>
        <th>Depth</th>
        <th>Memory + Disk</th>
        <th>In-Flight</th>
        <th>Deferred</th>
        <th>Requeued</th>
        <th>Timed Out</th>
        <th>Messages</th>
        {{if $g.Enabled}}<th>Rate</th>{{end}}
        <th>Connections</th>
        {{range $e2e := $firstTopic.E2eProcessingLatency.Percentiles}}
          <th>{{$e2e.quantile | floatToPercent}}<sup>{{$e2e.quantile | percSuffix}}</sup></th>
        {{end}}
    </tr>

{{range $c := .ChannelStats}}
    <tr >
        <th><a href="/topic/{{$c.TopicName | urlquery}}/{{$c.ChannelName | urlquery}}">{{$c.ChannelName}}</a> 
            {{if $c.Paused}}<span class="label label-important">paused</span>{{end}}
            </th>
        <td>
            {{$c.Depth | commafy}}</td>
        <td>{{$c.MemoryDepth | commafy}} + {{$c.BackendDepth | commafy}}</td>
        <td>{{$c.InFlightCount | commafy}}</td>
        <td>{{$c.DeferredCount | commafy}}</td>
        <td>{{$c.RequeueCount | commafy}}</td>
        <td>{{$c.TimeoutCount | commafy}}</td>
        <td>{{$c.MessageCount | commafy}}</td>
        {{if $g.Enabled}}<td class="bold rate" target="{{$g.Rate $c}}"></td> {{end}}
        <td>{{$c.ClientCount}}</td>
        {{if $c.E2eProcessingLatency.Percentiles}}
          {{range $e2e := $c.E2eProcessingLatency.Percentiles}}
            <td>
              <span title="{{$e2e.quantile | floatToPercent}}: min = {{$e2e.min | nanotohuman}}, max = {{$e2e.max | nanotohuman}}">{{$e2e.average | nanotohuman}}</span>
            </td>
          {{end}}
        {{end}}
    </tr>
    {{if $g.Enabled}}
    <tr class="graph-row">
        <td></td>
        <td><a href="{{$g.LargeGraph $c "depth"}}"><img width="120" height="20"  src="{{$g.Sparkline $c "depth"}}"></a></td>
        <td></td>
        <td><a href="{{$g.LargeGraph $c "in_flight_count"}}"><img width="120" height="20"  src="{{$g.Sparkline $c "in_flight_count"}}"></a></td>
        <td><a href="{{$g.LargeGraph $c "deferred_count"}}"><img width="120" height="20"  src="{{$g.Sparkline $c "deferred_count"}}"></a></td>
        <td><a href="{{$g.LargeGraph $c "requeue_count"}}"><img width="120" height="20"  src="{{$g.Sparkline $c "requeue_count"}}"></a></td>
        <td><a href="{{$g.LargeGraph $c "timeout_count"}}"><img width="120" height="20"  src="{{$g.Sparkline $c "timeout_count"}}"></a></td>
        <td><a href="{{$g.LargeGraph $c "message_count"}}"><img width="120" height="20"  src="{{$g.Sparkline $c "message_count"}}"></a></td>
        <td></td>
        <td><a href="{{$g.LargeGraph $c "clients"}}"><img width="120" height="20"  src="{{$g.Sparkline $c "clients"}}"></a></td>
        {{if $c.E2eProcessingLatency.Percentiles}}
            <td colspan="{{len $c.E2eProcessingLatency.Percentiles}}">
                <a href="{{$g.LargeGraph $c.E2eProcessingLatency "e2e_processing_latency"}}"><img width="120" height="20"  src="{{$g.Sparkline $c.E2eProcessingLatency "e2e_processing_latency"}}"></a>
            </td>
        {{end}}
    </tr>
    {{end}}
{{end}}
</table>
{{end}}
</div></div>

{{template "js.html" .}}
{{template "footer.html" .}}
`)
}
