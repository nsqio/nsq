package templates

func init() {
	registerTemplate("channel.html", `
{{template "header.html" .}}
{{$g := .GraphOptions}}
{{$firstHost := .FirstHost}}

<ul class="breadcrumb">
  <li><a href="/">Streams</a> <span class="divider">/</span></li>
  <li><a href="/topic/{{.Topic | urlquery}}">{{.Topic}}</a> <span class="divider">/</span></li>
  <li class="active">{{.Channel}}</li>
</ul>

<div class="row-fluid">
    <div class="span6">
        <blockquote>
            <p>Topic: <strong>{{.Topic}}</strong>
            <p>Channel: <strong>{{.Channel}}</strong>
        </blockquote>
    </div>
</div>

{{if not .ChannelStats}}
<div class="row-fluid">
    <div class="span6">
        <div class="alert">
            <h4>Notice</h4> No producers exist for this topic/channel.
            <p>See <a href="/lookup">Lookup</a> for more information.
        </div>
    </div>
</div>
{{else}}
<div class="row-fluid">
    <div class="span2">
        <form action="/empty_channel" method="POST">
            <input type="hidden" name="topic" value="{{.ChannelStats.TopicName}}">
            <input type="hidden" name="channel" value="{{.ChannelStats.ChannelName}}">
            <button class="btn btn-medium btn-warning" type="submit">Empty Queue</button>
        </form>
    </div>
    <div class="span2">
        <form action="/delete_channel" method="POST">
            <input type="hidden" name="topic" value="{{.ChannelStats.TopicName}}">
            <input type="hidden" name="channel" value="{{.ChannelStats.ChannelName}}">
            <button class="btn btn-medium btn-danger" type="submit">Delete Channel</button>
        </form>
    </div>
    <div class="span2">
        {{if .ChannelStats.Paused}}
        <form action="/unpause_channel" method="POST">
            <input type="hidden" name="topic" value="{{.ChannelStats.TopicName}}">
            <input type="hidden" name="channel" value="{{.ChannelStats.ChannelName}}">
            <button class="btn btn-medium btn-success" type="submit">UnPause Channel</button>
        </form>
        {{else}}
        <form action="/pause_channel" method="POST">
            <input type="hidden" name="topic" value="{{.ChannelStats.TopicName}}">
            <input type="hidden" name="channel" value="{{.ChannelStats.ChannelName}}">
            <button class="btn btn-medium btn-inverse" type="submit">Pause Channel</button>
        </form>
        {{end}}
    </div>
</div>

<div class="row-fluid"><div class="span12">
<h4>Channel</h4>
<table class="table table-bordered table-condensed">
    <thead>
        <tr>
            <th>&nbsp;</th>
            <th colspan="4" class='text-center'>Message Queues</th>
            <th colspan="{{if $g.Enabled}}5{{else}}4{{end}}" class='text-center'>Statistics</th>
            {{if $firstHost.E2eProcessingLatency.Percentiles}}
              <th colspan="{{len $firstHost.E2eProcessingLatency.Percentiles}}">E2E Processing Latency</th>
            {{end}}
        </tr>
        <tr>
            <th>NSQd Host</th>
            <th>Depth</th>
            <th>Memory + Disk</th>
            <th>In-Flight</th>
            <th>Deferred</th>
            <th>Requeued</th>
            <th>Timed Out</th>
            <th>Messages</th>
            {{if $g.Enabled}}<th>Rate</th>{{end}}
            <th>Connections</th>
            {{range $e2e := $firstHost.E2eProcessingLatency.Percentiles}}
              <th>{{$e2e.quantile | floatToPercent}}<sup>{{$e2e.quantile | percSuffix}}</sup></th>
            {{end}}
        </tr>
    </thead>
    <tbody>

{{range $c := .ChannelStats.HostStats}}
        <tr>
            <td><a href="/node/{{$c.HostAddress}}">{{$c.HostAddress}}</a>{{if $c.Paused}} <span class="label label-important">paused</span>{{end}}</td>
            <td>{{$c.Depth | commafy}}</td>
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

{{ end }}
{{ with $c := .ChannelStats }}
        <tr class="info">
            <td>Total:</td>
            <td>{{$c.Depth | commafy}}</td>
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
        <tr class="info graph-row">
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
    </tbody>
{{ end }}
</table>
</div></div>

<h4>Client Connections</h4>

<div class="row-fluid"><div class="span12">
{{if not .ChannelStats.Clients}}
<div class="alert"><h4>Notice</h4>No clients connected to this channel</div>
{{else}}
<table class="table table-bordered table-condensed">
    <tr>
        <th>Client Host</th>
        <th>Protocol</th>
        <th>Attributes</th>
        <th>NSQd Host</th>
        <th>In-Flight</th>
        <th>Ready Count</th>
        <th>Finished</th>
        <th>Requeued</th>
        <th>Messages</th>
        <th>Connected</th>
    </tr>

{{range .ChannelStats.Clients}}
    <tr>
        <td title="{{.RemoteAddress}}">{{.ClientID}}</td>
        <td>{{.Version}} {{if .HasUserAgent}}<small>({{.UserAgent}})</small>{{end}}</td>
        <td>
          {{if .HasSampleRate}}
          <span class="label label-info">Sampled {{.SampleRate}}%</span>
          {{end}}
          {{if .TLS}}
          <span class="label label-warning" {{ if .TLSVersion }}title="{{.TLSVersion}} {{.CipherSuite}} {{.TLSNegotiatedProtocol}} mutual:{{.TLSNegotiatedProtocolIsMutual}}"{{ end }}>TLS</span>
          {{end}}
          {{if .Deflate}}
          <span class="label label-default">Delfate</span>
          {{end}}
          {{if .Snappy}}
          <span class="label label-primary">Snappy</span>
          {{end}}
          {{if .Authed}}
          <span class="label label-success">{{if .AuthIdentityURL}}<a href="{{.AuthIdentityURL}}">{{end}}<i class="icon-user icon-white" title="Authed{{if .AuthIdentity}} Identity:{{.AuthIdentity}}{{end}}"></i>{{if .AuthIdentityURL}}</a>{{end}}</span>
          {{end}}
        </td>
        <td><a href="/node/{{.HostAddress}}">{{.HostAddress}}</a></td>
        <td>{{.InFlightCount | commafy}}</td>
        <td>{{.ReadyCount | commafy}}</td>
        <td>{{.FinishCount | commafy}}</td>
        <td>{{.RequeueCount | commafy}}</td>
        <td>{{.MessageCount | commafy}}</td>
        <td>{{.ConnectedDuration}}</td>
    </tr>
{{end}}
</table>
{{end}}
</div></div>
{{end}}

{{template "js.html" .}}
{{template "footer.html" .}}
`)
}
