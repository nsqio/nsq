package templates

func init() {
	registerTemplate("node.html", `
{{template "header.html" .}}
{{$g := .GraphOptions}}
{{$node := .Node}}
{{$numTopics := .TopicStats | len}}
{{$numMessages := .NumMessages | commafy}}
{{$numClients := .NumClients}}

<ul class="breadcrumb">
    <li><a href="/nodes">Nodes</a> <span class="divider">/</span></li>
    <li class="active">{{$node}}</li>
</ul>

{{if $g.Enabled}}
<div class="row-fluid">
  <div class="span8 offset2">
    <table class="table muted">
      <tr>
        <td>
          <a href="{{$g.LargeGraph $node "*_bytes"}}"><img class="img-polaroid" width="200" src="{{$g.LargeGraph $node "*_bytes"}}"/></a>
          <h5 style="text-align: center;">GC Pressure</h5>
        </td>
        <td>
          <a href="{{$g.LargeGraph $node "gc_pause_*"}}"><img class="img-polaroid" width="200" src="{{$g.LargeGraph $node "gc_pause_*"}}"/></a>
          <h5 style="text-align: center;">GC Pause Percentiles</h5>
        </td>
        <td>
          <a href="{{$g.LargeGraph $node "gc_runs"}}"><img class="img-polaroid" width="200" src="{{$g.LargeGraph $node "gc_runs"}}"/></a>
          <h5 style="text-align: center;">GC Runs</h5>
        </td>
        <td>
          <a href="{{$g.LargeGraph $node "heap_objects"}}"><img class="img-polaroid" width="200" src="{{$g.LargeGraph $node "heap_objects"}}"/></a>
          <h5 style="text-align: center;">Heap Objects In-Use</h5>
        </td>
      </tr>
    </table>
  </div>
</div>
{{end}}

<div class="row-fluid">
    <div class="span12">
    {{if not .TopicStats}}
        <div class="alert">
            <h4>Notice</h4> No topics exist on this node.
        </div>
    {{else}}
    {{range $t := .TopicStats}}
        <table class="table table-condensed">
            <tr>
                <td colspan="2"><strong>{{$numTopics}}</strong> Topics</td>
                <td colspan="7"></td>
                <td><strong>{{$numMessages}}</strong> Messages</td>
                <td><strong>{{$numClients}}</strong> Clients</td>
            </tr>
            <tr>
                <th colspan="3">Topic</th>
                <th>Depth</th>
                <th>Memory + Disk</th>
                <th colspan="4"></th>
                <th>Messages</th>
                <th>Channels</th>
            </tr>
            <tr class="info">
                <td colspan="3">
                    <form class="form-inline" style="margin:0" action="/tombstone_topic_producer" method="POST">
                        <input type="hidden" name="rd" value="/node/{{$node}}">
                        <input type="hidden" name="topic" value="{{.TopicName}}">
                        <input type="hidden" name="node" value="{{$node}}">
                        <button class="btn btn-mini btn-link red" type="submit">✘</button> {{.TopicName}}
                    </form>
                </td>
                <td>
                    {{if $g.Enabled}}<a href="{{$g.LargeGraph $t "depth"}}"><img width="120" src="{{$g.Sparkline $t "depth"}}"></a>{{end}}
                    {{.Depth | commafy}}</td>
                <td>{{.MemoryDepth | commafy}} + {{.BackendDepth | commafy}}</td>
                <td colspan="4"></td>
                <td>
                    {{if $g.Enabled}}<a href="{{$g.LargeGraph $t "message_count"}}"><img width="120" src="{{$g.Sparkline $t "message_count"}}"></a>{{end}}
                    {{.MessageCount | commafy}}
                    </td>
                <td>{{.ChannelCount}}</td>
            </tr>
            {{if not .Channels}}
            <tr>
                <td colspan="*">
                  <div class="alert"><h4>Notice</h4> No channels exist for this topic.</div>
                </td>
            </tr>
            {{else}}
            {{range $c := .Channels}}
            <tr>
                <th width="25"></th>
                <th colspan="2">Channel</th>
                <th>Depth</th>
                <th>Memory + Disk</th>
                <th>In-Flight</th>
                <th>Deferred</th>
                <th>Requeued</th>
                <th>Timed Out</th>
                <th>Messages</th>
                <th>Connections</th>
            </tr>
            <tr class="warning">
                <td></td>
                <td colspan="2">
                    {{$c.ChannelName}}
                    {{if $c.Paused}}<span class="label label-important">paused</span>{{end}}
                </td>
                <td>
                    {{if $g.Enabled}}<a href="{{$g.LargeGraph $c "depth"}}"><img width="120" height="20" src="{{$g.Sparkline $c "depth"}}"></a>{{end}}
                    {{$c.Depth | commafy}}</td>
                <td>{{$c.MemoryDepth | commafy}} + {{$c.BackendDepth | commafy}}</td>
                <td>{{$c.InFlightCount | commafy}}</td>
                <td>{{$c.DeferredCount | commafy}}</td>
                <td>{{$c.RequeueCount | commafy}}</td>
                <td>{{$c.TimeoutCount | commafy}}</td>
                <td>{{$c.MessageCount | commafy}}</td>
                <td>
                    {{if $g.Enabled}}<a href="{{$g.LargeGraph $c "clients"}}"><img width="120" height="20" src="{{$g.Sparkline $c "clients"}}"></a>{{end}}
                    {{$c.ClientCount}}
                </td>
            </tr>
            {{if not .Clients}}
            <tr>
                <td colspan="*">
                    <div class="alert"><h4>Notice</h4>No clients connected to this channel.</div>
                </td>
            </tr>
            {{else}}
            <tr>
                <th></th>
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
            {{range .Clients}}
            <tr>
                <td></td>
                <td>{{.ClientID}}</td>
                <td>{{.Version}} {{if .HasUserAgent}}({{.UserAgent}}){{end}}</td>
                <td>
                  {{if .HasSampleRate}}
                  <span class="label label-info">Sampled {{.SampleRate}}%</span>
                  {{end}}
                  {{if .TLS}}
                  <span class="label label-warning">TLS</span>
                  {{end}}
                  {{if .Deflate}}
                  <span class="label label-default">Delfate</span>
                  {{end}}
                  {{if .Snappy}}
                  <span class="label label-primary">Snappy</span>
                  {{end}}
                </td>
                <td>{{.HostAddress}}</td>
                <td>{{.InFlightCount | commafy}}</td>
                <td>{{.ReadyCount | commafy}}</td>
                <td>{{.FinishCount | commafy}}</td>
                <td>{{.RequeueCount | commafy}}</td>
                <td>{{.MessageCount | commafy}}</td>
                <td>{{.ConnectedDuration}}</td>
            </tr>
            {{end}}
            {{end}}
        {{end}}
        {{end}}
        </table>
    {{end}}
    {{end}}
</div>

{{template "js.html" .}}
{{template "footer.html" .}}
`)
}
