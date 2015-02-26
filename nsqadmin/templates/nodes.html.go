package templates

func init() {
	registerTemplate("nodes.html", `
{{template "header.html" .}}
{{$ld := .Lookupd}}

<div class="row-fluid"><div class="span12">
<h2>NSQD Hosts ({{.Producers | len}})</h2>
</div></div>

<div class="row-fluid"><div class="span12">
<table class="table table-bordered">
    <tr>
        <th>Hostname</th>
        <th>Broadcast Address</th>
        <th>TCP Port</th>
        <th>HTTP Port</th>
        <th>Version</th>
        {{if $ld}}
        <th>Lookupd Conns.</th>
        {{end}}
        <th>Topics</th>
    </tr>
    {{range $p := .Producers }}
    <tr {{if .OutOfDate}}class="warning"{{end}}>
        <td>{{.Hostname}}</td>
        <td><a href="/node/{{.BroadcastAddress}}:{{.HTTPPort}}">{{.BroadcastAddress}}</a></td>
        <td>{{.TCPPort}}</td>
        <td>{{.HTTPPort}}</td>
        <td>{{.Version}}</td>
        {{if $ld}}
        <td>
            <a href="#" class="conn-counts btn btn-small {{$p | getNodeConsistencyClass}}">{{.RemoteAddresses | len}}</a>
            <div style="display: none;">
                {{range .RemoteAddresses}}{{.}}<br/>{{end}}
            </div>
        </td>
        {{end}}
        <td>
        {{if .Topics}}
            <span class="badge">{{.Topics | len}}</span>
            {{range .Topics}}
            <a href="/topic/{{.Topic | urlquery}}" class="label {{if .Tombstoned}}label-important{{else}}label-info{{end}}" {{if .Tombstoned}}title="this topic is currently tombstoned on this node"{{end}}>{{.Topic}}</a>
            {{end}}
        {{end}}
        </td>
    </tr>
    {{ end }}
</table>
</div></div>

{{template "js.html" .}}
{{template "footer.html" .}}
`)
}
