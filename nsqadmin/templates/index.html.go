package templates

func init() {
	registerTemplate("index.html", `
{{template "header.html" .}}
{{$g := .GraphOptions}}

<div class="row-fluid"><div class="span12">
<h2>Topics</h2>
</div></div>


<div class="row-fluid"><div class="span6">
{{if .Topics}}
<table class="table table-condensed table-bordered">
    <tr>
        <th>Topic</th>
        {{if $g.Enabled}}<th width="120">Depth</th>{{end}}
        {{if $g.Enabled}}<th width="120">Messages</th>{{end}}
        {{if $g.Enabled}}<th width="120">Rate</th>{{end}}
    </tr>
{{range $t := .Topics}}
    <tr>
        <td><a href="/topic/{{.TopicName | urlquery}}">{{.TopicName}}</a></td>
        {{if $g.Enabled}}<td><a href="/topic/{{.TopicName | urlquery}}"><img width="120" height="20" src="{{$g.Sparkline $t "depth"}}"></a></td>{{end}}
        {{if $g.Enabled}}<td><a href="/topic/{{.TopicName | urlquery}}"><img width="120" height="20" src="{{$g.Sparkline $t "message_count"}}"></a></td>{{end}}
        {{if $g.Enabled}}<td class="bold rate" target="{{$g.Rate $t}}"></td> {{end}}
    </tr>
{{end}}
</table>
{{else}}
<div class="alert"><h4>Notice</h4>No Topics Found</div>
{{end}}

</div></div>

{{template "js.html" .}}
{{template "footer.html" .}}
`)
}
