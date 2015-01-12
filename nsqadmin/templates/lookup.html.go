package templates

func init() {
	registerTemplate("lookup.html", `
{{template "header.html" .}}

<div class="row-fluid">
    <div class="span12">
        <h2>Lookup</h2>
    </div>
</div>

{{if not .Lookupd}}
<div class="alert">
    <h4>Notice</h4> nsqadmin is not configured with nsqlookupd hosts
</div>
{{else}}
<div class="row-fluid">
    <div class="span4">
        <table class="table table-bordered table-condensed">
            <tr>
                <th>nsqlookupd Host</th>
            </tr>
            {{range .Lookupd}}
            <tr>
                <td>{{.}}</td>
            </tr>
            {{end}}
        </table>
    </div>
</div>

<div class="row-fluid">
    <div class="span4">
        {{if .TopicMap}}
        <div class="alert alert-info">
            Below is a tree of Topics/Channels that are currently inactive (i.e. not produced on any nsqd in the cluster but are present in the lookup data)
        </div>
        <ul>
            {{range $t, $channels := .TopicMap}}
            <li><form class="form-inline" style="margin:0" action="/delete_topic" method="POST">
                    <input type="hidden" name="rd" value="/lookup">
                    <input type="hidden" name="topic" value="{{$t}}">
                    <button class="btn btn-mini btn-link red" type="submit">✘</button><a href="/topic/{{$t | urlquery}}">{{$t}}</a>
                </form>
                <ul>
                    {{range $channels}}
                    <li><form class="form-inline" style="margin:0" action="/delete_channel" method="POST">
                        <input type="hidden" name="rd" value="/lookup">
                        <input type="hidden" name="topic" value="{{$t}}">
                        <input type="hidden" name="channel" value="{{.}}">
                        <button class="btn btn-mini btn-link red" type="submit">✘</button><a href="/topic/{{$t | urlquery}}/{{. | urlquery}}">{{.}}</a>
                    </form></li>
                    {{end}}
                </ul>
            </li>
            {{end}}
        </ul>
        {{else}}
        <div class="alert"><h4>Notice</h4>No inactive Topics</div>
        {{end}}
    </div>
</div>

<div class="row-fluid">
    <div class="span4">
        <form class="form" action="/create_topic_channel" method="POST">
            <fieldset>
                <legend>Create Topic/Channel</legend>
                <div class="alert alert-info">
                    <p>This provides a way to setup a stream hierarchy
                    before services are deployed to production.
                    <p>If <em>Channel Name</em> is empty, just the topic is created.
                </div>
                <input type="text" name="topic" placeholder="Topic Name">
                <input type="text" name="channel" placeholder="Channel Name"><br/>
                <button class="btn btn-info" type="submit">Create</button>
            </fieldset>
        </form>
    </div>
</div>
{{end}}

{{template "js.html" .}}
{{template "footer.html" .}}
`)
}
