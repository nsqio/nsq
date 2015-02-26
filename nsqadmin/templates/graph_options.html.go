package templates

func init() {
	registerTemplate("graph_options.html", `
{{if .GraphOptions.Configured}}

<li class="dropdown">
  <a class="dropdown-toggle" data-toggle="dropdown" href="#">
      <i class="icon-picture icon-white"></i> {{.GraphOptions.GraphInterval.Timeframe}}
      <span class="caret"></span>
  </a>

<ul class="dropdown-menu">
  {{range .GraphOptions.AllGraphIntervals}}
    <li><a href="?{{.URLOption}}">{{.Timeframe}}</a></li>
  {{end}}
</ul>

</li>
{{end}}
`)
}
