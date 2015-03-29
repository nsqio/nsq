package nsqadmin

import (
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/bitly/go-simplejson"
	"github.com/bitly/nsq/internal/http_api"
	"github.com/bitly/nsq/internal/statsd"
)

type GraphTarget interface {
	Target(key string) ([]string, string)
	Host() string
}

type Node string

func (n Node) Target(key string) ([]string, string) {
	target := fmt.Sprintf("%%smem.%s", key)
	if key == "gc_runs" {
		target = fmt.Sprintf("movingAverage(%s,45)", target)
	}
	return []string{target}, "red,green,blue,purple"
}

func (n Node) Host() string {
	return string(n)
}

type Topic struct {
	TopicName string
}

type Topics []*Topic

func TopicsFromStrings(s []string) Topics {
	t := make(Topics, 0, len(s))
	for _, ss := range s {
		tt := &Topic{ss}
		t = append(t, tt)
	}
	return t
}

func (t *Topic) Target(key string) ([]string, string) {
	color := "blue"
	if key == "depth" || key == "deferred_count" {
		color = "red"
	}
	target := fmt.Sprintf("sumSeries(%%stopic.%s.%s)", t.TopicName, key)
	return []string{target}, color
}

func (t *Topic) Host() string {
	return "*"
}

type GraphInterval struct {
	Selected   bool
	Timeframe  string        // the UI string
	GraphFrom  string        // ?from=.
	GraphUntil string        // ?until=.
	Duration   time.Duration // for sort order
}

type GraphIntervals []*GraphInterval

func (g *GraphInterval) URLOption() template.URL {
	return template.URL(fmt.Sprintf("t=%s", g.Timeframe))
}

func DefaultGraphTimeframes(selected string) GraphIntervals {
	var d GraphIntervals
	for _, t := range []string{"1h", "2h", "12h", "24h", "48h", "168h", "off"} {
		g, _ := GraphIntervalForTimeframe(t, t == selected)
		d = append(d, g)
	}
	return d
}

func GraphIntervalForTimeframe(t string, selected bool) (*GraphInterval, error) {
	if t == "off" {
		return &GraphInterval{
			Selected:   selected,
			Timeframe:  t,
			GraphFrom:  "",
			GraphUntil: "",
			Duration:   0,
		}, nil
	}
	duration, err := time.ParseDuration(t)
	if err != nil {
		return nil, err
	}
	start, end := startEndForTimeframe(duration)
	return &GraphInterval{
		Selected:   selected,
		Timeframe:  t,
		GraphFrom:  start,
		GraphUntil: end,
		Duration:   duration,
	}, nil
}

type GraphOptions struct {
	ctx               *Context
	Configured        bool
	Enabled           bool
	GraphiteURL       string
	TimeframeString   template.URL
	AllGraphIntervals []*GraphInterval
	GraphInterval     *GraphInterval
}

func NewGraphOptions(rw http.ResponseWriter, req *http.Request,
	r *http_api.ReqParams, ctx *Context) *GraphOptions {
	selectedTimeString, err := r.Get("t")
	if err != nil && selectedTimeString == "" {
		// get from cookie
		cookie, err := req.Cookie("t")
		if err != nil {
			selectedTimeString = "2h"
		} else {
			selectedTimeString = cookie.Value
		}
	} else {
		// set cookie
		host, _, _ := net.SplitHostPort(req.Host)
		cookie := &http.Cookie{
			Name:     "t",
			Value:    selectedTimeString,
			Path:     "/",
			Domain:   host,
			Expires:  time.Now().Add(time.Duration(720) * time.Hour),
			HttpOnly: true,
		}
		http.SetCookie(rw, cookie)
	}
	g, err := GraphIntervalForTimeframe(selectedTimeString, true)
	if err != nil {
		g, _ = GraphIntervalForTimeframe("2h", true)
	}
	base := ctx.nsqadmin.opts.GraphiteURL
	if ctx.nsqadmin.opts.ProxyGraphite {
		base = ""
	}
	o := &GraphOptions{
		ctx:               ctx,
		Configured:        ctx.nsqadmin.opts.GraphiteURL != "",
		Enabled:           g.Timeframe != "off" && ctx.nsqadmin.opts.GraphiteURL != "",
		GraphiteURL:       base,
		AllGraphIntervals: DefaultGraphTimeframes(selectedTimeString),
		GraphInterval:     g,
	}
	return o
}

func (g *GraphOptions) Prefix(host string, metricType string) string {
	prefix := ""
	statsdHostKey := statsd.HostKey(host)
	prefixWithHost := strings.Replace(g.ctx.nsqadmin.opts.StatsdPrefix, "%s", statsdHostKey, -1)
	if prefixWithHost[len(prefixWithHost)-1] != '.' {
		prefixWithHost += "."
	}
	if g.ctx.nsqadmin.opts.UseStatsdPrefixes && metricType == "counter" {
		prefix += "stats_counts."
	} else if g.ctx.nsqadmin.opts.UseStatsdPrefixes && metricType == "gauge" {
		prefix += "stats.gauges."
	}
	prefix += prefixWithHost
	return prefix
}

func (g *GraphOptions) Sparkline(gr GraphTarget, key string) template.URL {
	params := url.Values{}
	params.Set("height", "20")
	params.Set("width", "120")
	params.Set("hideGrid", "true")
	params.Set("hideLegend", "true")
	params.Set("hideAxes", "true")
	params.Set("bgcolor", "ff000000") // transparent
	params.Set("fgcolor", "black")
	params.Set("margin", "0")
	params.Set("yMin", "0")
	params.Set("lineMode", "connected")
	params.Set("drawNullAsZero", "false")

	interval := fmt.Sprintf("%dsec", g.ctx.nsqadmin.opts.StatsdInterval/time.Second)
	targets, color := gr.Target(key)
	for _, target := range targets {
		target = fmt.Sprintf(target, g.Prefix(gr.Host(), metricType(key)))
		params.Add("target", fmt.Sprintf(`summarize(%s,"%s","avg")`, target, interval))
	}
	params.Add("colorList", color)

	params.Set("from", g.GraphInterval.GraphFrom)
	params.Set("until", g.GraphInterval.GraphUntil)
	return template.URL(fmt.Sprintf("%s/render?%s", g.GraphiteURL, params.Encode()))
}

func (g *GraphOptions) LargeGraph(gr GraphTarget, key string) template.URL {
	params := url.Values{}
	params.Set("height", "450")
	params.Set("width", "800")
	params.Set("bgcolor", "ff000000") // transparent
	params.Set("fgcolor", "999999")
	params.Set("yMin", "0")
	params.Set("lineMode", "connected")
	params.Set("drawNullAsZero", "false")

	interval := fmt.Sprintf("%dsec", g.ctx.nsqadmin.opts.StatsdInterval/time.Second)
	targets, color := gr.Target(key)
	for _, target := range targets {
		target = fmt.Sprintf(target, g.Prefix(gr.Host(), metricType(key)))
		target = fmt.Sprintf(`summarize(%s,"%s","avg")`, target, interval)
		if metricType(key) == "counter" {
			scale := fmt.Sprintf("%.04f", 1/float64(g.ctx.nsqadmin.opts.StatsdInterval/time.Second))
			target = fmt.Sprintf(`scale(%s,%s)`, target, scale)
		}
		params.Add("target", target)
	}
	params.Add("colorList", color)

	params.Set("from", g.GraphInterval.GraphFrom)
	params.Set("until", g.GraphInterval.GraphUntil)
	return template.URL(fmt.Sprintf("%s/render?%s", g.GraphiteURL, params.Encode()))
}

func (g *GraphOptions) Rate(gr GraphTarget) string {
	target, _ := gr.Target("message_count")
	return fmt.Sprintf(target[0], g.Prefix(gr.Host(), metricType("message_count")))
}

func metricType(key string) string {
	return map[string]string{
		"depth":           "gauge",
		"in_flight_count": "gauge",
		"deferred_count":  "gauge",
		"requeue_count":   "counter",
		"timeout_count":   "counter",
		"message_count":   "counter",
		"clients":         "gauge",
		"*_bytes":         "gauge",
		"gc_pause_*":      "gauge",
		"gc_runs":         "counter",
		"heap_objects":    "gauge",

		"e2e_processing_latency": "gauge",
	}[key]
}

func rateQuery(target string, statsdInterval time.Duration) string {
	params := url.Values{}
	fromInterval := fmt.Sprintf("-%dsec", statsdInterval*2/time.Second)
	params.Set("from", fromInterval)
	untilInterval := fmt.Sprintf("-%dsec", statsdInterval/time.Second)
	params.Set("until", untilInterval)
	params.Set("format", "json")
	params.Set("target", target)
	return fmt.Sprintf("/render?%s", params.Encode())
}

func parseRateResponse(body []byte, statsdInterval time.Duration) ([]byte, error) {
	js, err := simplejson.NewJson([]byte(body))
	if err != nil {
		return nil, err
	}

	js, ok := js.GetIndex(0).CheckGet("datapoints")
	if !ok {
		return nil, errors.New("datapoints not found")
	}

	var rateStr string
	rate, _ := js.GetIndex(0).GetIndex(0).Float64()
	if rate < 0 {
		rateStr = "N/A"
	} else {
		rateDivisor := statsdInterval / time.Second
		rateStr = fmt.Sprintf("%.2f", rate/float64(rateDivisor))
	}
	return json.Marshal(map[string]string{"datapoint": rateStr})
}

func startEndForTimeframe(t time.Duration) (string, string) {
	start := fmt.Sprintf("-%dmin", int(t.Minutes()))
	return start, "-1min"
}
