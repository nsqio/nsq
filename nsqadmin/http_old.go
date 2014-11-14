package nsqadmin

import (
	"io/ioutil"
	"net/http"

	"github.com/bitly/nsq/internal/http_api"
	"github.com/julienschmidt/httprouter"
)

func (s *httpServer) staticAssetHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	// assetName := ps.ByName("asset")
	//
	// asset, error := Asset(assetName)
	// if error != nil {
	// 	return nil, http_api.Err{404, "NOT_FOUND"}
	// }
	//
	// if strings.HasSuffix(assetName, ".js") {
	// 	w.Header().Set("Content-Type", "text/javascript")
	// } else if strings.HasSuffix(assetName, ".css") {
	// 	w.Header().Set("Content-Type", "text/css")
	// }
	//
	//
	return nil, nil
}

func (s *httpServer) indexHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	// asset, error := Asset("index.html")
	// if error != nil {
	// 	s.ctx.nsqadmin.logf("ERROR: embedded asset access - %s : %s", "index.html", error)
	// 	return nil, http_api.Err{404, "NOT_FOUND"}
	// }
	//
	// t, _ := template.New("index").Parse(string(asset))
	//
	// w.Header().Set("Content-Type", "text/html")
	// t.Execute(w, struct {
	// 	Version      string
	// 	GraphEnabled bool
	// 	GraphiteURL  string
	// 	NSQLookupd   []string
	// }{
	// 	Version:      version.Binary,
	// 	GraphEnabled: s.ctx.nsqadmin.opts.GraphiteURL != "",
	// 	GraphiteURL:  s.ctx.nsqadmin.opts.GraphiteURL,
	// 	NSQLookupd:   s.ctx.nsqadmin.opts.NSQLookupdHTTPAddresses,
	// })
	return nil, nil
}

func (s *httpServer) graphiteDataHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	metric, err := reqParams.Get("metric")
	if err != nil || metric != "rate" {
		return nil, http_api.Err{404, "INVALID_ARG_METRIC"}
	}

	target, err := reqParams.Get("target")
	if err != nil {
		return nil, http_api.Err{404, "INVALID_ARG_TARGET"}
	}

	query := rateQuery(target, s.ctx.nsqadmin.opts.StatsdInterval)
	url := s.ctx.nsqadmin.opts.GraphiteURL + query
	s.ctx.nsqadmin.logf("GRAPHITE: %s", url)
	response, err := graphiteGet(url)
	if err != nil {
		s.ctx.nsqadmin.logf("ERROR: graphite request failed - %s", err)
		return nil, http_api.Err{500, "INTERNAL_ERROR"}
	}

	resp, err := parseRateResponse(response, s.ctx.nsqadmin.opts.StatsdInterval)
	if err != nil {
		s.ctx.nsqadmin.logf("ERROR: response formatting failed - %s", err)
		return nil, http_api.Err{500, "INTERNAL_ERROR"}
	}

	w.Header().Set("Content-Type", "application/json")
	return resp, nil
}

func graphiteGet(url string) ([]byte, error) {
	response, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	contents, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	return contents, nil
}
