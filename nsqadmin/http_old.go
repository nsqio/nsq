package nsqadmin

import (
	"net/http"

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
