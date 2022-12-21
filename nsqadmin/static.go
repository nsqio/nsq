package nsqadmin

import (
	"embed"
)

//go:embed static/build
var static embed.FS

func staticAsset(name string) ([]byte, error) {
	return static.ReadFile("static/build/" + name)
}
