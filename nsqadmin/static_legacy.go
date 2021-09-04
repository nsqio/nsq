//go:build !go1.16
// +build !go1.16

package nsqadmin

func staticAsset(name string) ([]byte, error) {
	return Asset(name)
}
