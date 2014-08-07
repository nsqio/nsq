package nsqlookupd

type logger interface {
	Output(maxdepth int, s string) error
}
