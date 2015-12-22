package nsqd

type logger interface {
	Level() int32
	SetLevel(int32)
	Output(maxdepth int, s string) error
	OutputErr(maxdepth int, s string) error
	OutputWarning(maxdepth int, s string) error
}
