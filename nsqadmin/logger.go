package main

type logger interface {
	Output(maxdepth int, s string) error
}
