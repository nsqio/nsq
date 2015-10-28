// +build windows

package winservice

import (
	"errors"
	"os"
	"testing"
	"time"

	"github.com/nsqio/nsq/internal/test"
	"golang.org/x/sys/windows/svc"
)

func setupWinServiceTest(wsf *mockWinServiceFuncs) {
	// wsfWrapper allows signalNotify, svcIsAnInteractiveSession, and svcRun
	// to be set once. Inidivual test functions set "wsf" to add behavior.
	wsfWrapper := &mockWinServiceFuncs{
		signalNotify: func(c chan<- os.Signal, sig ...os.Signal) {
			if c == nil {
				panic("os/signal: Notify using nil channel")
			}

			if wsf.signalNotify != nil {
				wsf.signalNotify(c, sig...)
			} else {
				wsf1 := *wsf
				go func() {
					for val := range wsf1.sigChan {
						for _, registeredSig := range sig {
							if val == registeredSig {
								c <- val
							}
						}
					}
				}()
			}
		},
		svcIsAnInteractiveSession: func() (bool, error) {
			return wsf.svcIsAnInteractiveSession()
		},
		svcRun: func(name string, handler svc.Handler) error {
			return wsf.svcRun(name, handler)
		},
	}

	signalNotify = wsfWrapper.signalNotify
	svcIsAnInteractiveSession = wsfWrapper.svcIsAnInteractiveSession
	svcRun = wsfWrapper.svcRun
}

type mockWinServiceFuncs struct {
	signalNotify              func(chan<- os.Signal, ...os.Signal)
	svcIsAnInteractiveSession func() (bool, error)
	svcRun                    func(string, svc.Handler) error
	sigChan                   chan os.Signal
	ws                        *windowsService
	executeReturnedBool       bool
	executeReturnedUInt32     uint32
	changes                   []svc.Status
}

type mockProgram struct {
	start       func() error
	stop        func() error
	beforeStart func(Environment) error
}

func (p *mockProgram) Start() error {
	return p.start()
}

func (p *mockProgram) Stop() error {
	return p.stop()
}

func (p *mockProgram) BeforeStart(wse Environment) error {
	return p.beforeStart(wse)
}

func makeProgram(startCalled, stopCalled, beforeStartCalled *int) *mockProgram {
	return &mockProgram{
		start: func() error {
			*startCalled++
			return nil
		},
		stop: func() error {
			*stopCalled++
			return nil
		},
		beforeStart: func(wse Environment) error {
			*beforeStartCalled++
			return nil
		},
	}
}

func setWindowsServiceFuncs(isInteractive bool, onRunningSendCmd *svc.Cmd) (*mockWinServiceFuncs, chan<- svc.ChangeRequest) {
	changeRequestChan := make(chan svc.ChangeRequest, 4)
	changesChan := make(chan svc.Status)
	done := make(chan struct{})

	var wsf *mockWinServiceFuncs
	wsf = &mockWinServiceFuncs{
		sigChan: make(chan os.Signal),
		svcIsAnInteractiveSession: func() (bool, error) {
			return isInteractive, nil
		},
		svcRun: func(name string, handler svc.Handler) error {
			wsf.ws = handler.(*windowsService)
			wsf.executeReturnedBool, wsf.executeReturnedUInt32 = handler.Execute(nil, changeRequestChan, changesChan)
			done <- struct{}{}
			return nil
		},
	}

	var currentState svc.State

	go func() {
	loop:
		for {
			select {
			case change := <-changesChan:
				wsf.changes = append(wsf.changes, change)
				currentState = change.State

				if change.State == svc.Running && onRunningSendCmd != nil {
					changeRequestChan <- svc.ChangeRequest{
						Cmd:           *onRunningSendCmd,
						CurrentStatus: svc.Status{State: currentState},
					}
				}
			case <-done:
				break loop
			}
		}
	}()

	setupWinServiceTest(wsf)

	return wsf, changeRequestChan
}

func TestWinService_RunWindowsService_Interactive(t *testing.T) {
	// arrange / act / assert
	for _, osSignal := range []os.Signal{os.Kill, os.Interrupt} {
		testRunWindowsServiceInteractive(t, osSignal)
	}
}

func TestWinService_RunWindowsService_NonInteractive(t *testing.T) {
	for _, svcCmd := range []svc.Cmd{svc.Stop, svc.Shutdown} {
		testRunWindowsServiceNonInteractive(t, svcCmd)
	}
}

func testRunWindowsServiceInteractive(t *testing.T, osSignal os.Signal) {
	// arrange
	var startCalled, stopCalled, beforeStartCalled int
	prg := makeProgram(&startCalled, &stopCalled, &beforeStartCalled)

	wsf, _ := setWindowsServiceFuncs(true, nil)

	go func() {
		wsf.sigChan <- osSignal
	}()

	// act
	if err := Run(prg); err != nil {
		t.Fatal(err)
	}

	// assert
	test.Equal(t, 1, startCalled)
	test.Equal(t, 1, stopCalled)
	test.Equal(t, 1, beforeStartCalled)
	test.Equal(t, 0, len(wsf.changes))
}

func testRunWindowsServiceNonInteractive(t *testing.T, svcCmd svc.Cmd) {
	// arrange
	var startCalled, stopCalled, beforeStartCalled int
	prg := makeProgram(&startCalled, &stopCalled, &beforeStartCalled)

	wsf, _ := setWindowsServiceFuncs(false, &svcCmd)

	// act
	if err := Run(prg); err != nil {
		t.Fatal(err)
	}

	// assert
	changes := wsf.changes

	test.Equal(t, 1, startCalled)
	test.Equal(t, 1, stopCalled)
	test.Equal(t, 1, beforeStartCalled)

	test.Equal(t, 3, len(changes))
	test.Equal(t, svc.StartPending, changes[0].State)
	test.Equal(t, svc.Running, changes[1].State)
	test.Equal(t, svc.StopPending, changes[2].State)

	test.Equal(t, false, wsf.executeReturnedBool)
	test.Equal(t, uint32(0), wsf.executeReturnedUInt32)

	test.Nil(t, wsf.ws.getError())
}

func TestRunWindowsServiceNonInteractive_StartError(t *testing.T) {
	// arrange
	var startCalled, stopCalled, beforeStartCalled int
	prg := makeProgram(&startCalled, &stopCalled, &beforeStartCalled)
	prg.start = func() error {
		startCalled++
		return errors.New("start error")
	}

	svcStop := svc.Stop
	wsf, _ := setWindowsServiceFuncs(false, &svcStop)

	// act
	err := Run(prg)

	// assert
	test.Equal(t, "start error", err.Error())

	changes := wsf.changes

	test.Equal(t, 1, startCalled)
	test.Equal(t, 0, stopCalled)
	test.Equal(t, 1, beforeStartCalled)

	test.Equal(t, 1, len(changes))
	test.Equal(t, svc.StartPending, changes[0].State)

	test.Equal(t, true, wsf.executeReturnedBool)
	test.Equal(t, uint32(1), wsf.executeReturnedUInt32)

	test.Equal(t, "start error", wsf.ws.getError().Error())
}

func TestRunWindowsServiceInteractive_StartError(t *testing.T) {
	// arrange
	var startCalled, stopCalled, beforeStartCalled int
	prg := makeProgram(&startCalled, &stopCalled, &beforeStartCalled)
	prg.start = func() error {
		startCalled++
		return errors.New("start error")
	}

	wsf, _ := setWindowsServiceFuncs(true, nil)

	// act
	err := Run(prg)

	// assert
	test.Equal(t, "start error", err.Error())

	changes := wsf.changes

	test.Equal(t, 1, startCalled)
	test.Equal(t, 0, stopCalled)
	test.Equal(t, 1, beforeStartCalled)

	test.Equal(t, 0, len(changes))
}

func TestRunWindowsService_BeforeStartError(t *testing.T) {
	// arrange
	var startCalled, stopCalled, beforeStartCalled int
	prg := makeProgram(&startCalled, &stopCalled, &beforeStartCalled)
	prg.beforeStart = func(Environment) error {
		beforeStartCalled++
		return errors.New("before start error")
	}

	wsf, _ := setWindowsServiceFuncs(false, nil)

	// act
	err := Run(prg)

	// assert
	test.Equal(t, "before start error", err.Error())

	changes := wsf.changes

	test.Equal(t, 0, startCalled)
	test.Equal(t, 0, stopCalled)
	test.Equal(t, 1, beforeStartCalled)

	test.Equal(t, 0, len(changes))
}

func TestRunWindowsService_IsAnInteractiveSessionError(t *testing.T) {
	// arrange
	var startCalled, stopCalled, beforeStartCalled int
	prg := makeProgram(&startCalled, &stopCalled, &beforeStartCalled)

	wsf, _ := setWindowsServiceFuncs(false, nil)
	wsf.svcIsAnInteractiveSession = func() (bool, error) {
		return false, errors.New("IsAnInteractiveSession error")
	}

	// act
	err := Run(prg)

	// assert
	test.Equal(t, "IsAnInteractiveSession error", err.Error())

	changes := wsf.changes

	test.Equal(t, 0, startCalled)
	test.Equal(t, 0, stopCalled)
	test.Equal(t, 0, beforeStartCalled)

	test.Equal(t, 0, len(changes))
}

func TestRunWindowsServiceNonInteractive_RunError(t *testing.T) {
	// arrange
	var startCalled, stopCalled, beforeStartCalled int
	prg := makeProgram(&startCalled, &stopCalled, &beforeStartCalled)

	svcStop := svc.Stop
	wsf, _ := setWindowsServiceFuncs(false, &svcStop)
	wsf.svcRun = func(name string, handler svc.Handler) error {
		wsf.ws = handler.(*windowsService)
		return errors.New("svc.Run error")
	}

	// act
	err := Run(prg)

	// assert
	test.Equal(t, "svc.Run error", err.Error())

	changes := wsf.changes

	test.Equal(t, 0, startCalled)
	test.Equal(t, 0, stopCalled)
	test.Equal(t, 1, beforeStartCalled)

	test.Equal(t, 0, len(changes))

	test.Nil(t, wsf.ws.getError())
}

func TestRunWindowsServiceNonInteractive_Interrogate(t *testing.T) {
	// arrange
	var startCalled, stopCalled, beforeStartCalled int
	prg := makeProgram(&startCalled, &stopCalled, &beforeStartCalled)

	wsf, changeRequest := setWindowsServiceFuncs(false, nil)

	time.AfterFunc(50*time.Millisecond, func() {
		// ignored, PausePending won't be in changes slice
		// make sure we don't panic/err on unexpected values
		changeRequest <- svc.ChangeRequest{
			Cmd:           svc.Pause,
			CurrentStatus: svc.Status{State: svc.PausePending},
		}
	})

	time.AfterFunc(100*time.Millisecond, func() {
		// handled, Paused will be in changes slice
		changeRequest <- svc.ChangeRequest{
			Cmd:           svc.Interrogate,
			CurrentStatus: svc.Status{State: svc.Paused},
		}
	})

	time.AfterFunc(200*time.Millisecond, func() {
		// handled, but CurrentStatus overridden with StopPending;
		// ContinuePending won't be in changes slice
		changeRequest <- svc.ChangeRequest{
			Cmd:           svc.Stop,
			CurrentStatus: svc.Status{State: svc.ContinuePending},
		}
	})

	// act
	if err := Run(prg); err != nil {
		t.Fatal(err)
	}

	// assert
	changes := wsf.changes

	test.Equal(t, 1, startCalled)
	test.Equal(t, 1, stopCalled)
	test.Equal(t, 1, beforeStartCalled)

	test.Equal(t, 4, len(changes))
	test.Equal(t, svc.StartPending, changes[0].State)
	test.Equal(t, svc.Running, changes[1].State)
	test.Equal(t, svc.Paused, changes[2].State)
	test.Equal(t, svc.StopPending, changes[3].State)

	test.Equal(t, false, wsf.executeReturnedBool)
	test.Equal(t, uint32(0), wsf.executeReturnedUInt32)

	test.Nil(t, wsf.ws.getError())
}

func TestRunWindowsServiceInteractive_StopError(t *testing.T) {
	// arrange
	var startCalled, stopCalled, beforeStartCalled int
	prg := makeProgram(&startCalled, &stopCalled, &beforeStartCalled)
	prg.stop = func() error {
		stopCalled++
		return errors.New("stop error")
	}

	wsf, _ := setWindowsServiceFuncs(true, nil)

	go func() {
		wsf.sigChan <- os.Interrupt
	}()

	// act
	err := Run(prg)

	// assert
	test.Equal(t, "stop error", err.Error())
	test.Equal(t, 1, startCalled)
	test.Equal(t, 1, stopCalled)
	test.Equal(t, 1, beforeStartCalled)
	test.Equal(t, 0, len(wsf.changes))
}

func TestRunWindowsServiceNonInteractive_StopError(t *testing.T) {
	// arrange
	var startCalled, stopCalled, beforeStartCalled int
	prg := makeProgram(&startCalled, &stopCalled, &beforeStartCalled)
	prg.stop = func() error {
		stopCalled++
		return errors.New("stop error")
	}

	shutdownCmd := svc.Shutdown
	wsf, _ := setWindowsServiceFuncs(false, &shutdownCmd)

	// act
	err := Run(prg)

	// assert
	changes := wsf.changes

	test.Equal(t, "stop error", err.Error())

	test.Equal(t, 1, startCalled)
	test.Equal(t, 1, stopCalled)
	test.Equal(t, 1, beforeStartCalled)

	test.Equal(t, 3, len(changes))
	test.Equal(t, svc.StartPending, changes[0].State)
	test.Equal(t, svc.Running, changes[1].State)
	test.Equal(t, svc.StopPending, changes[2].State)

	test.Equal(t, true, wsf.executeReturnedBool)
	test.Equal(t, uint32(2), wsf.executeReturnedUInt32)

	test.Equal(t, "stop error", wsf.ws.getError().Error())
}
