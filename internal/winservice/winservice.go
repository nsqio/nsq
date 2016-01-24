// +build windows

package winservice

import (
	"os"
	"os/signal"
	"sync"

	"golang.org/x/sys/windows/svc"
)

// Create variables for svc and signal functions so we can mock them in tests
var svcIsAnInteractiveSession = svc.IsAnInteractiveSession
var svcRun = svc.Run
var signalNotify = signal.Notify

// WindowsService interface contains Start and Stop methods which are called
// when the service is started and stopped.
//
// The Start method must be non-blocking.
//
// Implement this interface and pass it to RunWindowsService to enable running
// as a Windows Service.
type WindowsService interface {
	BeforeStart(Environment) error
	Start() error
	Stop() error
}

// Environment interface contains information about the environment
// your application is running in.
//
// IsInteractive() indicates whether your application has been executed from the
// command line or as a Windows Service.
type Environment interface {
	run() error
	IsAnInteractiveSession() bool
}

type windowsService struct {
	i             WindowsService
	errSync       sync.Mutex
	stopStartErr  error
	isInteractive bool
	Name          string
}

// Run runs your WindowsService. The callback argument is called
// just before WindowsService.Start() is executed; typically this is where you
// would make adjustments based on Service.IsInteractive(). The callback
// argument may be nil.
func Run(i WindowsService) error {
	var err error

	interactive, err := svcIsAnInteractiveSession()
	if err != nil {
		return err
	}
	ws := &windowsService{
		i:             i,
		isInteractive: interactive,
	}

	err = i.BeforeStart(ws)
	if err != nil {
		return err
	}

	return ws.run()
}

func (ws *windowsService) setError(err error) {
	ws.errSync.Lock()
	ws.stopStartErr = err
	ws.errSync.Unlock()
}

func (ws *windowsService) getError() error {
	ws.errSync.Lock()
	err := ws.stopStartErr
	ws.errSync.Unlock()
	return err
}

func (ws *windowsService) IsAnInteractiveSession() bool {
	return ws.isInteractive
}

func (ws *windowsService) run() error {
	ws.setError(nil)
	if !ws.IsAnInteractiveSession() {
		// Return error messages from start and stop routines
		// that get executed in the Execute method.
		// Guarded with a mutex as it may run a different thread
		// (callback from windows).
		runErr := svcRun(ws.Name, ws)
		startStopErr := ws.getError()
		if startStopErr != nil {
			return startStopErr
		}
		if runErr != nil {
			return runErr
		}
		return nil
	}
	err := ws.i.Start()
	if err != nil {
		return err
	}

	sigChan := make(chan os.Signal)

	signalNotify(sigChan, os.Interrupt, os.Kill)

	<-sigChan

	err = ws.i.Stop()

	return err
}

// The Execute method is invoked by Windows
// The second argument of svc.Run(ws.Name, ws) conforms to the svc.Handler interface
func (ws *windowsService) Execute(args []string, r <-chan svc.ChangeRequest, changes chan<- svc.Status) (bool, uint32) {
	const cmdsAccepted = svc.AcceptStop | svc.AcceptShutdown
	changes <- svc.Status{State: svc.StartPending}

	if err := ws.i.Start(); err != nil {
		ws.setError(err)
		return true, 1
	}

	changes <- svc.Status{State: svc.Running, Accepts: cmdsAccepted}
loop:
	for {
		c := <-r
		switch c.Cmd {
		case svc.Interrogate:
			changes <- c.CurrentStatus
		case svc.Stop, svc.Shutdown:
			changes <- svc.Status{State: svc.StopPending}
			err := ws.i.Stop()
			if err != nil {
				ws.setError(err)
				return true, 2
			}
			break loop
		default:
			continue loop
		}
	}

	return false, 0
}
