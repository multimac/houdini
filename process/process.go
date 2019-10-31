package process

import (
	"context"
	"io"
	"sync"

	"code.cloudfoundry.org/garden"
	docker_types "github.com/docker/docker/api/types"
	docker_container "github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
)

type process interface {
	Signal(garden.Signal) error
	Wait() (int, error)
	SetWindowSize(garden.WindowSize) error
}

type Process struct {
	cli         *client.Client
	id          string
	containerID string
	execID      string

	process process

	waiting    *sync.Once
	exitStatus int
	exitErr    error

	stdin  *faninWriter
	stdout *fanoutWriter
	stderr *fanoutWriter
}

func NewProcess(cli *client.Client, id string, containerID string, execID string) *Process {
	return &Process{
		cli:         cli,
		id:          id,
		containerID: containerID,
		execID:      execID,

		waiting: &sync.Once{},

		stdin:  &faninWriter{hasSink: make(chan struct{})},
		stdout: &fanoutWriter{},
		stderr: &fanoutWriter{},
	}
}

func (p *Process) ID() string {
	return p.id
}

func (p *Process) Wait() (int, error) {
	statusCh, errCh := p.cli.ContainerWait(context.Background(), p.containerID, docker_container.WaitConditionNotRunning)
	select {
	case err := <-errCh:
		if err != nil {
			return -1, err
		}
	case body := <-statusCh:
		return int(body.StatusCode), nil
	}
	return -1, nil
}

func (p *Process) SetTTY(tty garden.TTYSpec) error {
	if tty.WindowSize != nil {
		return p.cli.ContainerExecResize(context.Background(), p.execID, docker_types.ResizeOptions{
			Height: uint(tty.WindowSize.Rows),
			Width:  uint(tty.WindowSize.Columns),
		})
	}

	return nil
}

func (p *Process) Start(tty *garden.TTYSpec) error {
	err := p.cli.ContainerExecStart(context.Background(), p.execID, docker_types.ExecStartCheck{
		Detach: false,
		Tty:    tty.WindowSize != nil,
	})

	if err != nil {
		return err
	}

	resp, err := p.cli.ContainerExecAttach(context.Background(), p.execID, docker_types.ExecStartCheck{
		Detach: false,
		Tty:    tty.WindowSize != nil,
	})

	if err != nil {
		return err
	}

	if tty.WindowSize == nil {
		go stdcopy.StdCopy(p.stdout, p.stderr, resp.Reader)
	} else {
		go io.Copy(p.stdout, resp.Reader)
	}

	p.stdin.AddSink(resp.Conn)
	return nil
}

func (p *Process) Attach(processIO garden.ProcessIO) {
	if processIO.Stdin != nil {
		p.stdin.AddSource(processIO.Stdin)
	}

	if processIO.Stdout != nil {
		p.stdout.AddSink(processIO.Stdout)
	}

	if processIO.Stderr != nil {
		p.stderr.AddSink(processIO.Stderr)
	}
}

func (p *Process) Signal(signal garden.Signal) error {
	return nil
}
