package houdini

import (
	"context"
	"io"
	"sync"

	"code.cloudfoundry.org/garden"
	"code.cloudfoundry.org/lager"
	docker_types "github.com/docker/docker/api/types"
	docker_container "github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
)

type process struct {
	logger      lager.Logger
	cli         *client.Client
	containerId string
	execId      string
	id          string

	waiting    *sync.Once
	exitStatus int
	exitErr    error

	stdin  *faninWriter
	stdout *fanoutWriter
	stderr *fanoutWriter
}

func (container *container) newProcess(logger lager.Logger, spec garden.ProcessSpec, processIO garden.ProcessIO) (*process, error) {
	logger = logger.Session("process")

	resp, err := container.cli.ContainerExecCreate(context.Background(), container.containerId, docker_types.ExecConfig{
		Env:          spec.Env,
		Cmd:          append([]string{spec.Path}, spec.Args...),
		WorkingDir:   spec.Dir,
		AttachStdin:  processIO.Stdin != nil,
		AttachStdout: processIO.Stdout != nil,
		AttachStderr: processIO.Stderr != nil,
		Tty:          spec.TTY.WindowSize != nil,
	})

	if err != nil {
		return nil, err
	}

	logger.Info("process-created", lager.Data{
		"container": container.containerId,
		"id":        resp.ID,
	})

	process := &process{
		cli:         container.cli,
		logger:      logger,
		containerId: container.containerId,
		execId:      resp.ID,
		id:          spec.ID,

		waiting: &sync.Once{},

		stdin:  &faninWriter{hasSink: make(chan struct{})},
		stdout: &fanoutWriter{},
		stderr: &fanoutWriter{},
	}

	return process, nil
}

func (process *process) ID() string {
	return process.id
}

func (process *process) Wait() (int, error) {
	statusCh, errCh := process.cli.ContainerWait(context.Background(), process.containerId, docker_container.WaitConditionNotRunning)
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

func (process *process) SetTTY(tty garden.TTYSpec) error {
	if tty.WindowSize != nil {
		return process.cli.ContainerExecResize(context.Background(), process.execId, docker_types.ResizeOptions{
			Height: uint(tty.WindowSize.Rows),
			Width:  uint(tty.WindowSize.Columns),
		})
	}

	return nil
}

func (process *process) Start(tty *garden.TTYSpec) error {
	resp, err := process.cli.ContainerExecAttach(context.Background(), process.execId, docker_types.ExecStartCheck{
		Detach: false,
		Tty:    tty.WindowSize != nil,
	})

	if err != nil {
		return err
	}

	err = process.cli.ContainerExecStart(context.Background(), process.execId, docker_types.ExecStartCheck{
		Detach: false,
		Tty:    tty.WindowSize != nil,
	})

	if err != nil {
		return err
	}

	if tty.WindowSize == nil {
		go stdcopy.StdCopy(process.stdout, process.stderr, resp.Reader)
	} else {
		go io.Copy(process.stdout, resp.Reader)
	}

	process.stdin.AddSink(resp.Conn)
	return nil
}

func (process *process) Attach(processIO garden.ProcessIO) {
	if processIO.Stdin != nil {
		process.stdin.AddSource(processIO.Stdin)
	}

	if processIO.Stdout != nil {
		process.stdout.AddSink(processIO.Stdout)
	}

	if processIO.Stderr != nil {
		process.stderr.AddSink(processIO.Stderr)
	}
}

func (process *process) Signal(signal garden.Signal) error {
	return nil
}
