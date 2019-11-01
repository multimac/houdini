package houdini

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"sync"
	"time"

	"code.cloudfoundry.org/garden"
	"code.cloudfoundry.org/lager"
	docker_types "github.com/docker/docker/api/types"
	docker_container "github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
)

type UndefinedPropertyError struct {
	Key string
}

func (err UndefinedPropertyError) Error() string {
	return fmt.Sprintf("property does not exist: %s", err.Key)
}

type UnknownProcessError struct {
	ProcessID string
}

func (e UnknownProcessError) Error() string {
	return fmt.Sprintf("unknown process: %s", e.ProcessID)
}

type container struct {
	cli    *client.Client
	spec   garden.ContainerSpec
	logger lager.Logger

	handle      string
	containerId string

	processes      map[string]*process
	processesMutex *sync.RWMutex

	properties  garden.Properties
	propertiesL sync.RWMutex

	env []string

	graceTime  time.Duration
	graceTimeL sync.RWMutex
}

func (backend *Backend) newContainer(logger lager.Logger, spec garden.ContainerSpec) (*container, error) {
	logger = logger.Session("container")

	properties := spec.Properties
	if properties == nil {
		properties = garden.Properties{}
	}

	var image string
	if spec.RootFSPath != "" {
		rootfsURI, err := url.Parse(spec.RootFSPath)
		if err != nil {
			return nil, err
		}

		switch rootfsURI.Scheme {
		case "docker":
			if rootfsURI.Fragment != "" {
				image = rootfsURI.Path[1:] + ":" + rootfsURI.Fragment
			} else {
				image = rootfsURI.Path[1:] + ":latest"
			}
		default:
			return nil, fmt.Errorf("unsupported rootfs uri (must be docker://): %s", spec.RootFSPath)
		}
	} else {
		return nil, fmt.Errorf("unsupported spec")
	}

	logger.Info("pulling-image", lager.Data{
		"image": image,
	})

	reader, err := backend.cli.ImagePull(context.Background(), image, docker_types.ImagePullOptions{})
	if err != nil {
		return nil, err
	}

	loadResp, err := backend.cli.ImageLoad(context.Background(), reader, false)
	if err != nil {
		return nil, err
	}
	loadResp.Body.Close()

	resp, err := backend.cli.ContainerCreate(context.Background(), &docker_container.Config{
		Image:  image,
		Cmd:    []string{"c:\\windows\\system32\\cmd.exe"},
		Env:    spec.Env,
		Labels: properties,
	}, nil, nil, "")
	if err != nil {
		return nil, err
	}

	handle := resp.ID
	if spec.Handle != "" {
		handle = spec.Handle
	}

	logger.Info("container-created", lager.Data{
		"image":  image,
		"id":     resp.ID,
		"handle": handle,
	})

	err = backend.cli.ContainerStart(context.Background(), resp.ID, docker_types.ContainerStartOptions{})
	if err != nil {
		return nil, err
	}

	return &container{
		cli:    backend.cli,
		spec:   spec,
		logger: logger,

		handle:      handle,
		containerId: resp.ID,

		processes:      make(map[string]*process),
		processesMutex: new(sync.RWMutex),

		properties: properties,

		env: spec.Env,
	}, nil
}

func (container *container) cleanup() error {
	container.logger.Info("removing-container", lager.Data{
		"id": container.containerId,
	})

	return container.cli.ContainerRemove(context.Background(), container.containerId, docker_types.ContainerRemoveOptions{
		RemoveVolumes: true,
		RemoveLinks:   true,
		Force:         true,
	})
}

func (container *container) Handle() string {
	return container.handle
}

func (container *container) Stop(kill bool) error {
	container.logger.Info("stopping-container", lager.Data{
		"id": container.containerId,
	})

	return container.cli.ContainerStop(context.Background(), container.containerId, nil)
}

func (container *container) Info() (garden.ContainerInfo, error) { return garden.ContainerInfo{}, nil }

func (container *container) StreamIn(spec garden.StreamInSpec) error {
	return container.cli.CopyToContainer(context.Background(), container.containerId, spec.Path, spec.TarStream, docker_types.CopyToContainerOptions{
		AllowOverwriteDirWithFile: false,
		CopyUIDGID:                false,
	})
}

func (container *container) StreamOut(spec garden.StreamOutSpec) (io.ReadCloser, error) {
	reader, _, err := container.cli.CopyFromContainer(context.Background(), container.containerId, spec.Path)
	return reader, err
}

type waitCloser struct {
	io.ReadCloser
	wait <-chan error
}

func (c waitCloser) Close() error {
	err := c.ReadCloser.Close()
	if err != nil {
		return err
	}

	return <-c.wait
}

func (container *container) LimitBandwidth(limits garden.BandwidthLimits) error { return nil }

func (container *container) CurrentBandwidthLimits() (garden.BandwidthLimits, error) {
	return garden.BandwidthLimits{}, nil
}

func (container *container) LimitCPU(limits garden.CPULimits) error { return nil }

func (container *container) CurrentCPULimits() (garden.CPULimits, error) {
	return garden.CPULimits{}, nil
}

func (container *container) LimitDisk(limits garden.DiskLimits) error { return nil }

func (container *container) CurrentDiskLimits() (garden.DiskLimits, error) {
	return garden.DiskLimits{}, nil
}

func (container *container) LimitMemory(limits garden.MemoryLimits) error { return nil }

func (container *container) CurrentMemoryLimits() (garden.MemoryLimits, error) {
	return garden.MemoryLimits{}, nil
}

func (container *container) NetIn(hostPort, containerPort uint32) (uint32, uint32, error) {
	return 0, 0, nil
}

func (container *container) NetOut(garden.NetOutRule) error { return nil }

func (container *container) BulkNetOut([]garden.NetOutRule) error { return nil }

func (container *container) Run(spec garden.ProcessSpec, processIO garden.ProcessIO) (garden.Process, error) {
	container.processesMutex.Lock()
	defer container.processesMutex.Unlock()

	process, err := container.newProcess(container.logger, spec, processIO)
	if err != nil {
		return nil, err
	}

	container.logger.Info("process-created", lager.Data{
		"handle": process.ID(),
	})

	process.Attach(processIO)
	err = process.Start(spec.TTY)
	if err != nil {
		return nil, err
	}

	container.logger.Info("process-started", lager.Data{
		"handle": process.ID(),
	})

	container.processes[process.ID()] = process

	return process, nil
}

func (container *container) Attach(processID string, processIO garden.ProcessIO) (garden.Process, error) {
	container.processesMutex.RLock()
	process, ok := container.processes[processID]
	container.processesMutex.RUnlock()

	if !ok {
		return nil, UnknownProcessError{processID}
	}

	process.Attach(processIO)

	go container.waitAndReap(processID)

	return process, nil
}

func (container *container) Property(name string) (string, error) {
	container.propertiesL.RLock()
	property, found := container.properties[name]
	container.propertiesL.RUnlock()

	if !found {
		return "", UndefinedPropertyError{name}
	}

	return property, nil
}

func (container *container) SetProperty(name string, value string) error {
	container.propertiesL.Lock()
	container.properties[name] = value
	container.propertiesL.Unlock()

	return nil
}

func (container *container) RemoveProperty(name string) error {
	container.propertiesL.Lock()
	defer container.propertiesL.Unlock()

	_, found := container.properties[name]
	if !found {
		return UndefinedPropertyError{name}
	}

	delete(container.properties, name)

	return nil
}

func (container *container) Properties() (garden.Properties, error) {
	return container.currentProperties(), nil
}

func (container *container) Metrics() (garden.Metrics, error) {
	return garden.Metrics{}, nil
}

func (container *container) SetGraceTime(t time.Duration) error {
	container.graceTimeL.Lock()
	container.graceTime = t
	container.graceTimeL.Unlock()
	return nil
}

func (container *container) currentProperties() garden.Properties {
	properties := garden.Properties{}

	container.propertiesL.RLock()

	for k, v := range container.properties {
		properties[k] = v
	}

	container.propertiesL.RUnlock()

	return properties
}

func (container *container) currentGraceTime() time.Duration {
	container.graceTimeL.RLock()
	defer container.graceTimeL.RUnlock()
	return container.graceTime
}

func (container *container) waitAndReap(processID string) {
	container.processesMutex.RLock()
	process, ok := container.processes[processID]
	container.processesMutex.RUnlock()

	if !ok {
		return
	}

	process.Wait()

	container.unregister(processID)
}

func (container *container) unregister(processID string) {
	container.processesMutex.Lock()
	defer container.processesMutex.Unlock()

	delete(container.processes, processID)
}
