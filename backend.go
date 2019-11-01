package houdini

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"code.cloudfoundry.org/garden"
	"code.cloudfoundry.org/lager"

	"github.com/docker/docker/client"
)

type Backend struct {
	cli    *client.Client
	logger lager.Logger

	containersDir string
	containers    map[string]*container
	containersL   sync.RWMutex

	containerNum uint32
}

func NewBackend(containersDir string, logger lager.Logger) *Backend {
	logger = logger.Session("backend")

	cli, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		panic(err)
	}

	return &Backend{
		cli:    cli,
		logger: logger,

		containersDir: containersDir,
		containers:    make(map[string]*container),

		containerNum: uint32(time.Now().UnixNano()),
	}
}

func (backend *Backend) Start() error {
	version, err := backend.cli.ServerVersion(context.Background())
	if err != nil {
		return err
	}

	backend.logger.Info("started", lager.Data{
		"server-version": version.APIVersion,
	})

	return nil
}

func (backend *Backend) Stop() {
	containers, _ := backend.Containers(nil)

	backend.logger.Info("stopping", lager.Data{
		"remaining-containers": len(containers),
	})

	for _, container := range containers {
		backend.Destroy(container.Handle())
	}
}

func (backend *Backend) GraceTime(c garden.Container) time.Duration {
	return c.(*container).currentGraceTime()
}

func (backend *Backend) Ping() error {
	return nil
}

func (backend *Backend) Capacity() (garden.Capacity, error) {
	return garden.Capacity{}, nil
}

func (backend *Backend) Create(spec garden.ContainerSpec) (garden.Container, error) {
	container, err := backend.newContainer(backend.logger, spec)
	if err != nil {
		return nil, err
	}

	backend.logger.Info("container-created", lager.Data{
		"handle": container.Handle(),
	})

	backend.containersL.Lock()
	backend.containers[container.Handle()] = container
	backend.containersL.Unlock()

	return container, nil
}

func (backend *Backend) Destroy(handle string) error {
	backend.containersL.RLock()
	container, found := backend.containers[handle]
	backend.containersL.RUnlock()

	if !found {
		return garden.ContainerNotFoundError{Handle: handle}
	}

	err := container.Stop(false)
	if err != nil {
		return err
	}

	err = container.cleanup()
	if err != nil {
		return err
	}

	backend.containersL.Lock()
	delete(backend.containers, handle)
	backend.containersL.Unlock()

	return nil
}

func (backend *Backend) Containers(filter garden.Properties) ([]garden.Container, error) {
	matchingContainers := []garden.Container{}

	backend.containersL.RLock()

	for _, container := range backend.containers {
		if containerHasProperties(container, filter) {
			matchingContainers = append(matchingContainers, container)
		}
	}

	backend.containersL.RUnlock()

	return matchingContainers, nil
}

func (backend *Backend) BulkInfo(handles []string) (map[string]garden.ContainerInfoEntry, error) {
	return map[string]garden.ContainerInfoEntry{}, nil
}

func (backend *Backend) BulkMetrics(handles []string) (map[string]garden.ContainerMetricsEntry, error) {
	return map[string]garden.ContainerMetricsEntry{}, nil
}

func (backend *Backend) Lookup(handle string) (garden.Container, error) {
	backend.containersL.RLock()
	container, found := backend.containers[handle]
	backend.containersL.RUnlock()

	if !found {
		return nil, garden.ContainerNotFoundError{Handle: handle}
	}

	return container, nil
}

func (backend *Backend) generateContainerID() string {
	containerNum := atomic.AddUint32(&backend.containerNum, 1)

	containerID := []byte{}

	var i uint64
	for i = 0; i < 11; i++ {
		containerID = strconv.AppendUint(
			containerID,
			(uint64(containerNum)>>(55-(i+1)*5))&31,
			32,
		)
	}

	return string(containerID)
}

func containerHasProperties(container *container, properties garden.Properties) bool {
	containerProps := container.currentProperties()

	for key, val := range properties {
		cval, ok := containerProps[key]
		if !ok {
			return false
		}

		if cval != val {
			return false
		}
	}

	return true
}
