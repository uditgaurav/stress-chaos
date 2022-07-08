package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/containerd/cgroups"
	cgroupsv2 "github.com/containerd/cgroups/v2"
	"github.com/pkg/errors"
	"k8s.io/klog"
)

//list of cgroups in a container
var (
	cgroupSubsystemList = []string{"cpu", "memory", "systemd", "net_cls",
		"net_prio", "freezer", "blkio", "perf_event", "devices", "cpuset",
		"cpuacct", "pids", "hugetlb",
	}
)

var (
	err           error
	inject, abort chan os.Signal
)

const (
	// ProcessAlreadyFinished contains error code when process is finished
	ProcessAlreadyFinished = "os: process already finished"
)

// Helper injects the stress chaos
func main() {

	experimentsDetails := ExperimentDetails{}
	getENV(experimentsDetails)

	// inject channel is used to transmit signal notifications.
	inject = make(chan os.Signal, 1)
	// Catch and relay certain signal(s) to inject channel.
	signal.Notify(inject, os.Interrupt, syscall.SIGTERM)

	// abort channel is used to transmit signal notifications.
	abort = make(chan os.Signal, 1)
	// Catch and relay certain signal(s) to abort channel.
	signal.Notify(abort, os.Interrupt, syscall.SIGTERM)

	//Fetching all the ENV passed for the helper pod
	fmt.Println("[PreReq]: Getting the ENV variables")

	if err := prepareStressChaos(experimentsDetails); err != nil {
		klog.Errorf("helper pod failed, err: %v", err)
	}
}

//prepareStressChaos contains the chaos preparation and injection steps
func prepareStressChaos(experimentsDetails ExperimentDetails) error {

	select {
	case <-inject:
		// stopping the chaos execution, if abort signal received
		os.Exit(1)
	default:

		// extract out the pid of the target container
		containerID := "802b2058b5c8"
		targetPID := 23938
		getENV(experimentsDetails)

		cgroupManager, err := getCGroupManager(int(targetPID), containerID)
		if err != nil {
			return errors.Errorf("fail to get the cgroup manager, err: %v", err)
		}

		// get stressors in list format
		stressorList := prepareStressor(experimentsDetails)
		if len(stressorList) == 0 {
			return errors.Errorf("fail to prepare stressor for %v experiment", experimentsDetails.ExperimentName)
		}
		// stressors := strings.Join(stressorList, " ")
		stressCommand := "pause nsutil -t " + strconv.Itoa(targetPID) + " -p -- stress-ng --cpu 2 --timeout 120s"
		fmt.Println("[Info]: starting process: %v", stressCommand)

		// launch the stress-ng process on the target container in paused mode
		cmd := exec.Command("/bin/bash", "-c", stressCommand)
		var buf bytes.Buffer
		cmd.Stdout = &buf
		err = cmd.Start()
		if err != nil {
			return errors.Errorf("fail to start the stress process %v, err: %v", stressCommand, err)
		}

		fmt.Println("Done running the command")

		// watching for the abort signal and revert the chaos if an abort signal is received
		go abortWatcher(cmd.Process.Pid, experimentsDetails.TargetPods)

		// add the stress process to the cgroup of target container
		if err = addProcessToCgroup(cmd.Process.Pid, cgroupManager); err != nil {
			if killErr := cmd.Process.Kill(); killErr != nil {
				return errors.Errorf("stressors failed killing %v process, err: %v", cmd.Process.Pid, killErr)
			}
			return errors.Errorf("fail to add the stress process into target container cgroup, err: %v", err)
		}

		fmt.Println("[Info]: Sending signal to resume the stress process")
		// wait for the process to start before sending the resume signal
		// TODO: need a dynamic way to check the start of the process
		time.Sleep(700 * time.Millisecond)

		// remove pause and resume or start the stress process
		if err := cmd.Process.Signal(syscall.SIGCONT); err != nil {
			return errors.Errorf("fail to remove pause and start the stress process: %v", err)
		}

		fmt.Println("[Wait]: Waiting for chaos completion")
		// channel to check the completion of the stress process
		done := make(chan error)
		go func() { done <- cmd.Wait() }()

		// check the timeout for the command
		// Note: timeout will occur when process didn't complete even after 10s of chaos duration
		timeout := time.After((time.Duration(150)) * time.Second)

		select {
		case <-timeout:
			// the stress process gets timeout before completion
			fmt.Printf("[Chaos] The stress process is not yet completed after the chaos duration of %vs", experimentsDetails.ChaosDuration+150)
			fmt.Println("[Timeout]: Killing the stress process")
			if err = terminateProcess(cmd.Process.Pid); err != nil {
				return err
			}

			return nil
		case err := <-done:
			if err != nil {
				err, ok := err.(*exec.ExitError)
				if ok {
					status := err.Sys().(syscall.WaitStatus)
					if status.Signaled() && status.Signal() == syscall.SIGTERM {
						// wait for the completion of abort handler
						time.Sleep(10 * time.Second)
						return errors.Errorf("process stopped with SIGTERM signal")
					}
				}
				return errors.Errorf("process exited before the actual cleanup, err: %v", err)
			}
			fmt.Println("[Info]: Chaos injection completed")
			terminateProcess(cmd.Process.Pid)
		}
	}
	return nil
}

//terminateProcess will remove the stress process from the target container after chaos completion
func terminateProcess(pid int) error {
	process, err := os.FindProcess(pid)
	if err != nil {
		return errors.Errorf("unreachable path, err: %v", err)
	}
	if err = process.Signal(syscall.SIGTERM); err != nil && err.Error() != ProcessAlreadyFinished {
		return errors.Errorf("error while killing process, err: %v", err)
	}
	fmt.Println("[Info]: Stress process removed successfully")
	return nil
}

//prepareStressor will set the required stressors for the given experiment
func prepareStressor(experimentDetails ExperimentDetails) []string {

	stressArgs := []string{
		"stress-ng",
		"--cpu 2 --timeout 120s",
	}

	// switch experimentDetails.StressType {
	// case "pod-cpu-stress":

	// 	stressArgs = append(stressArgs, "")

	// case "pod-memory-stress":

	// 	fmt.PrintlnWithValues("[Info]: Details of Stressor:", logrus.Fields{
	// 		"Number of Workers":  experimentDetails.NumberOfWorkers,
	// 		"Memory Consumption": experimentDetails.MemoryConsumption,
	// 		"Timeout":            experimentDetails.ChaosDuration,
	// 	})
	// 	stressArgs = append(stressArgs, "--vm "+experimentDetails.NumberOfWorkers+" --vm-bytes "+experimentDetails.MemoryConsumption+"M")

	// case "pod-io-stress":
	// 	var hddbytes string
	// 	if experimentDetails.FilesystemUtilizationBytes == "0" {
	// 		if experimentDetails.FilesystemUtilizationPercentage == "0" {
	// 			hddbytes = "10%"
	// 			fmt.Println("Neither of FilesystemUtilizationPercentage or FilesystemUtilizationBytes provided, proceeding with a default FilesystemUtilizationPercentage value of 10%")
	// 		} else {
	// 			hddbytes = experimentDetails.FilesystemUtilizationPercentage + "%"
	// 		}
	// 	} else {
	// 		if experimentDetails.FilesystemUtilizationPercentage == "0" {
	// 			hddbytes = experimentDetails.FilesystemUtilizationBytes + "G"
	// 		} else {
	// 			hddbytes = experimentDetails.FilesystemUtilizationPercentage + "%"
	// 			log.Warn("Both FsUtilPercentage & FsUtilBytes provided as inputs, using the FsUtilPercentage value to proceed with stress exp")
	// 		}
	// 	}
	// 	fmt.PrintlnWithValues("[Info]: Details of Stressor:", logrus.Fields{
	// 		"io":                experimentDetails.NumberOfWorkers,
	// 		"hdd":               experimentDetails.NumberOfWorkers,
	// 		"hdd-bytes":         hddbytes,
	// 		"Timeout":           experimentDetails.ChaosDuration,
	// 		"Volume Mount Path": experimentDetails.VolumeMountPath,
	// 	})
	// 	if experimentDetails.VolumeMountPath == "" {
	// 		stressArgs = append(stressArgs, "--io "+experimentDetails.NumberOfWorkers+" --hdd "+experimentDetails.NumberOfWorkers+" --hdd-bytes "+hddbytes)
	// 	} else {
	// 		stressArgs = append(stressArgs, "--io "+experimentDetails.NumberOfWorkers+" --hdd "+experimentDetails.NumberOfWorkers+" --hdd-bytes "+hddbytes+" --temp-path "+experimentDetails.VolumeMountPath)
	// 	}
	// 	if experimentDetails.CPUcores != "0" {
	// 		stressArgs = append(stressArgs, "--cpu %v", experimentDetails.CPUcores)
	// 	}

	// default:
	// 	fmt.Errorf("stressor for %v experiment is not suported", experimentDetails.ExperimentName)
	// }
	return stressArgs
}

//pidPath will get the pid path of the container
func pidPath(pid int) cgroups.Path {
	processPath := "/proc/" + strconv.Itoa(pid) + "/cgroup"
	paths, err := parseCgroupFile(processPath)
	if err != nil {
		return getErrorPath(errors.Wrapf(err, "parse cgroup file %s", processPath))
	}
	return getExistingPath(paths, pid, "")
}

//parseCgroupFile will read and verify the cgroup file entry of a container
func parseCgroupFile(path string) (map[string]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, errors.Errorf("unable to parse cgroup file: %v", err)
	}
	defer file.Close()
	return parseCgroupFromReader(file)
}

//parseCgroupFromReader will parse the cgroup file from the reader
func parseCgroupFromReader(r io.Reader) (map[string]string, error) {
	var (
		cgroups = make(map[string]string)
		s       = bufio.NewScanner(r)
	)
	for s.Scan() {
		var (
			text  = s.Text()
			parts = strings.SplitN(text, ":", 3)
		)
		if len(parts) < 3 {
			return nil, errors.Errorf("invalid cgroup entry: %q", text)
		}
		for _, subs := range strings.Split(parts[1], ",") {
			if subs != "" {
				cgroups[subs] = parts[2]
			}
		}
	}
	if err := s.Err(); err != nil {
		return nil, errors.Errorf("buffer scanner failed: %v", err)
	}

	return cgroups, nil
}

//getExistingPath will be used to get the existing valid cgroup path
func getExistingPath(paths map[string]string, pid int, suffix string) cgroups.Path {
	for n, p := range paths {
		dest, err := getCgroupDestination(pid, n)
		if err != nil {
			return getErrorPath(err)
		}
		rel, err := filepath.Rel(dest, p)
		if err != nil {
			return getErrorPath(err)
		}
		if rel == "." {
			rel = dest
		}
		paths[n] = filepath.Join("/", rel)
	}
	return func(name cgroups.Name) (string, error) {
		root, ok := paths[string(name)]
		if !ok {
			if root, ok = paths[fmt.Sprintf("name=%s", name)]; !ok {
				return "", cgroups.ErrControllerNotActive
			}
		}
		if suffix != "" {
			return filepath.Join(root, suffix), nil
		}
		return root, nil
	}
}

//getErrorPath will give the invalid cgroup path
func getErrorPath(err error) cgroups.Path {
	return func(_ cgroups.Name) (string, error) {
		return "", err
	}
}

//getCgroupDestination will validate the subsystem with the mountpath in container mountinfo file.
func getCgroupDestination(pid int, subsystem string) (string, error) {
	mountinfoPath := fmt.Sprintf("/proc/%d/mountinfo", pid)
	file, err := os.Open(mountinfoPath)
	if err != nil {
		return "", err
	}
	defer file.Close()
	s := bufio.NewScanner(file)
	for s.Scan() {
		fields := strings.Fields(s.Text())
		for _, opt := range strings.Split(fields[len(fields)-1], ",") {
			if opt == subsystem {
				return fields[3], nil
			}
		}
	}
	if err := s.Err(); err != nil {
		return "", err
	}
	return "", errors.Errorf("no destination found for %v ", subsystem)
}

//findValidCgroup will be used to get a valid cgroup path
func findValidCgroup(path cgroups.Path, target string) (string, error) {
	for _, subsystem := range cgroupSubsystemList {
		path, err := path(cgroups.Name(subsystem))
		if err != nil {
			fmt.Errorf("fail to retrieve the cgroup path, subsystem: %v, target: %v, err: %v", subsystem, target, err)
			continue
		}
		if strings.Contains(path, target) {
			return path, nil
		}
	}
	return "", errors.Errorf("never found valid cgroup for %s", target)
}

type ExperimentDetails struct {
	ExperimentName                  string
	EngineName                      string
	ChaosDuration                   int
	LIBImage                        string
	LIBImagePullPolicy              string
	RampTime                        int
	ChaosLib                        string
	AppNS                           string
	AppLabel                        string
	AppKind                         string
	InstanceID                      string
	ChaosNamespace                  string
	ChaosPodName                    string
	RunID                           string
	TargetContainer                 string
	StressImage                     string
	Timeout                         int
	Delay                           int
	TargetPods                      string
	PodsAffectedPerc                string
	ContainerRuntime                string
	ChaosServiceAccount             string
	SocketPath                      string
	Sequence                        string
	TerminationGracePeriodSeconds   int
	CPUcores                        int
	CPULoad                         int
	FilesystemUtilizationPercentage int
	FilesystemUtilizationBytes      int
	NumberOfWorkers                 int
	MemoryConsumption               int
	VolumeMountPath                 string
	StressType                      string
	IsTargetContainerProvided       bool
	NodeLabel                       string
	SetHelperData                   string
}

//getENV fetches all the env variables from the runner pod
func getENV(experimentDetails ExperimentDetails) {
	experimentDetails.ExperimentName = Getenv("EXPERIMENT_NAME", "")
	experimentDetails.InstanceID = Getenv("INSTANCE_ID", "")
	experimentDetails.AppNS = Getenv("APP_NAMESPACE", "")
	experimentDetails.TargetContainer = Getenv("APP_CONTAINER", "")
	experimentDetails.TargetPods = Getenv("APP_POD", "")
	experimentDetails.ChaosDuration, _ = strconv.Atoi(Getenv("TOTAL_CHAOS_DURATION", "30"))
	experimentDetails.ChaosNamespace = Getenv("CHAOS_NAMESPACE", "litmus")
	experimentDetails.EngineName = Getenv("CHAOSENGINE", "")
	experimentDetails.ChaosPodName = Getenv("POD_NAME", "")
	experimentDetails.ContainerRuntime = Getenv("CONTAINER_RUNTIME", "")
	experimentDetails.SocketPath = Getenv("SOCKET_PATH", "")
	experimentDetails.CPUcores = 2
	experimentDetails.FilesystemUtilizationPercentage = 10
	experimentDetails.FilesystemUtilizationBytes = 200
	experimentDetails.NumberOfWorkers = 4
	experimentDetails.MemoryConsumption = 500
	experimentDetails.VolumeMountPath = Getenv("VOLUME_MOUNT_PATH", "")
	experimentDetails.StressType = Getenv("STRESS_TYPE", "pod-cpu-stress")
}

// Getenv fetch the env and set the default value, if any
func Getenv(key string, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		value = defaultValue
	}
	return value
}

// abortWatcher continuously watch for the abort signals
func abortWatcher(targetPID int, targetPodName string) {

	<-abort

	fmt.Println("[Chaos]: Killing process started because of terminated signal received")
	fmt.Println("[Abort]: Chaos Revert Started")
	// retry thrice for the chaos revert
	retry := 3
	for retry > 0 {
		if err = terminateProcess(targetPID); err != nil {
			fmt.Errorf("unable to kill stress process, err :%v", err)
		}
		retry--
		time.Sleep(1 * time.Second)
	}
	fmt.Println("[Abort]: Chaos Revert Completed")
	os.Exit(1)
}

// getCGroupManager will return the cgroup for the given pid of the process
func getCGroupManager(pid int, containerID string) (interface{}, error) {
	if cgroups.Mode() == cgroups.Unified {
		groupPath, err := cgroupsv2.PidGroupPath(pid)
		if err != nil {
			return nil, errors.Errorf("Error in getting groupPath, %v", err)
		}

		cgroup2, err := cgroupsv2.LoadManager("/sys/fs/cgroup", groupPath)
		if err != nil {
			return nil, errors.Errorf("Error loading cgroup v2 manager, %v", err)
		}
		return cgroup2, nil
	}
	path := pidPath(pid)
	cgroup, err := findValidCgroup(path, containerID)
	if err != nil {
		return nil, errors.Errorf("fail to get cgroup, err: %v", err)
	}
	cgroup1, err := cgroups.Load(cgroups.V1, cgroups.StaticPath(cgroup))
	if err != nil {
		return nil, errors.Errorf("fail to load the cgroup, err: %v", err)
	}

	return cgroup1, nil
}

// addProcessToCgroup will add the process to cgroup
// By default it will add to v1 cgroup
func addProcessToCgroup(pid int, control interface{}) error {
	if cgroups.Mode() == cgroups.Unified {
		var cgroup1 = control.(*cgroupsv2.Manager)
		return cgroup1.AddProc(uint64(pid))
	}
	var cgroup1 = control.(cgroups.Cgroup)
	return cgroup1.Add(cgroups.Process{Pid: pid})
}
