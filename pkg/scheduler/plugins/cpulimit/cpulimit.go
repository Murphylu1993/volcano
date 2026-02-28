/*
Copyright 2026 The Volcano Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cpulimit

import (
	"fmt"
	"strconv"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/framework"
)

const (
	// PluginName indicates name of volcano scheduler plugin.
	PluginName = "cpulimit"

	// CPUOnlyTasksMaxCPUAnnotation is the annotation key on nodes to specify the maximum
	// total CPU (in cores) that pure-CPU tasks can consume on this node.
	// Example: "8" means 8 cores.
	CPUOnlyTasksMaxCPUAnnotation = "volcano.sh/cpuonly-tasks-max-cpu"

	// CPUOnlyTasksMaxMemoryAnnotation is the annotation key on nodes to specify the maximum
	// total memory (in GB) that pure-CPU tasks can consume on this node.
	// Example: "16" means 16GB.
	CPUOnlyTasksMaxMemoryAnnotation = "volcano.sh/cpuonly-tasks-max-memory"
)

// milliCPUPerCore is the number of milliCPU per core.
const milliCPUPerCore = 1000

// bytesPerGB is the number of bytes per GB.
const bytesPerGB = 1000 * 1000 * 1000

// ignoredScalarResources are scalar resources that do not affect whether a task
// is considered "pure CPU". Pods and ephemeral-storage are always present and
// should be ignored when determining task type.
var ignoredScalarResources = map[v1.ResourceName]struct{}{
	v1.ResourcePods:             {},
	v1.ResourceEphemeralStorage: {},
}

type cpuLimitPlugin struct {
	pluginArguments framework.Arguments
}

// New returns a new cpulimit plugin instance.
func New(arguments framework.Arguments) framework.Plugin {
	return &cpuLimitPlugin{pluginArguments: arguments}
}

func (p *cpuLimitPlugin) Name() string {
	return PluginName
}

// isCPUOnlyTask returns true if the task only requests CPU and Memory
// (no other meaningful scalar resources like GPU, RDMA, etc.).
// A task that requests any extra scalar resource beyond pods/ephemeral-storage
// is NOT a pure-CPU task.
func isCPUOnlyTask(task *api.TaskInfo) bool {
	if task.Resreq == nil {
		return true
	}

	for rName, rQuant := range task.Resreq.ScalarResources {
		if rQuant <= 0 {
			continue
		}
		if _, ignored := ignoredScalarResources[rName]; ignored {
			continue
		}
		// Task requests a non-trivial scalar resource, not a pure-CPU task
		return false
	}
	return true
}

// getNodeCPUOnlyLimits reads the annotation from the node and returns
// (maxCPU in cores, maxMemory in GB, whether any limit is set).
func getNodeCPUOnlyLimits(node *api.NodeInfo) (float64, float64, bool) {
	if node.Node == nil || node.Node.Annotations == nil {
		return 0, 0, false
	}

	annotations := node.Node.Annotations
	cpuStr, cpuSet := annotations[CPUOnlyTasksMaxCPUAnnotation]
	memStr, memSet := annotations[CPUOnlyTasksMaxMemoryAnnotation]

	if !cpuSet && !memSet {
		return 0, 0, false
	}

	var maxCPU, maxMem float64

	if cpuSet {
		v, err := strconv.ParseFloat(cpuStr, 64)
		if err != nil {
			klog.Warningf("cpulimit: failed to parse annotation %s=%q on node %s: %v",
				CPUOnlyTasksMaxCPUAnnotation, cpuStr, node.Name, err)
			return 0, 0, false
		}
		maxCPU = v
	}

	if memSet {
		v, err := strconv.ParseFloat(memStr, 64)
		if err != nil {
			klog.Warningf("cpulimit: failed to parse annotation %s=%q on node %s: %v",
				CPUOnlyTasksMaxMemoryAnnotation, memStr, node.Name, err)
			return 0, 0, false
		}
		maxMem = v
	}

	return maxCPU, maxMem, true
}

// sumCPUOnlyTaskResources sums the resource requests of all existing pure-CPU tasks
// on the node, returning (totalCPU in cores, totalMemory in GB).
func sumCPUOnlyTaskResources(node *api.NodeInfo) (float64, float64) {
	var totalCPU, totalMem float64

	for _, task := range node.Tasks {
		if !isCPUOnlyTask(task) {
			continue
		}
		if task.Resreq != nil {
			totalCPU += task.Resreq.MilliCPU / milliCPUPerCore
			totalMem += task.Resreq.Memory / bytesPerGB
		}
	}

	return totalCPU, totalMem
}

// checkExceeded checks whether adding taskReq to existing would exceed maxLimit.
// Returns a status with Unschedulable code and reason if exceeded, nil otherwise.
func checkExceeded(existing, taskReq, maxLimit float64, resType, unit, nodeName string) *api.Status {
	if maxLimit <= 0 {
		return nil
	}
	if existing+taskReq <= maxLimit {
		return nil
	}
	return &api.Status{
		Plugin: PluginName,
		Code:   api.Unschedulable,
		Reason: fmt.Sprintf(
			"cpu-only tasks on node %s would exceed %s limit: existing=%.2f%s + request=%.2f%s > max=%.2f%s",
			nodeName, resType, existing, unit, taskReq, unit, maxLimit, unit),
	}
}

// getTaskResources extracts CPU (in cores) and Memory (in GB) from task.Resreq safely.
func getTaskResources(task *api.TaskInfo) (float64, float64) {
	if task.Resreq == nil {
		return 0, 0
	}
	return task.Resreq.MilliCPU / milliCPUPerCore, task.Resreq.Memory / bytesPerGB
}

func (p *cpuLimitPlugin) OnSessionOpen(ssn *framework.Session) {
	klog.V(5).Infof("Enter cpulimit plugin ...")
	defer func() {
		klog.V(5).Infof("Leaving cpulimit plugin.")
	}()

	predicateFn := func(task *api.TaskInfo, node *api.NodeInfo) error {
		// Only pure-CPU tasks (requesting only cpu+memory) are constrained
		if !isCPUOnlyTask(task) {
			return nil
		}

		maxCPU, maxMem, hasLimit := getNodeCPUOnlyLimits(node)
		if !hasLimit {
			return nil
		}

		existingCPU, existingMem := sumCPUOnlyTaskResources(node)
		taskCPU, taskMem := getTaskResources(task)

		cpuSt := checkExceeded(existingCPU, taskCPU, maxCPU, "CPU", " cores", node.Name)
		memSt := checkExceeded(existingMem, taskMem, maxMem, "Memory", "GB", node.Name)

		var statuses []*api.Status
		for _, st := range []*api.Status{cpuSt, memSt} {
			if st != nil {
				statuses = append(statuses, st)
				klog.V(4).Infof("cpulimit: rejecting cpu-only task %s/%s on node %s: %s",
					task.Namespace, task.Name, node.Name, st.Reason)
			}
		}
		if len(statuses) > 0 {
			return api.NewFitErrWithStatus(task, node, statuses...)
		}

		klog.V(4).Infof("cpulimit: allowing cpu-only task %s/%s on node %s (cpu: %.2f+%.2f <= %.2f cores, mem: %.2f+%.2f <= %.2fGB)",
			task.Namespace, task.Name, node.Name, existingCPU, taskCPU, maxCPU, existingMem, taskMem, maxMem)
		return nil
	}

	ssn.AddPredicateFn(p.Name(), predicateFn)
}

// OnSessionClose is a no-op for cpulimit plugin as no cleanup is needed.
func (p *cpuLimitPlugin) OnSessionClose(ssn *framework.Session) {
	// no-op: cpulimit plugin does not maintain session-level state that requires cleanup
}
