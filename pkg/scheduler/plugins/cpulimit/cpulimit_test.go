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
	"testing"

	v1 "k8s.io/api/core/v1"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/conf"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/uthelper"
	"volcano.sh/volcano/pkg/scheduler/util"

	schedulingv1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
)

func TestIsCPUOnlyTask(t *testing.T) {
	tests := []struct {
		name     string
		task     *api.TaskInfo
		expected bool
	}{
		{
			name: "cpu and memory only - pure CPU task",
			task: &api.TaskInfo{
				Resreq: api.NewResource(api.BuildResourceList("2", "4Gi")),
			},
			expected: true,
		},
		{
			name: "requests nvidia gpu - not pure CPU",
			task: &api.TaskInfo{
				Resreq: api.NewResource(api.BuildResourceListWithGPU("2", "4Gi", "1")),
			},
			expected: false,
		},
		{
			name: "requests volcano gpu-memory - not pure CPU",
			task: &api.TaskInfo{
				Resreq: api.NewResource(api.BuildResourceList("2", "4Gi",
					api.ScalarResource{Name: "volcano.sh/gpu-memory", Value: "1024"})),
			},
			expected: false,
		},
		{
			name: "requests RDMA - not pure CPU",
			task: &api.TaskInfo{
				Resreq: api.NewResource(api.BuildResourceList("2", "4Gi",
					api.ScalarResource{Name: "rdma/hca", Value: "1"})),
			},
			expected: false,
		},
		{
			name: "nil resreq - treated as pure CPU",
			task: &api.TaskInfo{
				Resreq: nil,
			},
			expected: true,
		},
		{
			name: "empty resource - pure CPU",
			task: &api.TaskInfo{
				Resreq: api.EmptyResource(),
			},
			expected: true,
		},
		{
			name: "requests amd gpu - not pure CPU",
			task: &api.TaskInfo{
				Resreq: api.NewResource(api.BuildResourceList("2", "4Gi",
					api.ScalarResource{Name: "amd.com/gpu", Value: "1"})),
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isCPUOnlyTask(tt.task)
			if got != tt.expected {
				t.Errorf("isCPUOnlyTask() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestGetNodeCPUOnlyLimits(t *testing.T) {
	tests := []struct {
		name        string
		annotations map[string]string
		wantCPU     float64
		wantMem     float64
		wantHas     bool
	}{
		{
			name:        "no annotations",
			annotations: map[string]string{},
			wantCPU:     0,
			wantMem:     0,
			wantHas:     false,
		},
		{
			name: "cpu limit only",
			annotations: map[string]string{
				CPUOnlyTasksMaxCPUAnnotation: "8",
			},
			wantCPU: 8,
			wantMem: 0,
			wantHas: true,
		},
		{
			name: "memory limit only",
			annotations: map[string]string{
				CPUOnlyTasksMaxMemoryAnnotation: "16",
			},
			wantCPU: 0,
			wantMem: 16,
			wantHas: true,
		},
		{
			name: "both cpu and memory limits",
			annotations: map[string]string{
				CPUOnlyTasksMaxCPUAnnotation:    "8",
				CPUOnlyTasksMaxMemoryAnnotation: "16",
			},
			wantCPU: 8,
			wantMem: 16,
			wantHas: true,
		},
		{
			name: "invalid cpu value",
			annotations: map[string]string{
				CPUOnlyTasksMaxCPUAnnotation: "invalid",
			},
			wantCPU: 0,
			wantMem: 0,
			wantHas: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := util.BuildNode("test-node", api.BuildResourceList("16", "64Gi"), nil)
			node.Annotations = tt.annotations
			ni := api.NewNodeInfo(node)

			gotCPU, gotMem, gotHas := getNodeCPUOnlyLimits(ni)
			if gotCPU != tt.wantCPU || gotMem != tt.wantMem || gotHas != tt.wantHas {
				t.Errorf("getNodeCPUOnlyLimits() = (%v, %v, %v), want (%v, %v, %v)",
					gotCPU, gotMem, gotHas, tt.wantCPU, tt.wantMem, tt.wantHas)
			}
		})
	}
}

func TestCPULimitPlugin(t *testing.T) {
	plugins := map[string]framework.PluginBuilder{PluginName: New}

	// CPU-only pods
	cpuPod1 := util.BuildPod("ns1", "cpu-pod1", "", v1.PodPending,
		api.BuildResourceList("2000m", "4Gi"), "pg1", nil, nil)
	cpuPod2 := util.BuildPod("ns1", "cpu-pod2", "", v1.PodPending,
		api.BuildResourceList("2000m", "4Gi"), "pg1", nil, nil)
	cpuPodSmall := util.BuildPod("ns1", "cpu-pod-small", "", v1.PodPending,
		api.BuildResourceList("1000m", "2Gi"), "pg1", nil, nil)
	cpuPodBig := util.BuildPod("ns1", "cpu-pod-big", "", v1.PodPending,
		api.BuildResourceList("6000m", "8Gi"), "pg1", nil, nil)

	// GPU pod
	gpuPod1 := util.BuildPod("ns1", "gpu-pod1", "", v1.PodPending,
		api.BuildResourceListWithGPU("4000m", "8Gi", "2"), "pg1", nil, nil)

	// Existing running CPU-only pod on a node
	existingCPUPod := util.BuildPod("ns1", "existing-cpu", "n1", v1.PodRunning,
		api.BuildResourceList("3000m", "4Gi"), "pg1", nil, nil)

	// Existing running GPU pod on a node
	existingGPUPod := util.BuildPod("ns1", "existing-gpu", "n1", v1.PodRunning,
		api.BuildResourceListWithGPU("4000m", "8Gi", "1"), "pg1", nil, nil)

	// Node with CPU-only limits (max 4 cores CPU, 8GB memory for CPU-only tasks)
	n1 := util.BuildNode("n1", api.BuildResourceList("32", "128Gi",
		api.ScalarResource{Name: "nvidia.com/gpu", Value: "4"}), nil)
	n1.Annotations[CPUOnlyTasksMaxCPUAnnotation] = "4"
	n1.Annotations[CPUOnlyTasksMaxMemoryAnnotation] = "8"

	// Node without annotations (no limit)
	n2 := util.BuildNode("n2", api.BuildResourceList("32", "128Gi"), nil)

	// Node with only CPU limit
	n3 := util.BuildNode("n3", api.BuildResourceList("32", "128Gi",
		api.ScalarResource{Name: "nvidia.com/gpu", Value: "4"}), nil)
	n3.Annotations[CPUOnlyTasksMaxCPUAnnotation] = "4"

	pg1 := util.BuildPodGroup("pg1", "ns1", "q1", 0, nil, "")
	queue1 := util.BuildQueue("q1", 1, nil)

	tests := []struct {
		name           string
		testStruct     uthelper.TestCommonStruct
		expectedStatus map[string]map[string]int // task -> node -> status code
	}{
		{
			name: "cpu-only task within limit should pass",
			testStruct: uthelper.TestCommonStruct{
				Name:      "cpu-only task within limit",
				PodGroups: []*schedulingv1.PodGroup{pg1},
				Queues:    []*schedulingv1.Queue{queue1},
				Pods:      []*v1.Pod{cpuPod1},
				Nodes:     []*v1.Node{n1, n2},
				Plugins:   plugins,
			},
			expectedStatus: map[string]map[string]int{
				"ns1/cpu-pod1": {
					"n1": api.Success,
					"n2": api.Success,
				},
			},
		},
		{
			name: "cpu-only task exceeding CPU limit should be rejected",
			testStruct: uthelper.TestCommonStruct{
				Name:      "cpu-only task exceeding CPU limit",
				PodGroups: []*schedulingv1.PodGroup{pg1},
				Queues:    []*schedulingv1.Queue{queue1},
				Pods:      []*v1.Pod{cpuPodBig},
				Nodes:     []*v1.Node{n1, n2},
				Plugins:   plugins,
			},
			expectedStatus: map[string]map[string]int{
				"ns1/cpu-pod-big": {
					"n1": api.Unschedulable, // 6 cores > 4 cores limit
					"n2": api.Success,       // no limit on n2
				},
			},
		},
		{
			name: "non-cpu-only task (with GPU) should not be limited",
			testStruct: uthelper.TestCommonStruct{
				Name:      "gpu task ignores limits",
				PodGroups: []*schedulingv1.PodGroup{pg1},
				Queues:    []*schedulingv1.Queue{queue1},
				Pods:      []*v1.Pod{gpuPod1},
				Nodes:     []*v1.Node{n1, n2},
				Plugins:   plugins,
			},
			expectedStatus: map[string]map[string]int{
				"ns1/gpu-pod1": {
					"n1": api.Success,
					"n2": api.Success,
				},
			},
		},
		{
			name: "cpu-only task rejected when existing cpu tasks fill the limit",
			testStruct: uthelper.TestCommonStruct{
				Name:      "existing cpu tasks fill limit",
				PodGroups: []*schedulingv1.PodGroup{pg1},
				Queues:    []*schedulingv1.Queue{queue1},
				Pods:      []*v1.Pod{existingCPUPod, cpuPod2},
				Nodes:     []*v1.Node{n1},
				Plugins:   plugins,
			},
			expectedStatus: map[string]map[string]int{
				"ns1/cpu-pod2": {
					"n1": api.Unschedulable, // existing 3 cores + 2 cores = 5 cores > 4 cores
				},
			},
		},
		{
			name: "cpu-only task allowed even with existing non-cpu-only tasks",
			testStruct: uthelper.TestCommonStruct{
				Name:      "non-cpu-only tasks don't count toward limit",
				PodGroups: []*schedulingv1.PodGroup{pg1},
				Queues:    []*schedulingv1.Queue{queue1},
				Pods:      []*v1.Pod{existingGPUPod, cpuPodSmall},
				Nodes:     []*v1.Node{n1},
				Plugins:   plugins,
			},
			expectedStatus: map[string]map[string]int{
				"ns1/cpu-pod-small": {
					"n1": api.Success, // only 1 core CPU-only, GPU pods not counted
				},
			},
		},
		{
			name: "node without annotation allows all tasks",
			testStruct: uthelper.TestCommonStruct{
				Name:      "no annotation means no limit",
				PodGroups: []*schedulingv1.PodGroup{pg1},
				Queues:    []*schedulingv1.Queue{queue1},
				Pods:      []*v1.Pod{cpuPodBig},
				Nodes:     []*v1.Node{n2},
				Plugins:   plugins,
			},
			expectedStatus: map[string]map[string]int{
				"ns1/cpu-pod-big": {
					"n2": api.Success,
				},
			},
		},
		{
			name: "cpu limit only - memory not checked",
			testStruct: uthelper.TestCommonStruct{
				Name:      "only CPU limit set",
				PodGroups: []*schedulingv1.PodGroup{pg1},
				Queues:    []*schedulingv1.Queue{queue1},
				Pods:      []*v1.Pod{cpuPod1},
				Nodes:     []*v1.Node{n3},
				Plugins:   plugins,
			},
			expectedStatus: map[string]map[string]int{
				"ns1/cpu-pod1": {
					"n3": api.Success, // 2 cores < 4 cores
				},
			},
		},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			trueValue := true
			tiers := []conf.Tier{
				{
					Plugins: []conf.PluginOption{
						{
							Name:             PluginName,
							EnabledPredicate: &trueValue,
						},
					},
				},
			}

			ssn := tt.testStruct.RegisterSession(tiers, nil)
			defer tt.testStruct.Close()

			for _, job := range ssn.Jobs {
				for _, task := range job.Tasks {
					taskID := fmt.Sprintf("%s/%s", task.Namespace, task.Name)

					expectedStatuses, ok := tt.expectedStatus[taskID]
					if !ok {
						// This task (e.g., existing running pod) is not being tested
						continue
					}

					for _, node := range ssn.Nodes {
						expectedCode, ok := expectedStatuses[node.Name]
						if !ok {
							continue
						}

						err := ssn.PredicateFn(task, node)
						var code int
						if err == nil {
							code = api.Success
						} else {
							fitErr, ok := err.(*api.FitError)
							if ok && len(fitErr.Status) > 0 {
								code = fitErr.Status[0].Code
							} else {
								code = api.Error
							}
						}

						if expectedCode != code {
							t.Errorf("case%d: task %s on node %s expect status code %v, got %v (err=%v)",
								i, taskID, node.Name, expectedCode, code, err)
						}
					}
				}
			}
		})
	}
}
