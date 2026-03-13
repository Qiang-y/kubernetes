/*
Copyright 2022 The Kubernetes Authors.

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

package kubelet

import (
	"fmt"
	"path/filepath"
	"time"

	"k8s.io/klog/v2"

	"k8s.io/kubernetes/pkg/kubelet/checkpoint"
	"k8s.io/kubernetes/pkg/kubelet/server"
)

// CheckpointPod checkpoints all business containers in a Pod using CRIU.
// This implements server.CheckpointRestoreInterface.
func (kl *Kubelet) CheckpointPod(podNamespace, podName string) (*server.CheckpointRestoreResponse, error) {
	start := time.Now()
	klog.V(2).InfoS("CheckpointPod starting", "namespace", podNamespace, "pod", podName)

	// Find the pod
	pod, ok := kl.GetPodByName(podNamespace, podName)
	if !ok {
		return nil, fmt.Errorf("pod %s/%s not found", podNamespace, podName)
	}

	// Get pod status from runtime
	podStatus, err := kl.containerRuntime.GetPodStatus(pod.UID, pod.Name, pod.Namespace)
	if err != nil {
		return nil, fmt.Errorf("failed to get pod status: %w", err)
	}

	// Get kuberuntime manager
	runtimeManager := kl.containerRuntime
	if runtimeManager == nil {
		return nil, fmt.Errorf("container runtime not initialized")
	}

	response := &server.CheckpointRestoreResponse{
		PodName:   podName,
		Namespace: podNamespace,
	}

	// Get the checkpoint manager from the runtime manager
	krm, ok := runtimeManager.(interface {
		CheckpointContainer(containerID string, checkpointPath string) error
		GetCheckpointManager() *checkpoint.Manager
	})
	if !ok {
		return nil, fmt.Errorf("container runtime does not support checkpoint")
	}

	checkpointMgr := krm.GetCheckpointManager()
	if checkpointMgr == nil {
		return nil, fmt.Errorf("checkpoint manager not initialized")
	}

	// Iterate over business containers (skip init containers)
	for _, container := range pod.Spec.Containers {
		containerStart := time.Now()

		result := server.ContainerCRResult{
			Name: container.Name,
		}

		// Find container status
		var containerID string
		for _, cs := range podStatus.ContainerStatuses {
			if cs.Name == container.Name {
				containerID = cs.ID.ID
				result.ContainerID = containerID
				break
			}
		}

		if containerID == "" {
			result.Error = "container not found in pod status"
			response.Containers = append(response.Containers, result)
			continue
		}

		// Determine checkpoint path
		checkpointPath := filepath.Join(kl.getPodsDir(), string(pod.UID), "checkpoint", container.Name)
		result.CheckpointPath = checkpointPath

		// Call CRI CheckpointContainer
		klog.V(2).InfoS("Checkpointing container", "pod", podName, "container", container.Name,
			"containerID", containerID, "path", checkpointPath)

		if err := krm.CheckpointContainer(containerID, checkpointPath); err != nil {
			result.Error = err.Error()
			result.Duration = time.Since(containerStart).String()
			response.Containers = append(response.Containers, result)
			return response, fmt.Errorf("checkpoint failed for container %s: %w", container.Name, err)
		}

		// Mark container as checkpointed
		if err := checkpointMgr.MarkCheckpointed(pod.UID, podName, podNamespace, container.Name,
			checkpoint.ContainerCheckpointInfo{
				ContainerID:    containerID,
				CheckpointPath: checkpointPath,
				CheckpointTime: time.Now(),
			}); err != nil {
			klog.ErrorS(err, "Failed to mark container as checkpointed", "container", container.Name)
		}

		result.Duration = time.Since(containerStart).String()
		response.Containers = append(response.Containers, result)
		klog.V(2).InfoS("Container checkpointed successfully",
			"pod", podName, "container", container.Name, "elapsed", result.Duration)
	}

	response.TotalTime = time.Since(start).String()
	klog.V(2).InfoS("CheckpointPod completed", "namespace", podNamespace, "pod", podName,
		"elapsed", response.TotalTime, "containers", len(response.Containers))
	return response, nil
}

// RestorePod restores all checkpointed containers in a Pod from their CRIU checkpoints.
// This implements server.CheckpointRestoreInterface.
func (kl *Kubelet) RestorePod(podNamespace, podName string) (*server.CheckpointRestoreResponse, error) {
	start := time.Now()
	klog.V(2).InfoS("RestorePod starting", "namespace", podNamespace, "pod", podName)

	// Find the pod
	pod, ok := kl.GetPodByName(podNamespace, podName)
	if !ok {
		return nil, fmt.Errorf("pod %s/%s not found", podNamespace, podName)
	}

	// Get kuberuntime manager
	runtimeManager := kl.containerRuntime
	if runtimeManager == nil {
		return nil, fmt.Errorf("container runtime not initialized")
	}

	response := &server.CheckpointRestoreResponse{
		PodName:   podName,
		Namespace: podNamespace,
	}

	krm, ok := runtimeManager.(interface {
		RestoreContainer(containerID string, checkpointPath string) error
		GetCheckpointManager() *checkpoint.Manager
	})
	if !ok {
		return nil, fmt.Errorf("container runtime does not support restore")
	}

	checkpointMgr := krm.GetCheckpointManager()
	if checkpointMgr == nil {
		return nil, fmt.Errorf("checkpoint manager not initialized")
	}

	// Get checkpoint state
	checkpointState := checkpointMgr.GetPodCheckpointState(pod.UID)
	if checkpointState == nil {
		return nil, fmt.Errorf("pod %s/%s is not checkpointed", podNamespace, podName)
	}

	// Restore each checkpointed container
	for containerName, info := range checkpointState.Containers {
		containerStart := time.Now()

		result := server.ContainerCRResult{
			Name:           containerName,
			ContainerID:    info.ContainerID,
			CheckpointPath: info.CheckpointPath,
		}

		klog.V(2).InfoS("Restoring container", "pod", podName, "container", containerName,
			"containerID", info.ContainerID, "checkpointPath", info.CheckpointPath)

		if err := krm.RestoreContainer(info.ContainerID, info.CheckpointPath); err != nil {
			result.Error = err.Error()
			result.Duration = time.Since(containerStart).String()
			response.Containers = append(response.Containers, result)
			return response, fmt.Errorf("restore failed for container %s: %w", containerName, err)
		}

		result.Duration = time.Since(containerStart).String()
		response.Containers = append(response.Containers, result)
		klog.V(2).InfoS("Container restored successfully",
			"pod", podName, "container", containerName, "elapsed", result.Duration)
	}

	// Clear checkpoint state after successful restore
	if err := checkpointMgr.ClearCheckpointed(pod.UID); err != nil {
		klog.ErrorS(err, "Failed to clear checkpoint state", "pod", podName)
	}

	response.TotalTime = time.Since(start).String()
	klog.V(2).InfoS("RestorePod completed", "namespace", podNamespace, "pod", podName,
		"elapsed", response.TotalTime, "containers", len(response.Containers))
	return response, nil
}
