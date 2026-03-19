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
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"

	"k8s.io/kubernetes/pkg/kubelet/checkpoint"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/pkg/kubelet/server"
)

// Ensure v1 is used by CRIUPodRestorer interface.
var _ *v1.Pod

// CRIUPodRestorer is the interface for CRIU Pod-level checkpoint/restore operations.
// This is implemented by kubeGenericRuntimeManager and accessed via type assertion
// from kubelet's CheckpointPod/RestorePod methods.
type CRIUPodRestorer interface {
	// CheckpointContainer performs a CRIU checkpoint of a single container.
	CheckpointContainer(containerID string, checkpointPath string) error
	// RestoreContainer performs a CRIU restore of a single container.
	// sandboxNetnsPath and oldNetnsInode are v2 extensions for netns remapping.
	RestoreContainer(containerID string, checkpointPath string, sandboxNetnsPath string, oldNetnsInode uint64) error
	// CreatePodSandboxForRestore creates a new sandbox for a checkpointed Pod.
	// Returns (sandboxID, sandboxNetnsPath, error).
	CreatePodSandboxForRestore(pod *v1.Pod) (string, string, error)
	// GetCheckpointManager returns the checkpoint manager.
	GetCheckpointManager() *checkpoint.Manager
}

// CheckpointPod checkpoints all business containers in a Pod using CRIU,
// then destroys all containers and the sandbox (v2: complete Pod exit).
// This implements server.CheckpointRestoreInterface.
func (kl *Kubelet) CheckpointPod(podNamespace, podName string) (*server.CheckpointRestoreResponse, error) {
	start := time.Now()
	klog.V(2).InfoS("CheckpointPod starting (v2: complete exit)", "namespace", podNamespace, "pod", podName)

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

	// Get the CRIU restore manager from the runtime manager
	krm, ok := runtimeManager.(CRIUPodRestorer)
	if !ok {
		return nil, fmt.Errorf("container runtime does not support checkpoint")
	}

	checkpointMgr := krm.GetCheckpointManager()
	if checkpointMgr == nil {
		return nil, fmt.Errorf("checkpoint manager not initialized")
	}

	// ========== Phase 1: CRIU dump all business containers ==========

	// v2: Record sandbox netns inode BEFORE dump (we need it for CRIU restore later).
	// Business containers share the sandbox's netns, so we can read the inode from
	// any running container's /proc/<pid>/ns/net. We use the first business container's PID.
	sandboxNetnsInode := uint64(0)
	for _, cs := range podStatus.ContainerStatuses {
		if cs.State == kubecontainer.ContainerStateRunning {
			// Container PID shares sandbox netns — read inode from /proc/PID/ns/net
			if inode, err := readNetnsInodeFromContainerID(cs.ID.ID); err == nil {
				sandboxNetnsInode = inode
				klog.V(2).InfoS("Recorded netns inode from container for CRIU restore",
					"pod", podName, "container", cs.Name, "containerID", cs.ID.ID, "netnsInode", inode)
			} else {
				klog.Warningf("Failed to read netns inode from container %s (%s): %v",
					cs.Name, cs.ID.ID, err)
			}
			break
		}
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
			// v2: If any container dump fails, do NOT proceed to KillPod.
			// Pod stays in v1-compatible state (some containers exited, sandbox still alive).
			return response, fmt.Errorf("checkpoint failed for container %s: %w", container.Name, err)
		}

		// Mark container as checkpointed (v2: with sandbox netns inode)
		if err := checkpointMgr.MarkCheckpointed(pod.UID, podName, podNamespace, container.Name,
			checkpoint.ContainerCheckpointInfo{
				ContainerID:    containerID,
				CheckpointPath: checkpointPath,
				CheckpointTime: time.Now(),
			}, sandboxNetnsInode); err != nil {
			klog.ErrorS(err, "Failed to mark container as checkpointed", "container", container.Name)
		}

		result.Duration = time.Since(containerStart).String()
		response.Containers = append(response.Containers, result)
		klog.V(2).InfoS("Container checkpointed successfully",
			"pod", podName, "container", container.Name, "elapsed", result.Duration)
	}

	// ========== Phase 2: v2 — Destroy all containers and sandbox ==========
	// Re-fetch pod status to get the current state of all containers and sandbox
	// (containers may have been stopped by CRIU dump, but sandbox should still be running).
	// We need to pass a proper runningPod to KillPod so it knows which sandbox to stop.
	currentPodStatus, statusErr := kl.containerRuntime.GetPodStatus(pod.UID, pod.Name, pod.Namespace)
	if statusErr != nil {
		klog.ErrorS(statusErr, "v2: Failed to get pod status for KillPod, using empty pod (sandbox may not be cleaned)",
			"pod", podName)
		currentPodStatus = podStatus // fallback to pre-dump status
	}
	runningPod := kubecontainer.ConvertPodStatusToRunningPod(kl.getRuntime().Type(), currentPodStatus)

	klog.V(2).InfoS("v2: All containers dumped, destroying Pod (KillPod)",
		"pod", podName, "sandboxNetnsInode", sandboxNetnsInode,
		"containers", len(runningPod.Containers), "sandboxes", len(runningPod.Sandboxes))

	killStart := time.Now()
	if err := kl.containerRuntime.KillPod(pod, runningPod, nil); err != nil {
		// v2: KillPod failure is logged but NOT fatal.
		// Pod is in a v1-compatible state: business containers already exited by CRIU dump,
		// sandbox may still be running, checkpointed markers are set.
		// computePodActions will still prevent restart.
		klog.ErrorS(err, "v2: KillPod failed after checkpoint (non-fatal, Pod in v1-compat state)",
			"pod", podName, "elapsed", time.Since(killStart))
	} else {
		klog.V(2).InfoS("v2: KillPod succeeded, Pod fully exited",
			"pod", podName, "elapsed", time.Since(killStart))
	}

	response.TotalTime = time.Since(start).String()
	klog.V(2).InfoS("CheckpointPod completed (v2: complete exit)", "namespace", podNamespace, "pod", podName,
		"elapsed", response.TotalTime, "containers", len(response.Containers),
		"sandboxNetnsInode", sandboxNetnsInode)
	return response, nil
}

// RestorePod restores a checkpointed Pod.
// v2 Phase C (CRIU restore to new netns):
//   1. Get checkpoint state (with old netns inode)
//   2. Create a new sandbox via CRI RunPodSandbox → get new netns path
//   3. For each checkpointed business container: CRIU restore with --external net[old]:netns[new]
//   4. Clear the checkpointed marker so kubelet syncPod resumes normal management
//
// This implements server.CheckpointRestoreInterface.
func (kl *Kubelet) RestorePod(podNamespace, podName string) (*server.CheckpointRestoreResponse, error) {
	start := time.Now()
	klog.V(2).InfoS("RestorePod starting (v2 Phase C: CRIU restore to new netns)", "namespace", podNamespace, "pod", podName)

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

	krm, ok := runtimeManager.(CRIUPodRestorer)
	if !ok {
		return nil, fmt.Errorf("container runtime does not support checkpoint")
	}

	checkpointMgr := krm.GetCheckpointManager()
	if checkpointMgr == nil {
		return nil, fmt.Errorf("checkpoint manager not initialized")
	}

	// Get checkpoint state (contains old netns inode and container checkpoint info)
	checkpointState := checkpointMgr.GetPodCheckpointState(pod.UID)
	if checkpointState == nil {
		return nil, fmt.Errorf("pod %s/%s is not checkpointed", podNamespace, podName)
	}

	oldNetnsInode := checkpointState.SandboxNetnsInode
	klog.V(2).InfoS("RestorePod: checkpoint state retrieved",
		"pod", podName, "oldNetnsInode", oldNetnsInode,
		"containers", len(checkpointState.Containers))

	// ========== Step 1: Create new sandbox (while checkpointed marker is still set) ==========
	// We keep the checkpointed marker set during sandbox creation and CRIU restore.
	// This prevents computePodActions from interfering (it returns empty actions for
	// checkpointed Pods). After CRIU restore completes, we clear the marker so
	// syncPod sees a fully running Pod and resumes normal management.
	sandboxStart := time.Now()
	sandboxID, sandboxNetnsPath, err := krm.CreatePodSandboxForRestore(pod)
	if err != nil {
		return response, fmt.Errorf("failed to create sandbox for restore: %w", err)
	}
	klog.V(2).InfoS("RestorePod: new sandbox created",
		"pod", podName, "sandboxID", sandboxID,
		"sandboxNetnsPath", sandboxNetnsPath,
		"elapsed", time.Since(sandboxStart))

	// ========== Step 2: CRIU restore each business container ==========
	for containerName, info := range checkpointState.Containers {
		containerStart := time.Now()

		result := server.ContainerCRResult{
			Name:           containerName,
			ContainerID:    info.ContainerID,
			CheckpointPath: info.CheckpointPath,
		}

		klog.V(2).InfoS("RestorePod: restoring container via CRIU",
			"pod", podName, "container", containerName,
			"containerID", info.ContainerID,
			"checkpointPath", info.CheckpointPath,
			"sandboxNetnsPath", sandboxNetnsPath,
			"oldNetnsInode", oldNetnsInode)

		if err := krm.RestoreContainer(info.ContainerID, info.CheckpointPath,
			sandboxNetnsPath, oldNetnsInode); err != nil {
			result.Error = err.Error()
			result.Duration = time.Since(containerStart).String()
			response.Containers = append(response.Containers, result)
			return response, fmt.Errorf("CRIU restore failed for container %s: %w", containerName, err)
		}

		result.Duration = time.Since(containerStart).String()
		response.Containers = append(response.Containers, result)
		klog.V(2).InfoS("RestorePod: container CRIU restore succeeded",
			"pod", podName, "container", containerName, "elapsed", result.Duration)
	}

	// ========== Step 3: Clear checkpointed marker ==========
	// All containers have been CRIU restored. Now clear the marker so syncPod
	// sees a fully running Pod (sandbox + containers all active) and resumes
	// normal lifecycle management.
	if err := checkpointMgr.ClearCheckpointed(pod.UID); err != nil {
		klog.ErrorS(err, "Failed to clear checkpoint state after CRIU restore (non-fatal, Pod is running)",
			"pod", podName)
		// Non-fatal: the Pod is already restored and running.
		// The leftover marker file will be cleaned up on next restore or kubelet restart.
	} else {
		klog.V(2).InfoS("RestorePod: checkpointed marker cleared", "pod", podName)
	}

	response.TotalTime = time.Since(start).String()
	klog.V(2).InfoS("RestorePod completed (v2 Phase C: CRIU restore to new netns)",
		"namespace", podNamespace, "pod", podName,
		"sandboxID", sandboxID, "sandboxNetnsPath", sandboxNetnsPath,
		"elapsed", response.TotalTime, "containers", len(response.Containers))
	return response, nil
}

// readNetnsInodeFromContainerID reads the network namespace inode from a container.
// It uses /proc/*/cgroup to find the container's init PID, then reads its netns inode.
// For simplicity in PoC, we scan /proc to find a process belonging to this container ID.
func readNetnsInodeFromContainerID(containerID string) (uint64, error) {
	if containerID == "" {
		return 0, fmt.Errorf("empty container ID")
	}

	// Strategy: scan /proc for processes whose cgroup contains the container ID.
	// This is a PoC approach — production code would use CRI's ContainerStatus(verbose=true).
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return 0, fmt.Errorf("failed to read /proc: %w", err)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		pid, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue // not a PID directory
		}
		if pid <= 1 {
			continue
		}

		// Read cgroup file to check if this process belongs to the container
		cgroupPath := fmt.Sprintf("/proc/%d/cgroup", pid)
		data, err := os.ReadFile(cgroupPath)
		if err != nil {
			continue
		}
		if strings.Contains(string(data), containerID) {
			// Found a process in this container — read its netns inode
			return readNetnsInode(pid)
		}
	}

	return 0, fmt.Errorf("no process found for container %s", containerID)
}

// readNetnsInode reads the network namespace inode number for a given PID.
// It reads /proc/<pid>/ns/net symlink (e.g., "net:[4026532XXX]") and extracts the inode.
func readNetnsInode(pid int) (uint64, error) {
	if pid <= 0 {
		return 0, fmt.Errorf("invalid PID: %d", pid)
	}
	link, err := os.Readlink(fmt.Sprintf("/proc/%d/ns/net", pid))
	if err != nil {
		return 0, fmt.Errorf("readlink /proc/%d/ns/net: %w", pid, err)
	}
	// link format: "net:[4026532XXX]"
	// Extract the inode number between '[' and ']'
	start := strings.Index(link, "[")
	end := strings.Index(link, "]")
	if start < 0 || end < 0 || end <= start+1 {
		return 0, fmt.Errorf("unexpected netns link format: %s", link)
	}
	inode, err := strconv.ParseUint(link[start+1:end], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse inode from %s: %w", link, err)
	}
	return inode, nil
}
