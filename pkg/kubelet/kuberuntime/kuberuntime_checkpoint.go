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

package kuberuntime

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/klog/v2"

	"k8s.io/kubernetes/pkg/kubelet/checkpoint"
	"k8s.io/kubernetes/pkg/kubelet/types"
)

// CheckpointContainerRequest mirrors the CRI v1 CheckpointContainerRequest.
// We define it here to avoid modifying the K8s 1.23 CRI proto.
type CheckpointContainerRequest struct {
	ContainerId string `protobuf:"bytes,1,opt,name=container_id,json=containerId,proto3" json:"container_id,omitempty"`
	Location    string `protobuf:"bytes,2,opt,name=location,proto3" json:"location,omitempty"`
	Timeout     int64  `protobuf:"varint,3,opt,name=timeout,proto3" json:"timeout,omitempty"`
}

func (m *CheckpointContainerRequest) Reset()         { *m = CheckpointContainerRequest{} }
func (m *CheckpointContainerRequest) String() string  { return fmt.Sprintf("%+v", *m) }
func (m *CheckpointContainerRequest) ProtoMessage()   {}

// CheckpointContainerResponse mirrors the CRI v1 CheckpointContainerResponse.
type CheckpointContainerResponse struct{}

func (m *CheckpointContainerResponse) Reset()         { *m = CheckpointContainerResponse{} }
func (m *CheckpointContainerResponse) String() string { return "CheckpointContainerResponse{}" }
func (m *CheckpointContainerResponse) ProtoMessage()  {}

// CheckpointContainer calls the containerd CRI CheckpointContainer RPC directly
// via the gRPC connection. Since K8s 1.23 CRI proto doesn't include CheckpointContainer,
// we use grpc.Invoke directly with the CRI v1 method path.
func (m *kubeGenericRuntimeManager) CheckpointContainer(containerID string, checkpointPath string) error {
	start := time.Now()
	klog.V(2).InfoS("CheckpointContainer", "containerID", containerID, "path", checkpointPath)

	// Get the gRPC connection from the runtime service
	conn := m.getGRPCConnection()
	if conn == nil {
		return fmt.Errorf("no gRPC connection available for checkpoint")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// containerd requires "containerd-namespace" gRPC header set to "k8s.io"
	ctx = metadata.AppendToOutgoingContext(ctx, "containerd-namespace", "k8s.io")

	req := &CheckpointContainerRequest{
		ContainerId: containerID,
		Location:    checkpointPath,
		Timeout:     120,
	}
	resp := &CheckpointContainerResponse{}

	// Call containerd's CRI v1 CheckpointContainer directly
	err := conn.Invoke(ctx, "/runtime.v1.RuntimeService/CheckpointContainer", req, resp)
	if err != nil {
		return fmt.Errorf("CRI CheckpointContainer failed for %s: %w", containerID, err)
	}

	elapsed := time.Since(start)
	klog.V(2).InfoS("CheckpointContainer succeeded", "containerID", containerID, "elapsed", elapsed)
	return nil
}

// RestoreContainerRequest is the request message for the custom RestoreContainer RPC.
// It matches the containerd-side RestoreContainerRequest with v2 extensions.
type RestoreContainerRequest struct {
	ContainerId      string `protobuf:"bytes,1,opt,name=container_id,json=containerId,proto3" json:"container_id,omitempty"`
	Location         string `protobuf:"bytes,2,opt,name=location,proto3" json:"location,omitempty"`
	Timeout          int64  `protobuf:"varint,3,opt,name=timeout,proto3" json:"timeout,omitempty"`
	// v2: Path to the new sandbox's network namespace (e.g. /proc/<sandbox_pid>/ns/net).
	// Empty means no netns mapping (v1 mode: sandbox preserved).
	SandboxNetnsPath string `protobuf:"bytes,4,opt,name=sandbox_netns_path,json=sandboxNetnsPath,proto3" json:"sandbox_netns_path,omitempty"`
	// v2: Inode number of the old netns recorded during checkpoint.
	// Zero means no netns mapping.
	OldNetnsInode    uint64 `protobuf:"varint,5,opt,name=old_netns_inode,json=oldNetnsInode,proto3" json:"old_netns_inode,omitempty"`
}

func (m *RestoreContainerRequest) Reset()         { *m = RestoreContainerRequest{} }
func (m *RestoreContainerRequest) String() string  { return fmt.Sprintf("%+v", *m) }
func (m *RestoreContainerRequest) ProtoMessage()   {}

// RestoreContainerResponse is the response for RestoreContainer RPC.
type RestoreContainerResponse struct{}

func (m *RestoreContainerResponse) Reset()         { *m = RestoreContainerResponse{} }
func (m *RestoreContainerResponse) String() string { return "RestoreContainerResponse{}" }
func (m *RestoreContainerResponse) ProtoMessage()  {}

// RestoreContainer calls containerd's custom RestoreContainer method.
// Since CRI doesn't have a RestoreContainer RPC, we use the containerd
// internal method by deleting the old task and creating a new one from checkpoint.
//
// v2 extension: sandboxNetnsPath and oldNetnsInode are used for CRIU --external net[...]
// parameter passing when the sandbox was destroyed and rebuilt (v2 complete exit mode).
// When both are empty/zero, this falls back to v1 behavior (sandbox preserved).
func (m *kubeGenericRuntimeManager) RestoreContainer(containerID string, checkpointPath string,
	sandboxNetnsPath string, oldNetnsInode uint64) error {
	start := time.Now()
	klog.V(2).InfoS("RestoreContainer", "containerID", containerID, "path", checkpointPath,
		"sandboxNetnsPath", sandboxNetnsPath, "oldNetnsInode", oldNetnsInode)

	// Get the gRPC connection from the runtime service
	conn := m.getGRPCConnection()
	if conn == nil {
		return fmt.Errorf("no gRPC connection available for restore")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// containerd requires "containerd-namespace" gRPC header set to "k8s.io"
	// v2: Also pass sandboxNetnsPath and oldNetnsInode via gRPC metadata headers
	// (proto struct tags don't reliably serialize across different proto libraries)
	md := metadata.Pairs("containerd-namespace", "k8s.io")
	if sandboxNetnsPath != "" {
		md.Append("x-criu-sandbox-netns-path", sandboxNetnsPath)
	}
	if oldNetnsInode > 0 {
		md.Append("x-criu-old-netns-inode", strconv.FormatUint(oldNetnsInode, 10))
	}
	ctx = metadata.NewOutgoingContext(ctx, md)

	// Use the RestoreContainerRequest (basic fields only, v2 fields via metadata)
	req := &RestoreContainerRequest{
		ContainerId:      containerID,
		Location:         checkpointPath,
		Timeout:          120,
	}
	resp := &RestoreContainerResponse{}

	// Try the custom restore RPC (registered in containerd as a custom gRPC service)
	err := conn.Invoke(ctx, "/runtime.v1.ContainerRestoreService/RestoreContainer", req, resp)
	if err != nil {
		klog.V(2).InfoS("Direct RestoreContainer RPC not available, will use alternative restore method",
			"containerID", containerID, "error", err)
		// Fall through to alternative method
		return fmt.Errorf("restore not available via CRI: %w", err)
	}

	elapsed := time.Since(start)
	klog.V(2).InfoS("RestoreContainer succeeded", "containerID", containerID, "elapsed", elapsed)
	return nil
}

// getGRPCConnection returns the underlying gRPC connection from the runtime service.
// This is set during kubelet initialization.
var grpcConn *grpc.ClientConn

// SetGRPCConnection sets the gRPC connection for checkpoint/restore operations.
func SetGRPCConnection(conn *grpc.ClientConn) {
	grpcConn = conn
}

func (m *kubeGenericRuntimeManager) getGRPCConnection() *grpc.ClientConn {
	return grpcConn
}

// GetCheckpointManager returns the checkpoint manager.
func (m *kubeGenericRuntimeManager) GetCheckpointManager() *checkpoint.Manager {
	return m.checkpointManager
}

// CreatePodSandboxForRestore creates a new sandbox for a Pod being restored from checkpoint.
// It calls CRI RunPodSandbox, then determines the new sandbox's network namespace path
// by finding the sandbox container's PID and reading /proc/<pid>/ns/net.
//
// Returns (sandboxID, sandboxNetnsPath, error).
// For hostNetwork pods, sandboxNetnsPath will be empty (no netns mapping needed).
func (m *kubeGenericRuntimeManager) CreatePodSandboxForRestore(pod *v1.Pod) (string, string, error) {
	start := time.Now()
	klog.V(2).InfoS("CreatePodSandboxForRestore: creating new sandbox for checkpointed Pod",
		"pod", klog.KObj(pod))

	// Determine the next sandbox attempt number by querying existing sandboxes for this Pod.
	// Previous restore cycles may have left NotReady sandboxes with earlier attempt numbers;
	// we must use max(existing_attempt) + 1 to avoid name reservation conflicts.
	nextAttempt := uint32(1) // default: first restore after initial sandbox (attempt=0)
	existingSandboxes, listErr := m.runtimeService.ListPodSandbox(&runtimeapi.PodSandboxFilter{
		LabelSelector: map[string]string{types.KubernetesPodUIDLabel: string(pod.UID)},
	})
	if listErr != nil {
		klog.V(2).InfoS("CreatePodSandboxForRestore: failed to list existing sandboxes, using default attempt=1",
			"pod", klog.KObj(pod), "error", listErr)
	} else {
		var maxAttempt uint32
		for _, sb := range existingSandboxes {
			if sb.Metadata != nil && sb.Metadata.Attempt >= maxAttempt {
				maxAttempt = sb.Metadata.Attempt
			}
		}
		nextAttempt = maxAttempt + 1
		klog.V(2).InfoS("CreatePodSandboxForRestore: determined next attempt from existing sandboxes",
			"pod", klog.KObj(pod), "existingCount", len(existingSandboxes),
			"maxAttempt", maxAttempt, "nextAttempt", nextAttempt)
	}

	sandboxID, msg, err := m.createPodSandbox(pod, nextAttempt)
	if err != nil {
		return "", "", fmt.Errorf("failed to create sandbox for restore: %s: %w", msg, err)
	}

	klog.V(2).InfoS("CreatePodSandboxForRestore: sandbox created",
		"pod", klog.KObj(pod), "sandboxID", sandboxID, "elapsed", time.Since(start))

	// For hostNetwork pods, no netns mapping is needed
	if pod.Spec.HostNetwork {
		klog.V(2).InfoS("CreatePodSandboxForRestore: hostNetwork pod, no netns mapping needed",
			"pod", klog.KObj(pod))
		return sandboxID, "", nil
	}

	// Get the sandbox's netns path by finding its PID
	sandboxNetnsPath, err := getSandboxNetnsPath(sandboxID)
	if err != nil {
		klog.ErrorS(err, "CreatePodSandboxForRestore: failed to get sandbox netns path (non-fatal, CRIU restore will use v1 mode)",
			"pod", klog.KObj(pod), "sandboxID", sandboxID)
		// Non-fatal: return empty netns path, CRIU restore will fall back to v1 mode
		// (which will fail for v2 since the netns changed, but at least the sandbox is created)
		return sandboxID, "", nil
	}

	klog.V(2).InfoS("CreatePodSandboxForRestore: got sandbox netns path",
		"pod", klog.KObj(pod), "sandboxID", sandboxID,
		"sandboxNetnsPath", sandboxNetnsPath, "totalElapsed", time.Since(start))

	return sandboxID, sandboxNetnsPath, nil
}

// getSandboxNetnsPath finds the network namespace path for a sandbox container.
// It scans /proc to find a process whose cgroup contains the sandbox container ID,
// then returns /proc/<pid>/ns/net.
//
// This is a PoC approach — production code would use CRI PodSandboxStatus(verbose=true)
// to get the sandbox PID from the "info" field.
func getSandboxNetnsPath(sandboxID string) (string, error) {
	if sandboxID == "" {
		return "", fmt.Errorf("empty sandbox ID")
	}

	entries, err := os.ReadDir("/proc")
	if err != nil {
		return "", fmt.Errorf("failed to read /proc: %w", err)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid <= 1 {
			continue
		}

		// Read cgroup file to check if this process belongs to the sandbox container
		cgroupPath := fmt.Sprintf("/proc/%d/cgroup", pid)
		data, err := os.ReadFile(cgroupPath)
		if err != nil {
			continue
		}
		if strings.Contains(string(data), sandboxID) {
			// Found a process in this sandbox — construct netns path
			netnsPath := fmt.Sprintf("/proc/%d/ns/net", pid)
			// Verify the path exists
			if _, err := os.Lstat(netnsPath); err != nil {
				continue
			}
			klog.V(3).InfoS("getSandboxNetnsPath: found sandbox process",
				"sandboxID", sandboxID, "pid", pid, "netnsPath", netnsPath)
			return netnsPath, nil
		}
	}

	return "", fmt.Errorf("no process found for sandbox %s", sandboxID)
}
