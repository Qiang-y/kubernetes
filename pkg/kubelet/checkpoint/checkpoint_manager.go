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

// Package checkpoint manages Pod-level checkpoint state for CRIU checkpoint/restore.
// It tracks which Pods have been checkpointed and prevents kubelet from
// automatically restarting their containers.
package checkpoint

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
)

// ContainerCheckpointInfo stores checkpoint information for a single container.
type ContainerCheckpointInfo struct {
	ContainerID    string    `json:"containerID"`
	CheckpointPath string    `json:"checkpointPath"`
	CheckpointTime time.Time `json:"checkpointTime"`
}

// CheckpointState stores the checkpoint state for a Pod.
type CheckpointState struct {
	PodUID            types.UID                          `json:"podUID"`
	PodName           string                             `json:"podName"`
	Namespace         string                             `json:"namespace"`
	Timestamp         time.Time                          `json:"timestamp"`
	Containers        map[string]ContainerCheckpointInfo `json:"containers"`        // container name → info
	SandboxNetnsInode uint64                             `json:"sandboxNetnsInode"` // v2: netns inode at dump time (0 = unknown/v1 compat)
}

// Manager manages Pod checkpoint state. It tracks which Pods have been
// checkpointed and provides methods to mark/clear/query checkpoint status.
// State is persisted to disk for kubelet restart resilience.
type Manager struct {
	mu       sync.RWMutex
	stateDir string
	states   map[types.UID]*CheckpointState
}

// NewManager creates a new checkpoint Manager. It loads any persisted state
// from the given directory.
func NewManager(stateDir string) (*Manager, error) {
	m := &Manager{
		stateDir: stateDir,
		states:   make(map[types.UID]*CheckpointState),
	}

	// Create state directory if it doesn't exist
	if err := os.MkdirAll(stateDir, 0700); err != nil {
		return nil, fmt.Errorf("failed to create checkpoint state dir %s: %w", stateDir, err)
	}

	// Load persisted state
	if err := m.loadAll(); err != nil {
		klog.Warningf("Failed to load some checkpoint states: %v", err)
	}

	klog.Infof("Checkpoint manager initialized with %d checkpointed pods", len(m.states))
	return m, nil
}

// MarkCheckpointed marks a container within a Pod as checkpointed.
// sandboxNetnsInode is the netns inode recorded at dump time (v2: for CRIU --external net[...] on restore).
// Pass 0 for v1 compatibility (sandbox preserved, no netns remapping needed).
func (m *Manager) MarkCheckpointed(podUID types.UID, podName, namespace, containerName string, info ContainerCheckpointInfo, sandboxNetnsInode uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	state, ok := m.states[podUID]
	if !ok {
		state = &CheckpointState{
			PodUID:            podUID,
			PodName:           podName,
			Namespace:         namespace,
			Timestamp:         time.Now(),
			Containers:        make(map[string]ContainerCheckpointInfo),
			SandboxNetnsInode: sandboxNetnsInode,
		}
		m.states[podUID] = state
	}

	// Update sandbox netns inode if provided (last call wins, but they should all be the same for a Pod)
	if sandboxNetnsInode != 0 {
		state.SandboxNetnsInode = sandboxNetnsInode
	}

	state.Containers[containerName] = info
	klog.Infof("Marked container %s/%s/%s as checkpointed (path=%s, sandboxNetnsInode=%d)",
		namespace, podName, containerName, info.CheckpointPath, state.SandboxNetnsInode)

	return m.persist(podUID)
}

// ClearCheckpointed clears the checkpoint state for a Pod.
func (m *Manager) ClearCheckpointed(podUID types.UID) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	state, ok := m.states[podUID]
	if !ok {
		return nil // Already clear
	}

	klog.Infof("Clearing checkpoint state for pod %s/%s", state.Namespace, state.PodName)
	delete(m.states, podUID)

	// Remove persisted state file
	return os.Remove(m.stateFilePath(podUID))
}

// IsCheckpointed checks if a specific container in a Pod is checkpointed.
func (m *Manager) IsCheckpointed(podUID types.UID, containerName string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	state, ok := m.states[podUID]
	if !ok {
		return false
	}
	_, ok = state.Containers[containerName]
	return ok
}

// IsPodCheckpointed checks if any container in a Pod is checkpointed.
func (m *Manager) IsPodCheckpointed(podUID types.UID) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	state, ok := m.states[podUID]
	if !ok {
		return false
	}
	return len(state.Containers) > 0
}

// GetCheckpointPath returns the checkpoint path for a container, or empty string.
func (m *Manager) GetCheckpointPath(podUID types.UID, containerName string) string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	state, ok := m.states[podUID]
	if !ok {
		return ""
	}
	info, ok := state.Containers[containerName]
	if !ok {
		return ""
	}
	return info.CheckpointPath
}

// GetPodCheckpointState returns the full checkpoint state for a Pod, or nil.
func (m *Manager) GetPodCheckpointState(podUID types.UID) *CheckpointState {
	m.mu.RLock()
	defer m.mu.RUnlock()

	state, ok := m.states[podUID]
	if !ok {
		return nil
	}
	// Return a copy
	cp := *state
	cp.Containers = make(map[string]ContainerCheckpointInfo)
	for k, v := range state.Containers {
		cp.Containers[k] = v
	}
	return &cp
}

// stateFilePath returns the file path for a Pod's checkpoint state.
func (m *Manager) stateFilePath(podUID types.UID) string {
	return filepath.Join(m.stateDir, string(podUID)+".json")
}

// persist writes the checkpoint state for a Pod to disk.
func (m *Manager) persist(podUID types.UID) error {
	state, ok := m.states[podUID]
	if !ok {
		return nil
	}

	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal checkpoint state: %w", err)
	}

	path := m.stateFilePath(podUID)
	if err := os.WriteFile(path, data, 0600); err != nil {
		return fmt.Errorf("failed to write checkpoint state to %s: %w", path, err)
	}
	return nil
}

// loadAll loads all persisted checkpoint states from the state directory.
func (m *Manager) loadAll() error {
	entries, err := os.ReadDir(m.stateDir)
	if err != nil {
		return fmt.Errorf("failed to read checkpoint state dir: %w", err)
	}

	var loadErrors []error
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}

		data, err := os.ReadFile(filepath.Join(m.stateDir, entry.Name()))
		if err != nil {
			loadErrors = append(loadErrors, err)
			continue
		}

		var state CheckpointState
		if err := json.Unmarshal(data, &state); err != nil {
			loadErrors = append(loadErrors, err)
			continue
		}

		m.states[state.PodUID] = &state
		klog.Infof("Restored checkpoint state for pod %s/%s (%d containers)",
			state.Namespace, state.PodName, len(state.Containers))
	}

	if len(loadErrors) > 0 {
		return fmt.Errorf("encountered %d errors loading checkpoint states", len(loadErrors))
	}
	return nil
}
