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

package server

import (
	"encoding/json"
	"net/http"
	"time"

	restful "github.com/emicklei/go-restful"
	"k8s.io/klog/v2"
)

// CheckpointRestoreInterface defines the methods needed for checkpoint/restore.
// This is implemented by kubelet.
type CheckpointRestoreInterface interface {
	// CheckpointPod checkpoints all business containers in a Pod.
	CheckpointPod(podNamespace, podName string) (*CheckpointRestoreResponse, error)
	// RestorePod restores all checkpointed containers in a Pod.
	RestorePod(podNamespace, podName string) (*CheckpointRestoreResponse, error)
}

// CheckpointRestoreResponse is the response for checkpoint/restore operations.
type CheckpointRestoreResponse struct {
	PodName    string                    `json:"podName"`
	Namespace  string                    `json:"namespace"`
	Containers []ContainerCRResult       `json:"containers"`
	TotalTime  string                    `json:"totalTime"`
	Error      string                    `json:"error,omitempty"`
}

// ContainerCRResult is the checkpoint/restore result for a single container.
type ContainerCRResult struct {
	Name           string `json:"name"`
	ContainerID    string `json:"containerID"`
	CheckpointPath string `json:"checkpointPath,omitempty"`
	Duration       string `json:"duration"`
	Error          string `json:"error,omitempty"`
}

// installCheckpointRestoreHandlers registers the /checkpoint and /restore HTTP handlers.
// These are custom extensions for CRIU checkpoint/restore PoC.
func (s *Server) installCheckpointRestoreHandlers(crHost CheckpointRestoreInterface) {
	klog.InfoS("Installing checkpoint/restore handlers")

	// Checkpoint handler: POST /checkpoint/{podNamespace}/{podName}
	s.addMetricsBucketMatcher("checkpoint")
	ws := new(restful.WebService)
	ws.Path("/checkpoint").
		Consumes("*/*").
		Produces(restful.MIME_JSON)
	ws.Route(ws.POST("/{podNamespace}/{podName}").
		To(func(req *restful.Request, resp *restful.Response) {
			s.handleCheckpoint(req, resp, crHost)
		}).
		Operation("checkpointPod"))
	s.restfulCont.Add(ws)

	// Restore handler: POST /restore/{podNamespace}/{podName}
	s.addMetricsBucketMatcher("restore")
	ws = new(restful.WebService)
	ws.Path("/restore").
		Consumes("*/*").
		Produces(restful.MIME_JSON)
	ws.Route(ws.POST("/{podNamespace}/{podName}").
		To(func(req *restful.Request, resp *restful.Response) {
			s.handleRestore(req, resp, crHost)
		}).
		Operation("restorePod"))
	s.restfulCont.Add(ws)
}

func (s *Server) handleCheckpoint(req *restful.Request, resp *restful.Response, crHost CheckpointRestoreInterface) {
	podNamespace := req.PathParameter("podNamespace")
	podName := req.PathParameter("podName")

	klog.V(2).InfoS("Handling checkpoint request", "namespace", podNamespace, "pod", podName)

	start := time.Now()
	result, err := crHost.CheckpointPod(podNamespace, podName)
	elapsed := time.Since(start)

	if result == nil {
		result = &CheckpointRestoreResponse{
			PodName:   podName,
			Namespace: podNamespace,
		}
	}
	result.TotalTime = elapsed.String()

	if err != nil {
		result.Error = err.Error()
		klog.ErrorS(err, "Checkpoint failed", "namespace", podNamespace, "pod", podName)
		resp.WriteHeaderAndEntity(http.StatusInternalServerError, result)
		return
	}

	klog.V(2).InfoS("Checkpoint succeeded", "namespace", podNamespace, "pod", podName, "elapsed", elapsed)
	resp.WriteHeaderAndEntity(http.StatusOK, result)
}

func (s *Server) handleRestore(req *restful.Request, resp *restful.Response, crHost CheckpointRestoreInterface) {
	podNamespace := req.PathParameter("podNamespace")
	podName := req.PathParameter("podName")

	klog.V(2).InfoS("Handling restore request", "namespace", podNamespace, "pod", podName)

	start := time.Now()
	result, err := crHost.RestorePod(podNamespace, podName)
	elapsed := time.Since(start)

	if result == nil {
		result = &CheckpointRestoreResponse{
			PodName:   podName,
			Namespace: podNamespace,
		}
	}
	result.TotalTime = elapsed.String()

	if err != nil {
		result.Error = err.Error()
		klog.ErrorS(err, "Restore failed", "namespace", podNamespace, "pod", podName)
		resp.WriteHeaderAndEntity(http.StatusInternalServerError, result)
		return
	}

	klog.V(2).InfoS("Restore succeeded", "namespace", podNamespace, "pod", podName, "elapsed", elapsed)
	resp.WriteHeaderAndEntity(http.StatusOK, result)
}

// writeJSON is a helper to write JSON response.
func writeJSON(w http.ResponseWriter, statusCode int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(v)
}
