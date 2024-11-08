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

package networkawaredynamic

import (
	"context"
	"fmt"
	"math"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"

	"sigs.k8s.io/controller-runtime/pkg/client"

	pluginconfig "sigs.k8s.io/scheduler-plugins/apis/config"
	networkawareutil "sigs.k8s.io/scheduler-plugins/pkg/networkaware/util"

	agv1alpha1 "github.com/diktyo-io/appgroup-api/pkg/apis/appgroup/v1alpha1"
)

var _ framework.PreFilterPlugin = &NetworkAwareDynamic{}
var _ framework.FilterPlugin = &NetworkAwareDynamic{}
var _ framework.ScorePlugin = &NetworkAwareDynamic{}

const (
	// Name : name of plugin used in the plugin registry and configurations.
	Name = "NetworkAwareDynamic"

	// SameHostname : If pods belong to the same host, then consider cost as 0
	SameHostname = 0

	// preFilterStateKey is the key in CycleState to NetworkAwareDynamic pre-computed data.
	preFilterStateKey = "PreFilter" + Name
)

var scheme = runtime.NewScheme()

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(agv1alpha1.AddToScheme(scheme))
}

// NetworkAwareDynamic : Filter and Score nodes based on Pod's AppGroup requirements: MaxNetworkCosts requirements among Pods with dependencies
type NetworkAwareDynamic struct {
	client.Client

	podLister  corelisters.PodLister
	handle     framework.Handle
	namespaces []string
	prometheus *PrometheusHandle
}

// PreFilterState computed at PreFilter and used at Filter and Score.
type PreFilterState struct {
	// boolean that tells the filter and scoring functions to pass the pod since it does not belong to an AppGroup
	scoreEqually bool

	// AppGroup name of the pod
	agName string

	// AppGroup CR
	appGroup *agv1alpha1.AppGroup

	// Dependency List of the given pod
	dependencyList []agv1alpha1.DependenciesInfo

	// Pods already scheduled based on the dependency list
	scheduledList networkawareutil.ScheduledList

	// node map for cost / destinations. Search for requirements faster...
	nodeCostMap map[string]map[string]int64

	// node map for satisfied dependencies
	satisfiedMap map[string]int64

	// node map for violated dependencies
	violatedMap map[string]int64

	// node map for costs
	finalCostMap map[string]int64
}

// Clone the preFilter state.
func (no *PreFilterState) Clone() framework.StateData {
	return no
}

// Name : returns name of the plugin.
func (no *NetworkAwareDynamic) Name() string {
	return Name
}

func getArgs(obj runtime.Object) (*pluginconfig.NetworkAwareDynamicArgs, error) {
	NetworkAwareDynamicArgs, ok := obj.(*pluginconfig.NetworkAwareDynamicArgs)
	if !ok {
		return nil, fmt.Errorf("want args to be of type NetworkAwareDynamic, got %T", obj)
	}

	return NetworkAwareDynamicArgs, nil
}

// ScoreExtensions : an interface for Score extended functionality
func (no *NetworkAwareDynamic) ScoreExtensions() framework.ScoreExtensions {
	return no
}

// New : create an instance of a NetworkAwareDynamic plugin
func New(ctx context.Context, obj runtime.Object, handle framework.Handle) (framework.Plugin, error) {
	logger := klog.FromContext(ctx)
	logger.V(4).Info("Creating new instance of the NetworkAwareDynamic plugin")

	args, err := getArgs(obj)
	if err != nil {
		return nil, err
	}
	client, err := client.New(handle.KubeConfig(), client.Options{
		Scheme: scheme,
	})
	if err != nil {
		return nil, err
	}

	no := &NetworkAwareDynamic{
		Client: client,

		podLister:  handle.SharedInformerFactory().Core().V1().Pods().Lister(),
		handle:     handle,
		namespaces: args.Namespaces,
		prometheus: NewPrometheus(args.Address, args.Port, args.TimeRangeInMinutes, args.WeightLatency, args.WeightForwardBytes, args.WeightDropBytes),
	}
	return no, nil
}

func (no *NetworkAwareDynamic) PreFilter(ctx context.Context, state *framework.CycleState, pod *corev1.Pod) (*framework.PreFilterResult, *framework.Status) {
	// Init PreFilter State
	preFilterState := &PreFilterState{
		scoreEqually: true,
	}
	logger := klog.FromContext(ctx)

	// Write initial status
	state.Write(preFilterStateKey, preFilterState)

	// Check if Pod belongs to an AppGroup
	agName := networkawareutil.GetPodAppGroupLabel(pod)
	if len(agName) == 0 { // Return
		return nil, framework.NewStatus(framework.Success, "Pod does not belong to an AppGroup, return")
	}

	// Get AppGroup CR
	appGroup := no.findAppGroupNetworkAwareDynamic(ctx, logger, agName)

	// Get Dependencies of the given pod
	dependencyList := networkawareutil.GetDependencyList(pod, appGroup)

	// If the pod has no dependencies, return
	if dependencyList == nil {
		return nil, framework.NewStatus(framework.Success, "Pod has no dependencies, return")
	}

	// Get pods from lister
	selector := labels.Set(map[string]string{agv1alpha1.AppGroupLabel: agName}).AsSelector()
	pods, err := no.podLister.List(selector)
	if err != nil {
		return nil, framework.NewStatus(framework.Success, "Error while returning pods from appGroup, return")
	}

	// Return if pods are not yet allocated for the AppGroup...
	if len(pods) == 0 {
		return nil, framework.NewStatus(framework.Success, "No pods yet allocated, return")
	}

	// Pods already scheduled: Get Scheduled List (Deployment name, replicaID, hostname)
	scheduledList := networkawareutil.GetScheduledList(pods)
	// Check if scheduledList is empty...
	if len(scheduledList) == 0 {
		logger.Error(nil, "Scheduled list is empty, return")
		return nil, framework.NewStatus(framework.Success, "Scheduled list is empty, return")
	}

	// Get all nodes
	nodeList, err := no.handle.SnapshotSharedLister().NodeInfos().List()
	if err != nil {
		return nil, framework.NewStatus(framework.Error, fmt.Sprintf("Error getting the nodelist: %v", err))
	}

	// Create variables to fill PreFilterState
	nodeCostMap := make(map[string]map[string]int64)
	satisfiedMap := make(map[string]int64)
	violatedMap := make(map[string]int64)
	finalCostMap := make(map[string]int64)
	nodeIPHostnameInfoMap := GetNodeHostnamesAndIPs(nodeList)

	for _, nodeInfo := range nodeList {

		// Create map for cost / destinations.
		costMap := make(map[string]int64)

		// Populate cost map for the given node
		no.populateCostMap(logger, costMap, nodeIPHostnameInfoMap, nodeInfo.Node().Name)
		logger.V(6).Info("Map", "costMap", costMap)

		// Update nodeCostMap
		nodeCostMap[nodeInfo.Node().Name] = costMap

		// Get Satisfied and Violated number of dependencies
		satisfied, violated, ok := checkMaxNetworkCostRequirements(logger, scheduledList, dependencyList, nodeInfo, costMap, no)
		if ok != nil {
			return nil, framework.NewStatus(framework.Error, fmt.Sprintf("pod hostname not found: %v", ok))
		}

		// Update Satisfied and Violated maps
		satisfiedMap[nodeInfo.Node().Name] = satisfied
		violatedMap[nodeInfo.Node().Name] = violated
		logger.V(6).Info("Number of dependencies", "satisfied", satisfied, "violated", violated)

		// Get accumulated cost based on pod dependencies
		cost, ok := no.getAccumulatedCost(logger, scheduledList, dependencyList, nodeInfo.Node().Name, costMap)
		if ok != nil {
			return nil, framework.NewStatus(framework.Error, fmt.Sprintf("getting pod hostname from Snapshot: %v", ok))
		}
		logger.V(6).Info("Node final cost", "cost", cost)
		finalCostMap[nodeInfo.Node().Name] = cost
	}

	// Update PreFilter State
	preFilterState = &PreFilterState{
		scoreEqually:   false,
		agName:         agName,
		appGroup:       appGroup,
		dependencyList: dependencyList,
		scheduledList:  scheduledList,
		nodeCostMap:    nodeCostMap,
		satisfiedMap:   satisfiedMap,
		violatedMap:    violatedMap,
		finalCostMap:   finalCostMap,
	}

	state.Write(preFilterStateKey, preFilterState)
	return nil, framework.NewStatus(framework.Success, "PreFilter State updated")
}

// PreFilterExtensions returns prefilter extensions, pod add and remove.
func (no *NetworkAwareDynamic) PreFilterExtensions() framework.PreFilterExtensions {
	return no
}

// AddPod from pre-computed data in cycleState.
// no current need for the NetworkAwareDynamic plugin
func (no *NetworkAwareDynamic) AddPod(ctx context.Context,
	cycleState *framework.CycleState,
	podToSchedule *corev1.Pod,
	podToAdd *framework.PodInfo,
	nodeInfo *framework.NodeInfo) *framework.Status {
	return framework.NewStatus(framework.Success, "")
}

// RemovePod from pre-computed data in cycleState.
// no current need for the NetworkAwareDynamic plugin
func (no *NetworkAwareDynamic) RemovePod(ctx context.Context,
	cycleState *framework.CycleState,
	podToSchedule *corev1.Pod,
	podToRemove *framework.PodInfo,
	nodeInfo *framework.NodeInfo) *framework.Status {
	return framework.NewStatus(framework.Success, "")
}

// Filter : evaluate if node can respect maxNetworkCost requirements
func (no *NetworkAwareDynamic) Filter(ctx context.Context,
	cycleState *framework.CycleState,
	pod *corev1.Pod,
	nodeInfo *framework.NodeInfo) *framework.Status {
	if nodeInfo.Node() == nil {
		return framework.NewStatus(framework.Error, "node not found")
	}
	logger := klog.FromContext(ctx)

	// Get PreFilterState
	preFilterState, err := getPreFilterState(cycleState)
	if err != nil {
		logger.Error(err, "Failed to read preFilterState from cycleState", "preFilterStateKey", preFilterStateKey)
		return framework.NewStatus(framework.Error, "not eligible due to failed to read from cycleState")
	}

	// If scoreEqually, return nil
	if preFilterState.scoreEqually {
		logger.V(6).Info("Score all nodes equally, return")
		return nil
	}

	// Get satisfied and violated number of dependencies
	satisfied := preFilterState.satisfiedMap[nodeInfo.Node().Name]
	violated := preFilterState.violatedMap[nodeInfo.Node().Name]
	logger.V(6).Info("Number of dependencies:", "satisfied", satisfied, "violated", violated)

	// The pod is filtered out if the number of violated dependencies is higher than the satisfied ones
	if violated > satisfied {
		return framework.NewStatus(framework.Unschedulable,
			fmt.Sprintf("Node %v does not meet several network requirements from Workload dependencies: Satisfied: %v Violated: %v", nodeInfo.Node().Name, satisfied, violated))
	}
	return nil
}

// Score : evaluate score for a node
func (no *NetworkAwareDynamic) Score(ctx context.Context,
	cycleState *framework.CycleState,
	pod *corev1.Pod,
	nodeName string) (int64, *framework.Status) {
	score := framework.MinNodeScore

	logger := klog.FromContext(ctx)
	// Get PreFilterState
	preFilterState, err := getPreFilterState(cycleState)
	if err != nil {
		logger.Error(err, "Failed to read preFilterState from cycleState", "preFilterStateKey", preFilterStateKey)
		return score, framework.NewStatus(framework.Error, "not eligible due to failed to read from cycleState, return min score")
	}

	// If scoreEqually, return minScore
	if preFilterState.scoreEqually {
		return score, framework.NewStatus(framework.Success, "scoreEqually enabled: minimum score")
	}

	// Return Accumulated Cost as score
	score = preFilterState.finalCostMap[nodeName]
	logger.V(4).Info("Score:", "pod", pod.GetName(), "node", nodeName, "finalScore", score)
	return score, framework.NewStatus(framework.Success, "Accumulated cost added as score, normalization ensures lower costs are favored")
}

// NormalizeScore : normalize scores since lower scores correspond to lower latency
func (no *NetworkAwareDynamic) NormalizeScore(ctx context.Context,
	state *framework.CycleState,
	pod *corev1.Pod,
	scores framework.NodeScoreList) *framework.Status {
	logger := klog.FromContext(ctx)
	logger.V(4).Info("before normalization: ", "scores", scores)

	// Get Min and Max Scores to normalize between framework.MaxNodeScore and framework.MinNodeScore
	minCost, maxCost := getMinMaxScores(scores)

	// If all nodes were given the minimum score, return
	if minCost == 0 && maxCost == 0 {
		return nil
	}

	var normCost float64
	for i := range scores {
		if maxCost != minCost { // If max != min
			// node_normalized_cost = MAX_SCORE * ( ( nodeScore - minCost) / (maxCost - minCost)
			// nodeScore = MAX_SCORE - node_normalized_cost
			normCost = float64(framework.MaxNodeScore) * float64(scores[i].Score-minCost) / float64(maxCost-minCost)
			scores[i].Score = framework.MaxNodeScore - int64(normCost)
		} else { // If maxCost = minCost, avoid division by 0
			normCost = float64(scores[i].Score - minCost)
			scores[i].Score = framework.MaxNodeScore - int64(normCost)
		}
	}
	logger.V(4).Info("after normalization: ", "scores", scores)
	return nil
}

// MinMax : get min and max scores from NodeScoreList
func getMinMaxScores(scores framework.NodeScoreList) (int64, int64) {
	var max int64 = math.MinInt64 // Set to min value
	var min int64 = math.MaxInt64 // Set to max value

	for _, nodeScore := range scores {
		if nodeScore.Score > max {
			max = nodeScore.Score
		}
		if nodeScore.Score < min {
			min = nodeScore.Score
		}
	}
	// return min and max scores
	return min, max
}

// populateCostMap : Populates costMap based on the node being filtered/scored
func (no *NetworkAwareDynamic) populateCostMap(
	logger klog.Logger,
	costMap map[string]int64,
	nodeIPHostnameInfoMap map[string]string,
	nodeHostName string) {

	// Get the IP address corresponding to the nodeHostName
	instance, exists := nodeIPHostnameInfoMap[nodeHostName]
	if !exists {
		logger.Error(nil, "Node hostname not found in nodeIPHostnameInfoMap", "nodeHostName", nodeHostName)
		return
	}

	// Log the source IP
	logger.Info("Source IP for node", "nodeHostName", nodeHostName, "instance", instance)

	// Iterate through the nodeIPHostnameInfoMap to find other nodes
	for hostname, ipAddress := range nodeIPHostnameInfoMap {
		// Skip if the current hostname is the same as nodeHostName
		if hostname == nodeHostName {
			continue
		}

		// Assign the IP address of the current entry to 'target'
		targetNodeIP := ipAddress
		logger.Info("Calculating Network Cost", "source", instance, "target", targetNodeIP, "targetHostname", hostname)

		// Calculate network cost
		networkCost, err := no.prometheus.CalculateNetworkCost(instance, targetNodeIP)
		if err != nil {
			logger.Error(err, "Failed to calculate network cost", "source", instance, "target", targetNodeIP)
			continue
		}

		// Update the costMap with the hostname and networkCost
		costMap[hostname] = int64(networkCost)
		logger.Info("Updated costMap", "hostname", hostname, "networkCost", int64(networkCost))
	}
}

// checkMaxNetworkCostRequirements : verifies the number of met and unmet dependencies based on the pod being filtered
func checkMaxNetworkCostRequirements(
	logger klog.Logger,
	scheduledList networkawareutil.ScheduledList,
	dependencyList []agv1alpha1.DependenciesInfo,
	nodeInfo *framework.NodeInfo,
	costMap map[string]int64,
	no *NetworkAwareDynamic) (int64, int64, error) {

	var satisfied int64 = 0
	var violated int64 = 0

	// Check if maxNetworkCost fits
	for _, podAllocated := range scheduledList { // For each pod already allocated
		if podAllocated.Hostname == "" {
			continue // Skip if hostname is empty
		}

		for _, d := range dependencyList { // For each pod dependency
			// If the pod allocated is not an established dependency, continue.
			if podAllocated.Selector != d.Workload.Selector {
				continue
			}

			// If the Pod hostname matches the node being filtered, requirements are satisfied by default
			if podAllocated.Hostname == nodeInfo.Node().Name {
				satisfied += 1
				continue
			}

			// Get NodeInfo for the allocated pod's hostname
			podNodeInfo, err := no.handle.SnapshotSharedLister().NodeInfos().Get(podAllocated.Hostname)
			if err != nil {
				logger.Error(err, "Error getting pod NodeInfo from Snapshot", "podHostname", podAllocated.Hostname)
				return satisfied, violated, err
			}

			// Check the network cost against MaxNetworkCost
			if costMap[podNodeInfo.Node().Name] > d.MaxNetworkCost {
				violated += 1
				logger.Info("Network cost violated", "podHostname", podAllocated.Hostname, "nodeName", podNodeInfo.Node().Name, "cost", costMap[podNodeInfo.Node().Name], "maxAllowedCost", d.MaxNetworkCost)
			} else {
				satisfied += 1
				logger.Info("Network cost satisfied", "podHostname", podAllocated.Hostname, "nodeName", podNodeInfo.Node().Name, "cost", costMap[podNodeInfo.Node().Name], "maxAllowedCost", d.MaxNetworkCost)
			}
		}
	}

	return satisfied, violated, nil
}

// getAccumulatedCost : calculate the accumulated cost based on the Pod's dependencies
func (no *NetworkAwareDynamic) getAccumulatedCost(
	logger klog.Logger,
	scheduledList networkawareutil.ScheduledList,
	dependencyList []agv1alpha1.DependenciesInfo,
	nodeName string,
	costMap map[string]int64) (int64, error) {
	// keep track of the accumulated cost
	var cost int64 = 0

	// calculate accumulated shortest path
	for _, podAllocated := range scheduledList { // For each pod already allocated
		for _, d := range dependencyList { // For each pod dependency
			// If the pod allocated is not an established dependency, continue.
			if podAllocated.Selector != d.Workload.Selector {
				continue
			}

			if podAllocated.Hostname == nodeName { // If the Pod hostname is the node being scored
				cost += SameHostname
			} else { // If Nodes are not the same
				// Get NodeInfo from pod Hostname
				// Retrieve the cost from the costMap
				if podCost, ok := costMap[podAllocated.Hostname]; ok {
					cost += podCost
				} else {
					// Handle the case where the cost is not found in the costMap
					logger.V(2).Info("Cost not found for hostname", "hostname", podAllocated.Hostname)
				}
			}
		}
	}
	return cost, nil
}

func getPreFilterState(cycleState *framework.CycleState) (*PreFilterState, error) {
	no, err := cycleState.Read(preFilterStateKey)
	if err != nil {
		// preFilterState doesn't exist, likely PreFilter wasn't invoked.
		return nil, fmt.Errorf("error reading %q from cycleState: %w", preFilterStateKey, err)
	}

	state, ok := no.(*PreFilterState)
	if !ok {
		return nil, fmt.Errorf("%+v  convert to NetworkAwareDynamic.preFilterState error", no)
	}
	return state, nil
}

func (no *NetworkAwareDynamic) findAppGroupNetworkAwareDynamic(ctx context.Context, logger klog.Logger, agName string) *agv1alpha1.AppGroup {
	logger.V(6).Info("namespaces: %s", strings.Join(no.namespaces, ", "))
	for _, namespace := range no.namespaces {
		logger.V(6).Info("appGroup CR", "namespace", namespace, "name", agName)
		// AppGroup could not be placed in several namespaces simultaneously
		appGroup := &agv1alpha1.AppGroup{}
		err := no.Get(ctx, client.ObjectKey{
			Namespace: namespace,
			Name:      agName,
		}, appGroup)
		if err != nil {
			logger.V(4).Error(err, "Cannot get AppGroup from AppGroupNamespaceLister:")
			continue
		}
		if appGroup != nil && appGroup.GetUID() != "" {
			return appGroup
		}
	}
	return nil
}

// Function to extract hostname and internal IP address from a list of NodeInfo
func GetNodeHostnamesAndIPs(nodeInfoList []*framework.NodeInfo) map[string]string {
	nodeInfoMap := make(map[string]string)
	// Loop through each NodeInfo in the nodeInfoList
	for _, nodeInfo := range nodeInfoList {
		node := nodeInfo.Node() // Extract the *corev1.Node object
		if node == nil {
			continue // Skip if the Node object is nil
		}

		hostname := ""
		internalIP := ""

		// Loop through addresses to find the Hostname and InternalIP
		for _, address := range node.Status.Addresses {
			if address.Type == corev1.NodeHostName {
				hostname = address.Address
			}
			if address.Type == corev1.NodeInternalIP {
				internalIP = address.Address
			}
		}

		// If both Hostname and InternalIP are found, add them to the result
		if hostname != "" && internalIP != "" {
			nodeInfoMap[hostname] = internalIP
		}
	}

	return nodeInfoMap
}
