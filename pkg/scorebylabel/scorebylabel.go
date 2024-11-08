package scorebylabel

import (
	"context"
	"fmt"
	"strconv"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"

	pluginConfig "sigs.k8s.io/scheduler-plugins/apis/config"
)

type ScoreByLabel struct {
	handle framework.Handle
}

const (
	Name                     = "ScoreByLabel" // Name is the name of the plugin used in Registry and configurations.
	DefaultMissingLabelScore = 0
)

var _ = framework.ScorePlugin(&ScoreByLabel{})
var LabelKey string

// New initializes a new plugin and returns it.
func New(_ context.Context, obj runtime.Object, handle framework.Handle) (framework.Plugin, error) {
	var args, ok = obj.(*pluginConfig.ScoreByLabelArgs)
	if !ok {
		return nil, fmt.Errorf("[ScoreByLabelArgs] want args to be of type ScoreByLabelArgs, got %T", obj)
	}

	klog.Infof("[ScoreByLabelArgs] args received. LabelKey: %s", args.LabelKey)
	LabelKey = args.LabelKey

	return &ScoreByLabel{
		handle: handle,
	}, nil
}

func (s *ScoreByLabel) Name() string {
	return Name
}

func (s *ScoreByLabel) Score(ctx context.Context, state *framework.CycleState, p *v1.Pod, nodeName string) (int64, *framework.Status) {
	nodeInfo, err := s.handle.SnapshotSharedLister().NodeInfos().Get(nodeName)
	if err != nil {
		return 0, framework.NewStatus(framework.Error, fmt.Sprintf("error getting node information: %s", err))
	}

	nodeLabels := nodeInfo.Node().Labels

	if val, ok := nodeLabels[LabelKey]; ok {
		scoreVal, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			klog.V(4).InfoS("unable to parse score value from node labels", LabelKey, "=", val)
			klog.V(4).InfoS("use the default score", DefaultMissingLabelScore, " for node with labels not convertable to int64!")
			return DefaultMissingLabelScore, nil
		}

		klog.Infof("[ScoreByLabel] Label score for node %s is %s = %v", nodeName, LabelKey, scoreVal)

		return scoreVal, nil
	}

	return DefaultMissingLabelScore, nil
}

func (s *ScoreByLabel) ScoreExtensions() framework.ScoreExtensions {
	return s
}

func (s *ScoreByLabel) NormalizeScore(ctx context.Context, state *framework.CycleState, p *v1.Pod, scores framework.NodeScoreList) *framework.Status {
	var higherScore int64
	higherScore = framework.MinNodeScore
	for _, node := range scores {
		if higherScore < node.Score {
			higherScore = node.Score
		}
	}

	for i, node := range scores {
		scores[i].Score = node.Score * framework.MaxNodeScore / higherScore
	}

	klog.Infof("[ScoreByLabel] Nodes final score: %v", scores)
	return nil
}
