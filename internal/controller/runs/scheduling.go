package runs

import (
	"strconv"
	"strings"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
	"github.com/bubustack/core/contracts"
	coreidentity "github.com/bubustack/core/runtime/identity"
)

const defaultQueueName = "default"

type schedulingDecision struct {
	Queue    string
	Priority int32
}

func normalizeQueueName(name string) string {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return ""
	}
	return strings.ToLower(trimmed)
}

func queueLabelValue(queue string) string {
	normalized := normalizeQueueName(queue)
	if normalized == "" {
		normalized = defaultQueueName
	}
	return coreidentity.SafeLabelValue(normalized)
}

func priorityLabelValue(priority int32) string {
	return strconv.FormatInt(int64(priority), 10)
}

func applySchedulingLabels(labels map[string]string, decision schedulingDecision) map[string]string {
	if labels == nil {
		labels = map[string]string{}
	}
	ensureSchedulingLabels(labels, decision)
	return labels
}

func ensureSchedulingLabels(labels map[string]string, decision schedulingDecision) bool {
	if labels == nil {
		return false
	}
	queue := decision.Queue
	if strings.TrimSpace(queue) == "" {
		queue = defaultQueueName
	}
	desiredQueue := queueLabelValue(queue)
	desiredPriority := priorityLabelValue(decision.Priority)
	changed := false
	if labels[contracts.QueueLabelKey] != desiredQueue {
		labels[contracts.QueueLabelKey] = desiredQueue
		changed = true
	}
	if labels[contracts.QueuePriorityLabelKey] != desiredPriority {
		labels[contracts.QueuePriorityLabelKey] = desiredPriority
		changed = true
	}
	return changed
}

func applySchedulingLabelsFromStoryRun(labels map[string]string, srun *runsv1alpha1.StoryRun) map[string]string {
	if srun == nil || srun.Labels == nil {
		return labels
	}
	if labels == nil {
		labels = map[string]string{}
	}
	if val := strings.TrimSpace(srun.Labels[contracts.QueueLabelKey]); val != "" {
		labels[contracts.QueueLabelKey] = val
	}
	if val := strings.TrimSpace(srun.Labels[contracts.QueuePriorityLabelKey]); val != "" {
		labels[contracts.QueuePriorityLabelKey] = val
	}
	return labels
}

func schedulingConfig(resolver *config.Resolver) config.SchedulingConfig {
	if resolver != nil {
		if cfg := resolver.GetOperatorConfig(); cfg != nil {
			sched := cfg.Controller.StoryRun.Scheduling
			if len(sched.Queues) == 0 {
				defaults := config.DefaultControllerConfig().StoryRun.Scheduling
				sched.Queues = defaults.Queues
			}
			return sched
		}
	}
	return config.DefaultControllerConfig().StoryRun.Scheduling
}

func queueConfigForName(cfg config.SchedulingConfig, queue string) config.QueueConfig {
	name := normalizeQueueName(queue)
	if name == "" {
		name = defaultQueueName
	}
	if cfg.Queues != nil {
		if entry, ok := cfg.Queues[name]; ok {
			return entry
		}
	}
	return config.QueueConfig{}
}

func queueDefaultPriority(cfg config.SchedulingConfig, queue string) int32 {
	return queueConfigForName(cfg, queue).DefaultPriority
}

func queueConcurrencyLimit(cfg config.SchedulingConfig, queue string) int32 {
	return queueConfigForName(cfg, queue).Concurrency
}

func queuePriorityAgingSeconds(cfg config.SchedulingConfig, queue string) int32 {
	return queueConfigForName(cfg, queue).PriorityAgingSeconds
}

func globalConcurrencyLimit(resolver *config.Resolver) int32 {
	return schedulingConfig(resolver).GlobalConcurrency
}

func resolveSchedulingDecision(story *bubuv1alpha1.Story, resolver *config.Resolver, fallback *schedulingDecision) schedulingDecision {
	cfg := schedulingConfig(resolver)
	decision := schedulingDecision{Queue: defaultQueueName, Priority: 0}
	var storyQueue string
	var storyPriority *int32
	if story != nil && story.Spec.Policy != nil {
		if story.Spec.Policy.Queue != nil {
			storyQueue = normalizeQueueName(*story.Spec.Policy.Queue)
		}
		storyPriority = story.Spec.Policy.Priority
	}

	if storyQueue != "" {
		decision.Queue = storyQueue
	} else if fallback != nil && fallback.Queue != "" {
		decision.Queue = normalizeQueueName(fallback.Queue)
	}

	switch {
	case storyPriority != nil:
		decision.Priority = *storyPriority
	case storyQueue != "":
		decision.Priority = queueDefaultPriority(cfg, decision.Queue)
	case fallback != nil:
		decision.Priority = fallback.Priority
	default:
		decision.Priority = queueDefaultPriority(cfg, decision.Queue)
	}

	if strings.TrimSpace(decision.Queue) == "" {
		decision.Queue = defaultQueueName
	}
	return decision
}

func priorityFromLabels(labels map[string]string) int32 {
	if labels == nil {
		return 0
	}
	raw := strings.TrimSpace(labels[contracts.QueuePriorityLabelKey])
	if raw == "" {
		return 0
	}
	parsed, err := strconv.ParseInt(raw, 10, 32)
	if err != nil {
		return 0
	}
	return int32(parsed)
}
