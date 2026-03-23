/*
Copyright 2025 BubuStack.

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

package hub

import "time"

type flowControlMode string

const (
	flowControlNone    flowControlMode = "none"
	flowControlCredits flowControlMode = "credits"
	flowControlWindow  flowControlMode = "window"
)

type flowControlPolicy struct {
	mode                flowControlMode
	initialCreditsMsg   int
	initialCreditsBytes int
	ackEveryMessages    int
	ackEveryBytes       int
	ackEveryDelay       time.Duration
	pauseThresholdPct   float64
	resumeThresholdPct  float64
}

func (p flowControlPolicy) enabled() bool {
	return p.mode == flowControlCredits || p.mode == flowControlWindow
}

type orderingMode string

const (
	orderingNone         orderingMode = "none"
	orderingPerStream    orderingMode = "per_stream"
	orderingPerPartition orderingMode = "per_partition"
)

type deliverySemantics string

const (
	semanticsBestEffort  deliverySemantics = "best_effort"
	semanticsAtLeastOnce deliverySemantics = "at_least_once"
)

type replayMode string

const (
	replayNone    replayMode = "none"
	replayMemory  replayMode = "memory"
	replayDurable replayMode = "durable"
)

type replayPolicy struct {
	mode               replayMode
	retention          time.Duration
	checkpointInterval time.Duration
}

type deliveryPolicy struct {
	ordering  orderingMode
	semantics deliverySemantics
	replay    replayPolicy
}

func (p deliveryPolicy) orderingEnabled() bool {
	return p.ordering == orderingPerStream || p.ordering == orderingPerPartition
}

func (p deliveryPolicy) atLeastOnce() bool {
	return p.semantics == semanticsAtLeastOnce
}

func (p deliveryPolicy) replayEnabled() bool {
	return p.replay.mode == replayMemory || p.replay.mode == replayDurable
}
