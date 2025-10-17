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

import (
	"os"
	"strconv"
	"time"

	"github.com/bubustack/bobrapet/pkg/contracts"
)

var (
	// EvictionInterval is the interval at which the hub evicts idle message buffers.
	EvictionInterval = getEvictionInterval()
)

func getChannelBufferSize() int {
	valStr := os.Getenv(contracts.GRPCChannelBufferSizeEnv)
	if val, err := strconv.Atoi(valStr); err == nil && val > 0 {
		return val
	}
	return 100 // default
}

func getEvictionInterval() time.Duration {
	valStr := os.Getenv(contracts.HubBufferEvictionIntervalEnv)
	if d, err := time.ParseDuration(valStr); err == nil && d > 0 {
		return d
	}
	return 1 * time.Minute // default
}
