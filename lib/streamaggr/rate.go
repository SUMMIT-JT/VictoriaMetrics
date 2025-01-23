package streamaggr

import (
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
)

// rateAggrSharedValue calculates output=rate_avg and rate_sum, e.g. the average per-second increase rate for counter metrics.
type rateAggrSharedValue struct {
	value          float64
	deleteDeadline int64

	// prevTimestamp is the timestamp of the last registered sample in the previous aggregation interval
	prevTimestamp int64
	blue          *rateAggrStateValue
	green         *rateAggrStateValue
}

type rateAggrStateValue struct {
	// increase stores cumulative increase for the current time series on the current aggregation interval
	increase  float64
	timestamp int64
}

type rateAggrValue struct {
	shared  map[string]rateAggrSharedValue
	isGreen bool
}

func (av *rateAggrValue) pushSample(c aggrConfig, sample *pushSample, key string, deleteDeadline int64) {
	ac := c.(*rateAggrConfig)
	var state *rateAggrStateValue
	sv, ok := av.shared[key]
	if ok {
		if av.isGreen {
			state = sv.green
		} else {
			state = sv.blue
		}
		if sample.timestamp < state.timestamp {
			// Skip out of order sample
			return
		}
		if sample.value >= sv.value {
			state.increase += sample.value - sv.value
		} else {
			// counter reset
			state.increase += sample.value
		}
	} else {
		state = &rateAggrStateValue{}
		if ac.useSharedState {
			if av.isGreen {
				sv.blue = &rateAggrStateValue{}
			} else {
				sv.green = &rateAggrStateValue{}
			}
		}
		if av.isGreen {
			sv.green = state
		} else {
			sv.blue = state
		}
		sv.prevTimestamp = sample.timestamp
	}
	sv.value = sample.value
	sv.deleteDeadline = deleteDeadline
	state.timestamp = sample.timestamp
	key = bytesutil.InternString(key)
	av.shared[key] = sv
}

func (av *rateAggrValue) flush(c aggrConfig, ctx *flushCtx, key string) {
	ac := c.(*rateAggrConfig)
	var state *rateAggrStateValue
	suffix := ac.getSuffix()
	rate := 0.0
	countSeries := 0
	for sk, sv := range av.shared {
		if ctx.flushTimestamp > sv.deleteDeadline {
			delete(av.shared, sk)
			continue
		}
		if sv.prevTimestamp == 0 {
			continue
		}
		if av.isGreen {
			state = sv.green
		} else {
			state = sv.blue
		}
		d := float64(state.timestamp-sv.prevTimestamp) / 1000
		if d > 0 {
			rate += state.increase / d
			countSeries++
		}
		sv.prevTimestamp = state.timestamp
		state.timestamp = 0
		state.increase = 0
		av.shared[sk] = sv
	}

	if countSeries == 0 {
		return
	}
	if ac.isAvg {
		rate /= float64(countSeries)
	}
	if rate > 0 {
		ctx.appendSeries(key, suffix, rate)
	}
}

func (av *rateAggrValue) state() any {
	return av.shared
}

func newRateAggrConfig(isAvg, useSharedState bool) aggrConfig {
	return &rateAggrConfig{
		isAvg:          isAvg,
		useSharedState: useSharedState,
	}
}

type rateAggrConfig struct {
	isAvg          bool
	useSharedState bool
}

func (*rateAggrConfig) getValue(s any) aggrValue {
	var shared map[string]rateAggrSharedValue
	if s == nil {
		shared = make(map[string]rateAggrSharedValue)
	} else {
		shared = s.(map[string]rateAggrSharedValue)
	}
	return &rateAggrValue{
		shared:  shared,
		isGreen: s != nil,
	}
}

func (ac *rateAggrConfig) getSuffix() string {
	if ac.isAvg {
		return "rate_avg"
	}
	return "rate_sum"
}
