// Package replay provides safe projection rebuild tooling. It supports
// two strategies, selectable per projection:
//
//   - [StrategyReset]: truncate the read model and replay every event
//     from KurrentDB Start. The read model is briefly empty during
//     rebuild. Simple; suitable when the projection is fast and the
//     read-model outage is acceptable.
//
//   - [StrategyShadow]: replay into a shadow table/index, dual-write
//     during rebuild, then atomically cut over. Zero read-model
//     downtime. Requires per-sink support.
//
// The package's primary entry point is the Run function used by
// cmd/replay; module-specific integrations can also call the lower-
// level ResetCheckpoint and ShadowCutover helpers directly.
package replay

// Strategy enumerates the rebuild approaches supported by the replay
// tool.
type Strategy int

const (
	// StrategyReset truncates the read model and replays every event
	// from KurrentDB stream-start. The projection worker is then
	// restarted with the reset checkpoint, rebuilding the read model
	// from scratch. The read model is unavailable (empty or stale)
	// during the rebuild window. This is the simplest strategy and
	// the default for projections whose outage is acceptable.
	StrategyReset Strategy = iota
	// StrategyShadow replays events into a shadow read model
	// (e.g. mytable__rebuild for Postgres) while the live read model
	// continues to serve reads. Once the shadow catches up to the live
	// $all tail, an atomic cutover swaps the shadow into place (RENAME
	// TABLE for Postgres). Zero read-model downtime.
	// Requires per-sink ShadowSink support.
	StrategyShadow
)

// String renders the strategy as a human-readable name for CLI usage.
func (s Strategy) String() string {
	switch s {
	case StrategyReset:
		return "reset"
	case StrategyShadow:
		return "shadow"
	default:
		return "unknown"
	}
}
