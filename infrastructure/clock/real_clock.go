package clock

import "time"

// RealClock returns the current UTC time from the system.
type RealClock struct{}

func NewRealClock() *RealClock { return &RealClock{} }

func (*RealClock) Now() time.Time { return time.Now().UTC() }
