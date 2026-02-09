package limiter

import ()

type Limiter interface {
	AllowN(tokenToConsume int, replenishPerSecond float64, burst int) bool
	ForceN(tokenToConsume int, replenishPerSecond float64, burst int) bool
}

type Reservation interface {
	Cancel()
}
