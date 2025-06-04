package limiter

type Limiter interface {
	AllowN(int, float64, int) bool
	ForceN(int, float64, int) bool
}
