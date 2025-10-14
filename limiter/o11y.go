package limiter

type MetricUpdateFunc func(value float64, labels ...string)