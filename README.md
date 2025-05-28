# go-ratelimit

`go-ratelimit` is a high-performance Go library for rate limiting that provides multiple implementations optimized for different use cases. The library focuses on providing both isolated and distributed rate limiting solutions with a focus on performance and flexibility.

## Design Goals

1. **High Performance**: The library is designed to handle high-throughput distributed servers with minimal latency impact.
2. **Flexibility**: Multiple implementations to choose from based on your specific needs:
   - Isolated rate limiting for single-instance applications
   - Distributed rate limiting for multi-instance deployments
3. **Compatibility**: Follows the familiar `golang.org/x/time/rate` interface for easy integration
4. **Accuracy vs Performance Trade-offs**: Different implementations offer different trade-offs between accuracy and performance

## Features

- All `limiter.Limiter` implementations follow the `Ratelimit` interface in [domain.go](./domain.go) which closely follows `golang.org/x/time/rate` interface
- Multiple synchronization mechanisms: `sync.Mutex`, `sync.RWMutex`, and `sync.Map`
- Built-in benchmarking tools to compare different implementations
- Support for both isolated and distributed rate limiting scenarios

## Installation

To install the library:
```bash
go get -u github.com/yesyoukenspace/go-ratelimit
```

## Usage

For usage examples, check out [distributed_bench_test.go](v1/ratelimit/distributed_bench_test.go) and [isolated_bench_test.go](v1/ratelimit/isolated_bench_test.go).

## Implementations

### Distributed Rate Limiting

#### **RedisDelayedSync**
A high-performance distributed rate limiter designed for systems that prioritize throughput over strict accuracy:

- **Design**: Uses a hybrid approach combining local state with asynchronous Redis synchronization
- **Key Features**:
  - Non-blocking `AllowN` operations for maximum throughput
  - Asynchronous Redis synchronization to minimize latency impact
  - Local state management using `SyncMapLoadThenStore` for concurrent access
- **Performance**: Up to 1000x faster than [go-redis/redis_rate](https://github.com/go-redis/redis_rate)
  - Benchmarks show 44 million request_per_second vs 45,000 request_per_second for go-redis/redis_rate
- **Trade-offs**: Slightly relaxed accuracy in exchange for significantly better performance, this is mitigated by **Penalty Spillover**
- **Penalty Spillover**: If users exceed designated rate-limit globally, upon the next synchronization the user would still throttled accordingly
- **Use Cases**: High-throughput distributed systems where occasional rate limit inaccuracies are acceptable

```mermaid
sequenceDiagram
    participant Client
    Box Server 1
      participant RDS1 as RedisDelayedSync1
    end
    Box Server 2
      participant RDS2 as RedisDelayedSync2
    end
    participant Redis



    Note over RDS1: Initial State: 5 tokens available
    Note over RDS2: Initial State: 5 tokens available
    Note over Redis: Initial State: 5 tokens available

    par
      Client->>RDS1: Request 3 tokens
      RDS1->>RDS1: Check and modify local state (2 tokens remaining)
      Note over RDS1: Local State: 2 tokens available
      RDS1->>Client: Allow
    and
      Client->>RDS2: Request 2 tokens
      RDS2->>RDS2: Check and modify local state (3 tokens remaining)
      RDS2->>Client: Allow
      Note over RDS2: Local State: 3 tokens available

    end

    alt happy path
      RDS1->>Redis: Async sync (-3 tokens)
      Redis->>RDS1: 2 tokens available
      Note over Redis: 2 tokens available
      RDS2->>Redis: Async sync (-2 tokens, local state: 0 tokens available)
      Redis->>RDS2: 0 tokens available
      Note over Redis: 0 tokens available
      Note over RDS2: Local State: 0 tokens available
      RDS1->>Redis: Async sync 
      Redis->>RDS1: 0 tokens available
      Note over RDS1: 0 tokens available
    else not so happy path
      RDS1->>Redis: Async sync (-3 tokens)
      Redis->>RDS1: 2 tokens available
      Note over Redis: 2 tokens available
      RDS2->>Redis: Async sync (-2 tokens, local state: 0 tokens available)
      Redis->>RDS2: 0 tokens available
      Note over Redis: 0 tokens available
      Note over RDS2: Local State: 0 tokens available
      Client->>RDS1: Request 2 tokens
      RDS1->>RDS1: Check and modify local state (0 tokens remaining)
      Note over RDS1: Local State: 0 tokens available
      RDS1->>Client: Allow
      RDS1->>Redis: Async sync (-2 tokens)
      Redis->>RDS1: -2 tokens available
      Note over RDS1: -2 tokens available, RDS1 will be in deficit and wait for tokens to be replenished
    end 

    Note over RateLimiter,Redis: After sync: Global state updated
```

#### **GoRedisRate**
A wrapper around `github.com/go-redis/redis_rate` for testing and benchmarking purposes.

### Isolated Rate Limiting

The repository provides several implementations optimized for single-instance use cases:

- **Mutex**: Simple implementation using `sync.Mutex` for basic rate limiting
- **RWMutex**: Optimized for read-heavy workloads using `sync.RWMutex`
- **SyncMapLoadThenLoadOrStore**: Uses `sync.Map` with `Load` then `LoadOrStore` pattern
- **SyncMapLoadOrStore**: Direct `sync.Map` implementation using `LoadOrStore`
- **SyncMapLoadThenStore**: Optimized `sync.Map` implementation using `Load` then `Store` pattern

## Benchmarking

The repo includes benchmarks to compare different implementations. Results are available at [./out/bench](./out/bench/).

### Benchmark Environment
```bash
> sysctl -a machdep.cpu
machdep.cpu.cores_per_package: 10
machdep.cpu.core_count: 10
machdep.cpu.logical_per_package: 10
machdep.cpu.thread_count: 10
machdep.cpu.brand_string: Apple M1 Pro
```

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change. Make sure to update tests as appropriate.

## License

See the [LICENSE](LICENSE) file for details.

## References
- [go-redis/redis_rate](https://github.com/go-redis/redis_rate)
- [uber-go/ratelimit](https://github.com/uber-go/ratelimit)

## Authors and Acknowledgments
- @YesYouKenSpace


