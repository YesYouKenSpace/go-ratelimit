module github.com/yesyoukenspace/go-ratelimit

go 1.23.0

toolchain go1.24.2

require (
	github.com/go-redis/redis_rate/v10 v10.0.1
	github.com/redis/go-redis/v9 v9.10.0
	golang.org/x/time v0.12.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
)

retract (
	v1.1.0 // Problems with backward compatibility
	v1.0.0 // Problems with interface
)
