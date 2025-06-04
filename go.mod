module github.com/yesyoukenspace/go-ratelimit

go 1.24

require (
	github.com/go-redis/redis_rate/v10 v10.0.1
	github.com/redis/go-redis/v9 v9.6.3
	golang.org/x/time v0.11.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
)

retract v1.0.0 // Problems with interface
