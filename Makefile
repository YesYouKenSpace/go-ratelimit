.PHONY: test bench-all bench-limiter bench-ratelimit

TEST_COUNT?=1
TEST_BENCHTIME?=10s
CONCURRENT_USERS?=32768

test:
	go test -race -covermode=atomic -coverprofile=coverage.out -test.count=$(TEST_COUNT) ./... -v -parallel=8

coverage:
	go tool cover -html=coverage.out

lint:
	golangci-lint run

bench-all: bench-limiter bench-isolated bench-distributed

# Bench recipes for limiter and ratelimit
# -run=^$ to avoid running tests
bench-limiter:
	go test ./limiter/... -v -bench=. -run=^$ -test.count=$(TEST_COUNT) -test.benchtime=$(TEST_BENCHTIME) > out/bench/limiter/$(TEST_BENCHTIME).txt.draft
	cat out/bench/limiter/$(TEST_BENCHTIME).txt.draft > out/bench/limiter/$(TEST_BENCHTIME).txt

bench-isolated:
	mkdir -p out/bench/ratelimit/isolated; \
	draft=out/bench/ratelimit/isolated/$(CONCURRENT_USERS)-$(TEST_BENCHTIME).txt.draft; \
	out=out/bench/ratelimit/isolated/$(CONCURRENT_USERS)-$(TEST_BENCHTIME).txt; \
	CONCURRENT_USERS=$(CONCURRENT_USERS) go test ./v1/ratelimit/... -v -bench=BenchmarkIsolated -run=^$ -test.count=$(TEST_COUNT) -test.benchtime=$(TEST_BENCHTIME) > $$draft; \
	cat $$draft > $$out

bench-distributed:
# -run=^$ to avoid running any tests
	mkdir -p out/bench/ratelimit/distributed; \
	draft=out/bench/ratelimit/distributed/$(CONCURRENT_USERS)-$(TEST_BENCHTIME).txt.draft; \
	out=out/bench/ratelimit/distributed/$(CONCURRENT_USERS)-$(TEST_BENCHTIME).txt; \
	CONCURRENT_USERS=$(CONCURRENT_USERS) go test ./v1/ratelimit/... -v -bench=BenchmarkDistributed -run=^$ -test.count=$(TEST_COUNT) -test.benchtime=$(TEST_BENCHTIME) > $$draft; \
	cat $$draft > $$out