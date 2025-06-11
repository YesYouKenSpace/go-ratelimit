.PHONY: test bench-all bench-limiter bench-ratelimit

TEST_COUNT?=1
TEST_BENCHTIME?=10s
TOTAL_CONCURRENCY?=10
CONCURRENCY_PER_USER?=2
NUM_SERVERS?=2

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
	draft=out/bench/ratelimit/isolated/$(TOTAL_CONCURRENCY)-$(CONCURRENCY_PER_USER)-$(TEST_BENCHTIME).txt.draft; \
	out=out/bench/ratelimit/isolated/$(TOTAL_CONCURRENCY)-$(CONCURRENCY_PER_USER)-$(TEST_BENCHTIME).txt; \
	TOTAL_CONCURRENCY=$(TOTAL_CONCURRENCY) CONCURRENCY_PER_USER=$(CONCURRENCY_PER_USER) go test ./v1/ratelimit/... -v -bench=BenchmarkIsolated -run=^$ -test.count=$(TEST_COUNT) -test.benchtime=$(TEST_BENCHTIME) > $$draft; \
	cat $$draft > $$out

bench-distributed:
# -run=^$ to avoid running any tests
	mkdir -p out/bench/ratelimit/distributed; \
	draft=out/bench/ratelimit/distributed/$(TOTAL_CONCURRENCY)-$(CONCURRENCY_PER_USER)-$(NUM_SERVERS)-$(TEST_BENCHTIME).txt.draft; \
	out=out/bench/ratelimit/distributed/$(TOTAL_CONCURRENCY)-$(CONCURRENCY_PER_USER)-$(NUM_SERVERS)-$(TEST_BENCHTIME).txt; \
	TOTAL_CONCURRENCY=$(TOTAL_CONCURRENCY) CONCURRENCY_PER_USER=$(CONCURRENCY_PER_USER) NUM_SERVERS=$(NUM_SERVERS) go test ./v1/ratelimit/... -v -bench=BenchmarkDistributed -run=^$ -test.count=$(TEST_COUNT) -test.benchtime=$(TEST_BENCHTIME) > $$draft; \
	cat $$draft > $$out