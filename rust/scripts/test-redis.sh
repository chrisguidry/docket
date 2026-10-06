#!/bin/bash
# Starts a Redis for docket-rs's tests in Docker and prints its test URL.
#
#   export DOCKET_TEST_URL=$(rust/scripts/test-redis.sh redis:8.10)
#   export DOCKET_TEST_URL=$(rust/scripts/test-redis.sh redis:8.10 cluster)
#   export DOCKET_TEST_URL=$(rust/scripts/test-redis.sh redis:8.10 acl)
#   export DOCKET_TEST_URL=$(rust/scripts/test-redis.sh redis:8.10 sentinel)
#   export DOCKET_TEST_URL=$(rust/scripts/test-redis.sh valkey/valkey:9.1)
#
# The optional third argument is the first port to use.  Containers are
# named docket-rs-test-<port> and removed when they stop.
set -euo pipefail

image=${1:?usage: test-redis.sh IMAGE [plain|cluster|acl|sentinel] [PORT]}
mode=${2:-plain}
port=${3:-16379}
here=$(cd "$(dirname "$0")" && pwd)
cli=$(case "$image" in valkey/*) echo valkey-cli ;; *) echo redis-cli ;; esac)

wait_for() {
    local container=$1 node_port=$2
    until docker exec "$container" "$cli" -p "$node_port" ping >/dev/null 2>&1; do sleep 0.1; done
}

case "$mode" in
plain | acl)
    name=docket-rs-test-$port
    docker run -d --rm --name "$name" -p "$port:6379" "$image" >/dev/null
    wait_for "$name" 6379
    if [ "$mode" = acl ]; then
        # The test user may touch only the tests' dockets, like a tenant on a
        # shared server.
        docker exec "$name" "$cli" ACL SETUSER docket on '>secret' '~docket-test-*' '&*' '+@all' >/dev/null
        docker exec "$name" "$cli" ACL SETUSER default on '>admin' >/dev/null
        echo "redis://docket:secret@localhost:$port/0"
    else
        echo "redis://localhost:$port/0"
    fi
    ;;
cluster)
    # pydocket's tests use the same three-node cluster image.
    tag="docket-cluster:${image//[\/:]/-}"
    docker build -q -t "$tag" --build-arg "BASE_IMAGE=$image" "$here/../../python/tests/cluster" >/dev/null
    name=docket-rs-test-$port
    docker run -d --rm --name "$name" \
        -e "CLUSTER_PORT_0=$port" -e "CLUSTER_PORT_1=$((port + 1))" -e "CLUSTER_PORT_2=$((port + 2))" \
        -p "$port:$port" -p "$((port + 1)):$((port + 1))" -p "$((port + 2)):$((port + 2))" \
        "$tag" >/dev/null
    until docker exec "$name" "$cli" -p "$port" cluster info 2>/dev/null | grep -q cluster_state:ok; do sleep 0.2; done
    echo "redis+cluster://localhost:$port"
    ;;
sentinel)
    # Host networking makes the master address that the sentinel reports
    # reachable from the tests.
    sentinel_port=$((port + 10000))
    docker run -d --rm --name "docket-rs-test-$port" --network host "$image" \
        redis-server --port "$port" >/dev/null
    docker run -d --rm --name "docket-rs-test-$sentinel_port" --network host "$image" sh -c \
        "printf 'port $sentinel_port\nsentinel monitor mymaster 127.0.0.1 $port 1\n' > /tmp/sentinel.conf && redis-sentinel /tmp/sentinel.conf" >/dev/null
    wait_for "docket-rs-test-$port" "$port"
    until docker exec "docket-rs-test-$sentinel_port" "$cli" -p "$sentinel_port" sentinel get-master-addr-by-name mymaster 2>/dev/null | grep -q "$port"; do sleep 0.2; done
    echo "redis+sentinel://localhost:$sentinel_port/mymaster"
    ;;
*)
    echo "unknown mode $mode: use plain, cluster, acl, or sentinel" >&2
    exit 2
    ;;
esac
