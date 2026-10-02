#!/bin/bash
set -e

# Ports passed via environment (required)
PORT0=${CLUSTER_PORT_0:?CLUSTER_PORT_0 required}
PORT1=${CLUSTER_PORT_1:?CLUSTER_PORT_1 required}
PORT2=${CLUSTER_PORT_2:?CLUSTER_PORT_2 required}

# Generate Redis configs at runtime
for port in $PORT0 $PORT1 $PORT2; do
    cat > /etc/redis/node-$port.conf << EOF
port $port
cluster-enabled yes
cluster-config-file /data/nodes-$port.conf
cluster-node-timeout 5000
EOF
done

# Start all Redis instances
redis-server /etc/redis/node-$PORT0.conf &
redis-server /etc/redis/node-$PORT1.conf &
redis-server /etc/redis/node-$PORT2.conf &

# Wait for all nodes to be ready
for port in $PORT0 $PORT1 $PORT2; do
    until redis-cli -p $port ping 2>/dev/null; do
        sleep 0.1
    done
done

# Configure and create cluster
exec /start-cluster.sh
