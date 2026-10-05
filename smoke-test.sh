#!/usr/bin/env bash
# Checks a branch after `docker compose -f docker-compose.yml -f demo/<branch>.compose.yaml up -d`, from the `wan`
# network, the way HQ (or anyone on the internet) reaches the branch:
#   1. HQ's user can log in over TLS through kafka.<branch>.example:9094, with the HQ CA and host name check
#   2. HQ's user sees the two contract topics
#   3. HQ's user cannot write the summary topic (only the branch producer writes it)
#   4. a wrong password is refused
#   5. a client without TLS/SASL cannot use port 9094
#   6. wan cannot resolve the branch's Kafka, MongoDB, database or producer, nor reach the broker's other ports
#
#   HQ_KAFKA_PASSWORD=... ./smoke-test.sh BR0001 ../branch-sales-consumer/infra/tls/out/ca.crt
set -euo pipefail
export MSYS_NO_PATHCONV=1   # Git Bash on Windows: keep container paths as they are
branch=${1:?usage: HQ_KAFKA_PASSWORD=... $0 <branchCode> <hq-ca.crt>}
ca=${2:?usage: HQ_KAFKA_PASSWORD=... $0 <branchCode> <hq-ca.crt>}
password=${HQ_KAFKA_PASSWORD:?set HQ_KAFKA_PASSWORD (HQ password at this branch)}
[[ -f $ca ]] || { echo "$ca not found"; exit 1; }
ca="$(cd "$(dirname "$ca")" && (pwd -W 2>/dev/null || pwd))/$(basename "$ca")"
host="kafka.$(tr '[:upper:]' '[:lower:]' <<<"$branch").example"
IMAGE=apache/kafka:4.3.1
BIN=/opt/kafka/bin

hq() { # hq <password> <protocol> <command>: run on the wan network as HQ's client
  docker run --rm -i --network branch-sales-wan -v "$ca:/certs/ca.crt:ro" --entrypoint /bin/bash "$IMAGE" -c "
cat > /tmp/client.properties <<P
security.protocol=$2
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"hq\" password=\"$1\";
ssl.truststore.type=PEM
ssl.truststore.location=/certs/ca.crt
max.block.ms=10000
request.timeout.ms=10000
delivery.timeout.ms=15000
P
$3"
}

echo "1-2. HQ -> $host:9094 (TLS + SCRAM): topics"
topics=$(hq "$password" SASL_SSL "$BIN/kafka-topics.sh --bootstrap-server $host:9094 \
  --command-config /tmp/client.properties --list 2>&1") || { echo "   FAIL: $topics"; exit 1; }
for t in branch-sales.daily-summary branch-sales.receipt; do
  grep -qx "$t" <<<"$topics" || { echo "   FAIL: $t not listed: $topics"; exit 1; }
done
echo "   ok"

echo "3. HQ cannot write the summary topic"
out=$(echo "smoke-test" | hq "$password" SASL_SSL "$BIN/kafka-console-producer.sh --bootstrap-server $host:9094 \
  --topic branch-sales.daily-summary --producer.config /tmp/client.properties 2>&1" || true)
grep -q 'TopicAuthorizationException' <<<"$out" || { echo "   FAIL: write was not refused: $out"; exit 1; }
echo "   ok"

echo "4. wrong password is refused"
out=$(hq "wrong-password" SASL_SSL "$BIN/kafka-topics.sh --bootstrap-server $host:9094 \
  --command-config /tmp/client.properties --list 2>&1" || true)
grep -q 'SaslAuthenticationException' <<<"$out" || { echo "   FAIL: login was not refused: $out"; exit 1; }
echo "   ok"

echo "5. client without TLS/SASL cannot use port 9094"
if hq "$password" PLAINTEXT "timeout 20 $BIN/kafka-topics.sh --bootstrap-server $host:9094 \
  --command-config /tmp/client.properties --list > /dev/null 2>&1"; then
  echo "   FAIL: plaintext client got a topic list"; exit 1
fi
echo "   ok"

echo "6. wan reaches only the edge, port 9094"
docker run --rm --network branch-sales-wan --entrypoint /bin/bash "$IMAGE" -c "
  for name in kafka mongodb branch-db producer; do
    if getent hosts \$name > /dev/null; then echo \"   FAIL: \$name resolvable from wan\"; exit 1; fi
  done
  for port in 19092 9093 27017 5432; do
    if timeout 5 bash -c \"echo > /dev/tcp/$host/\$port\" 2> /dev/null; then
      echo \"   FAIL: $host:\$port reachable from wan\"; exit 1
    fi
  done"
echo "   ok"

echo "smoke test passed"
