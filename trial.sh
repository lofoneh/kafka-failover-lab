#!/usr/bin/env bash
# One failover trial: produce to a fresh topic (1 partition, RF 3,
# min.insync.replicas 2), SIGKILL the partition leader's container mid-stream,
# then count the records the producer saw acked that the topic no longer has.
#
# Usage: [MODE=graceful] ./trial.sh ACKS NAME [MESSAGES] [RATE] [KILL_AFTER_ACKED]
#   ACKS: all or 1 (the verifiable producer takes all as -1)
#   MODE: sigkill (default) kills the leader's container outright, with no
#         controlled shutdown; graceful deletes its pod (SIGTERM), so Kafka
#         moves leadership before the broker stops.
set -euo pipefail
cd "$(dirname "$0")"
export MSYS_NO_PATHCONV=1 # Git Bash on Windows: keep /opt/... and /tmp/... as they are

ACKS=$1 NAME=$2 MSGS=${3:-30000} RATE=${4:-3000} KILL_AT=${5:-6000}
K="kubectl --kubeconfig ./kubeconfig -n kafka"
NODE=kafka-lab-control-plane
BS=lab-kafka-bootstrap:9092
BIN=/opt/kafka/bin
T="t-$NAME"
OUT=results/$NAME
mkdir -p "$OUT"

kc() { $K exec client -- "$@"; }
die() { echo "$NAME: $*" >&2; exit 1; }
# The producer stopped on its own: bad arguments, or it already finished.
producer_done() { kc sh -c "grep -q usage: /tmp/$T.err || grep -q tool_data /tmp/$T.out" 2>/dev/null; }
describe() { kc $BIN/kafka-topics.sh --bootstrap-server $BS --describe --topic "$T"; }
isr_size() { describe | grep -o 'Isr: [0-9,]*' | cut -d' ' -f2 | tr ',' '\n' | grep -c . || true; }
wait_isr3() {
  for _ in $(seq 180); do
    [ "$(isr_size)" = 3 ] && return 0
    sleep 1
  done
  echo "ISR never reached 3" >&2
  return 1
}

kc $BIN/kafka-topics.sh --bootstrap-server $BS --create --topic "$T" \
  --partitions 1 --replication-factor 3 --config min.insync.replicas=2 >/dev/null
wait_isr3
leader=$(describe | grep -o 'Leader: [0-9]*' | cut -d' ' -f2)
pod=lab-dual-role-$leader

# acks=1 cannot be idempotent; say so rather than rely on the default.
extra=""
if [ "$ACKS" = 1 ]; then
  kc sh -c 'echo enable.idempotence=false > /tmp/acks1.properties'
  extra="--producer.config /tmp/acks1.properties"
fi
kc sh -c "$BIN/kafka-verifiable-producer.sh --bootstrap-server $BS --topic $T \
  --max-messages $MSGS --throughput $RATE --acks $([ "$ACKS" = all ] && echo -1 || echo "$ACKS") $extra \
  </dev/null >/tmp/$T.out 2>/tmp/$T.err &"

# Kill the leader once KILL_AT records are acked.
for _ in $(seq 600); do
  n=$(kc sh -c "grep -c producer_send_success /tmp/$T.out || true")
  [ "$n" -ge "$KILL_AT" ] && break
  producer_done && die "the producer stopped before $KILL_AT acks: $(kc head -3 /tmp/$T.err)"
  sleep 0.2
done
[ "$n" -ge "$KILL_AT" ] || die "only $n acks after 2 minutes"
MODE=${MODE:-sigkill}
if [ "$MODE" = graceful ]; then
  $K delete pod "$pod" --wait=false >/dev/null
else
  cid=$(docker exec $NODE crictl ps -q --name '^kafka$' --label io.kubernetes.pod.name=$pod)
  pid=$(docker exec $NODE crictl inspect -o go-template --template '{{.info.pid}}' "$cid")
  docker exec $NODE kill -9 "$pid"
fi
killed_at=$(kc sh -c "grep -c producer_send_success /tmp/$T.out || true")
echo "acks=$ACKS $NAME: stopped leader $pod (broker $leader, $MODE) at $killed_at acked"

# Wait for the producer to finish, and for the killed broker to rejoin.
for _ in $(seq 300); do
  kc sh -c "grep -q tool_data /tmp/$T.out" 2>/dev/null && break
  sleep 1
done
wait_isr3

kc cat /tmp/$T.out >"$OUT/producer.jsonl"
kc cat /tmp/$T.err >"$OUT/producer.err" || true
kc $BIN/kafka-console-consumer.sh --bootstrap-server $BS --topic "$T" \
  --from-beginning --timeout-ms 20000 >"$OUT/consumed.txt" 2>"$OUT/consumer.err" || true
describe >"$OUT/topic-after.txt"
printf '{"acks": "%s", "mode": "%s", "leader": %s, "killed_at_acked": %s}\n' "$ACKS" "$MODE" "$leader" "$killed_at" >"$OUT/meta.json"
python analyze.py "$OUT"
