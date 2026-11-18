# Kafka failover lab

Does Kafka lose writes it has already acknowledged when a partition leader dies?
Three brokers on kind, Kafka's own verifiable producer, and the leader stopped
mid-stream: 13 trials, with `acks=all` and `acks=1`, crashed and restarted.

## Setup

- kind (Kubernetes 1.34, one node), Strimzi 1.3.1, Kafka 4.3.2 in KRaft mode.
- 3 nodes, each both controller and broker, 512 MB heap
  ([manifests/kafka.yaml](manifests/kafka.yaml), from Strimzi's dual-role example).
- A fresh topic per trial: 1 partition, replication factor 3, `min.insync.replicas=2`.

## Method ([trial.sh](trial.sh))

1. Create the topic and wait until all 3 replicas are in sync.
2. Start `kafka-verifiable-producer.sh`: 30,000 records at up to 3,000 a second.
   It prints a JSON line for every record a broker acknowledged and every send
   that failed. It runs with retries off (`retries=0` in its source, which also
   turns idempotence off), so a failed send is reported, not retried. A producer
   with the defaults (retries on, and idempotence on since Kafka 3.0) would resend
   those records, so the `failed` column describes this tool, not a normal app.
3. After 9,300 to 17,200 acks, stop the partition leader:
   - **sigkill**: `kill -9` its container from the kind node (`crictl`). No
     controlled shutdown, as when the process crashes.
   - **graceful**: delete its pod. Kafka's controlled shutdown moves leadership
     before the broker stops, as in a rolling restart.
4. Wait for the producer to finish and the stopped broker to rejoin the ISR.
5. Read the whole topic back ([analyze.py](analyze.py)):
   - **lost**: acked records the topic doesn't have;
   - **duplicates**: records read twice;
   - **failed_but_written**: sends reported as failed whose records are in the topic;
   - **stall**: from the last ack before the stop to the first ack after it.

## Results

| trial | acks | stop | acked | lost | duplicates | failed | failed_but_written | stall (ms) |
|---|---|---|---|---|---|---|---|---|
| all-1 | all | sigkill | 29898 | 0 | 0 | 102 | 0 | 8753 |
| all-2 | all | sigkill | 29882 | 0 | 0 | 118 | 34 | 12961 |
| all-3 | all | sigkill | 29895 | 0 | 0 | 105 | 21 | 8662 |
| all-4 | all | sigkill | 29897 | 0 | 0 | 103 | 0 | 13773 |
| all-5 | all | sigkill | 29890 | 0 | 0 | 110 | 0 | 26631 |
| graceful-all-1 | all | graceful | 29188 | 0 | 0 | 812 | 0 | 523 |
| graceful-all-2 | all | graceful | 29895 | 0 | 0 | 105 | 0 | 777 |
| graceful-all-3 | all | graceful | 29896 | 0 | 0 | 104 | 0 | 239 |
| one-1 | 1 | sigkill | 29901 | 0 | 0 | 99 | 0 | 11561 |
| one-2 | 1 | sigkill | 29901 | 0 | 0 | 99 | 0 | 11467 |
| one-3 | 1 | sigkill | 29894 | **21** | 0 | 106 | 0 | 11611 |
| one-4 | 1 | sigkill | 29898 | 0 | 0 | 102 | 0 | 20776 |
| one-5 | 1 | sigkill | 14351 | 0 | 0 | 15649 | 0 | never recovered |

Each trial's summary is in `results/`, with the raw producer output and the
records read back, gzipped.

## What it shows

- **`acks=all` lost nothing** in all 8 trials, crashed or restarted.
- **`acks=1` lost 21 acknowledged writes** in 1 of 5 crashes (one-3). They were
  the last 21 records the leader acked before it died (13750 to 13770), before
  any follower had copied them. When the broker came back, it truncated its log
  to match the new leader. The other 4 crashes lost nothing: the window is a few
  milliseconds, so whether a crash lands in it is down to timing.
- **A crash stalled writes for 8.7 to 26.6 s; a restart, for under 1 s.** The
  crash stalls fit `broker.session.timeout.ms` (9 s by default): with no
  controlled shutdown, the controller moves leadership only once it fences the
  silent broker (it logs `Fencing broker N ... because its session has timed
  out`), and the producer then has to refresh its metadata.
- **"Failed" doesn't mean "not written".** In all-2 and all-3, 34 and 21 sends
  came back as `NetworkException: Disconnected`, yet those records were
  committed. Here nothing resent them. A producer that retries with idempotence
  off would write them twice; idempotence, the default, is what prevents that.
- **one-5 never recovered.** From its timeline: the producer held the records
  meant for the dead leader in its buffer for about 10 s. When that broker came
  back it was no longer the leader, so all 15,542 were rejected with
  `NotLeaderOrFollower`. With retries off none were resent; a default producer
  would have refreshed its metadata and sent them again. No acked write was lost.

## What it doesn't show

- **This is not a machine or power failure.** All three brokers share one kind
  node. A killed broker's writes that haven't reached disk stay in that node's
  page cache. Kafka doesn't fsync each write by default; it relies on having
  replicas on other machines. Here every replica is on the same machine, so
  losing that machine would be a different test.
- One partition, one stop per trial, few trials. It shows what can happen, not
  how often.
- These are laptop timings. The stalls depend on the session timeout, the
  producer's metadata refresh and the load; they aren't production numbers.

## Run it

```sh
kind create cluster --name kafka-lab --kubeconfig ./kubeconfig
K="kubectl --kubeconfig ./kubeconfig -n kafka"
kubectl --kubeconfig ./kubeconfig create namespace kafka
curl -sSLf https://github.com/strimzi/strimzi-kafka-operator/releases/download/1.3.1/strimzi-cluster-operator-1.3.1.yaml \
  | sed 's/namespace: myproject/namespace: kafka/' >manifests/strimzi-cluster-operator-1.3.1-kafka-ns.yaml
$K create -f manifests/strimzi-cluster-operator-1.3.1-kafka-ns.yaml
$K apply -f manifests/kafka.yaml -f manifests/client.yaml
$K wait kafka/lab --for=condition=Ready --timeout=900s

bash run-all.sh                                    # 5 x acks=all, 5 x acks=1, sigkill
MODE=graceful bash trial.sh all graceful-all-1     # one restart trial
python analyze.py --table

kind delete cluster --name kafka-lab
```

The `sed` moves Strimzi's release YAML from its `myproject` namespace to `kafka`.
On Git Bash, `trial.sh` sets `MSYS_NO_PATHCONV=1` so container paths aren't
rewritten.
