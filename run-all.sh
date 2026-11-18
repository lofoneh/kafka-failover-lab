#!/usr/bin/env bash
# Five trials with acks=all and five with acks=1, then the table.
set -euo pipefail
cd "$(dirname "$0")"
for i in 1 2 3 4 5; do
  [ -f results/all-$i/summary.json ] || bash trial.sh all all-$i
done
for i in 1 2 3 4 5; do
  [ -f results/one-$i/summary.json ] || bash trial.sh 1 one-$i
done
python analyze.py --table | tee results/table.md
