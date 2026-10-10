#!/usr/bin/env bash
# Fire N enqueue requests as fast as possible and print a histogram of HTTP status codes.
# usage: scripts/burst.sh [count] [job-type] [client-id]
N=${1:-30}; TYPE=${2:-echo}; CLIENT=${3:-demo}
for i in $(seq 1 "$N"); do
  curl -s -o /dev/null -w "%{http_code}\n" -X POST localhost:8080/api/jobs \
    -H "Content-Type: application/json" -H "X-Client-Id: $CLIENT" \
    -d "{\"type\":\"$TYPE\",\"payload\":{\"n\":$i}}"
done | sort | uniq -c
