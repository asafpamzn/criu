#!/bin/bash
# Fuzzy traffic: mixed GET/SET/DEL/HSET/HGET/SADD/SREM on random keys
# Runs until killed
HOST=${1:-127.0.0.1}
PORT=${2:-6379}

while true; do
    valkey-cli -h $HOST -p $PORT <<'EOF' 2>/dev/null
SET fuzz:str:$RANDOM "value_$(date +%s%N)"
GET fuzz:str:$RANDOM
DEL fuzz:str:$RANDOM
HSET fuzz:hash:$((RANDOM % 100)) field:$RANDOM "val_$RANDOM"
HGET fuzz:hash:$((RANDOM % 100)) field:$RANDOM
HDEL fuzz:hash:$((RANDOM % 100)) field:$RANDOM
SADD fuzz:set:$((RANDOM % 50)) "member_$RANDOM"
SREM fuzz:set:$((RANDOM % 50)) "member_$RANDOM"
ZADD fuzz:zset:$((RANDOM % 50)) $RANDOM "elem_$RANDOM"
ZREM fuzz:zset:$((RANDOM % 50)) "elem_$RANDOM"
LPUSH fuzz:list:$((RANDOM % 20)) "item_$RANDOM"
RPOP fuzz:list:$((RANDOM % 20))
SET fuzz:big:$((RANDOM % 1000)) "$(head -c 4096 /dev/urandom | base64)"
GET fuzz:big:$((RANDOM % 1000))
EOF
done
