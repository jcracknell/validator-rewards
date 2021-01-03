#!/bin/bash

showUsage() {
  {
    [ ! -z "$1" ] && echo "$1"
    echo ""
    echo "Usage: $(basename "$0") -d <dbfile> -b <beacon_url> -v <validator_ids>"
    echo ""
    echo "-b  Base beacon node API url."
    echo "-d  SQLite data file."
    echo "-v  Comma separated list of validator indices."
  } >&2
  exit 1
}

GENESIS_TIME=1606824023
BEACON_NODE=""
DBFILE=""
VALIDATORS=""

while [ $# -gt 0 ]; do
  case "$1" in
    -b)
      BEACON_NODE="$2"
      shift 2
      ;;
    -d)
      DBFILE="$2"
      shift 2
      ;;
    -v)
      if [ -z "$VALIDATORS" ]; then
        VALIDATORS="$2"
      else
        VALIDATORS="$VALIDATORS,$2"
      fi
      VALIDATORS="$(echo "$VALIDATORS" | sed -r 's/\s*(,\s*)/,/g')"
      shift 2
      ;;
    -?|--help)
      showUsage
      ;;
    *)
      showUsage "Unsupported argument: $1"
      ;;
  esac
done

[ -z "$BEACON_NODE" ] && showUsage "No beacon node API url specified."
[ -z "$DBFILE" ] && showUsage "No db file specified."

if [ -z "$VALIDATORS" -a -f "$DBFILE" ]; then
  VALIDATORS="$(sqlite3 "$DBFILE" 'select group_concat("index", '"'"','"'"') from "Validator";')"
fi

[ -z "$VALIDATORS" ] && showUsage "No validators specified."

echo "Beacon node: $BEACON_NODE"
echo "   Database: $DBFILE"
echo " Validators: $VALIDATORS"
echo ""

cat "$(dirname "$0")/validator-rewards.schema.sql" | sqlite3 -bail "$DBFILE"

echo '
  insert into "Validator" ("index", "pubkey", "status", "active_epoch", "withdrawable_epoch", "data")
  select
    cast(json_extract("json"."value", '"'\$.index'"') as integer) as "index",
    json_extract("json"."value", '"'\$.validator.pubkey'"') as "pubkey",
    json_extract("json"."value", '"'\$.status'"') as "status",
    cast(json_extract("json"."value", '"'\$.validator.activation_epoch'"') as integer) as "active_epoch",
    cast(json_extract("json"."value", '"'\$.validator.withdrawable_epoch'"') as integer) as "withdrawable_epoch",
    "json"."value" as "data"
  from json_each('"'$(
    curl -sSG "$BEACON_NODE/eth/v1/beacon/states/head/validators" -d id="$VALIDATORS" \
    | sed -r "s/'/''/g"
  )'"', '"'\$.data'"') as "json"
  where 1 = 1
  on conflict ("index") do update set
    "pubkey" = "excluded"."pubkey",
    "status" = "excluded"."status",
    "active_epoch" = "excluded"."active_epoch",
    "withdrawable_epoch" = "excluded"."withdrawable_epoch",
    "data" = "excluded"."data"
  ;
' | sqlite3 -bail "$DBFILE"

while true; do
  lastEpoch=$(( ($(date +%s) - $GENESIS_TIME) / (12 * 32) ))

  # Get the next epoch we need to process from the database, the minumum of the most
  # recent epoch we have data for each validator or its activation epoch
  epoch=$(sqlite3 "$DBFILE" '
    select min("epoch") from (
      select coalesce("m"."max_epoch" + 1, "v"."active_epoch") as "epoch"
      from "Validator" as "v"
      left join (
        select "r"."validator_index", max("r"."epoch_index") as "max_epoch"
        from "ValidatorReward" as "r"
        group by "r"."validator_index"
      ) as "m" on "m"."validator_index" = "v"."index"
      where "m"."max_epoch" is null or ("v"."active_epoch" <= "m"."max_epoch" and "m"."max_epoch" + 1 <= "v"."withdrawable_epoch")
    )
  ')


  if [ -z "$epoch" ]; then
    echo "All validators exited."
    break
  fi
  if [ $epoch -ge $lastEpoch ]; then
    echo "All validators up to date."
    break
  fi

  rewardSlot=$((32 * ($epoch + 1)))
  epochStart=$(($GENESIS_TIME + 12 * 32 * $epoch))
  epochEnd=$(($GENESIS_TIME + 12 * 32 * ($epoch + 1)))
  echo "Processing epoch $epoch of $lastEpoch ($((100 * $epoch / $lastEpoch))%, $(date -Iseconds --date @$epochStart))"

  echo -n '
    pragma busy_timeout = 30000;
    pragma foreign_keys = on;
    begin transaction;

    insert into "Epoch" ("index", "start_time", "end_time")
    values ('$epoch', '$epochStart', '$epochEnd')
    on conflict do nothing;

    with
      "data" as (
        select
          cast(json_extract("json"."value", '"'\$.index'"') as integer) as "validator_index",
          cast(json_extract("json"."value", '"'\$.balance'"') as integer) as "balance"
        from json_each('"'$(
          curl -sSG "$BEACON_NODE/eth/v1/beacon/states/$rewardSlot/validator_balances" -d id="$VALIDATORS" \
          | sed -r "s/'/''/g"
        )'"', '"'\$.data'"') as "json"
      )
    insert into "ValidatorReward" ("epoch_index", "validator_index", "balance", "reward")
    select
      '$epoch' as "epoch_index",
      "data"."validator_index",
      "data"."balance",
      ("data"."balance" - coalesce("last"."balance", 32000000000)) as "reward"
    from
      "data"
      join "Validator" as "v" on "v"."index" = "data"."validator_index"
      left join "ValidatorReward" as "last"
        on "last"."epoch_index" = '$epoch' - 1 and "last"."validator_index" = "data"."validator_index"
    where "v"."active_epoch" <= '$epoch' and '$epoch' <= "v"."withdrawable_epoch"
    on conflict ("epoch_index", "validator_index") do update set
      "balance" = "excluded"."balance",
      "reward" = "excluded"."reward"
    ;

    commit transaction;
  ' | sqlite3 -bail "$DBFILE"

  if [ $(($epoch % 100)) -eq 0 ]; then
    echo "Vacuuming database..."
    echo 'vacuum;' | sqlite3 "$DBFILE";
  fi

  sleep 1
done

echo 'vacuum;' | sqlite3 "$DBFILE";
