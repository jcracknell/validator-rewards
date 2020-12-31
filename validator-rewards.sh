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
[ -z "$VALIDATORS" ] && showUsage "No validators specified."

echo '
  begin transaction;

  create table if not exists "Epoch" (
    "index" integer not null,
    "start_time" bigint not null,
    "end_time" bigint not null,
    constraint "PK_Epoch" primary key ("index")
  ) without rowid;

  create table if not exists "Validator" (
    "index" integer not null,
    "pubkey" text not null,
    "entry_epoch" integer not null,
    "active_epoch" integer not null,
    "exit_epoch" integer not null,
    constraint "PK_Validator" primary key ("index")
  ) without rowid;

  create table if not exists "ValidatorReward" (
    "epoch_index" integer not null,
    "validator_index" integer not null,
    "balance" bigint not null,
    "reward" bigint not null,
    constraint "PK_ValidatorReward" primary key ("epoch_index", "validator_index"),
    constraint "FK_ValidatorReward_Epoch" foreign key ("epoch_index") references "Epoch" ("index"),
    constraint "FK_ValidatorReword_Validator" foreign key ("validator_index") references "Validator" ("index")
  ) without rowid;

  end transaction;
' | sqlite3 -bail "$DBFILE"

echo '
  with "data" ("index", "pubkey", "entry_epoch", "active_epoch", "exit_epoch") as
    (select * from (values '"$(
      curl -sSG "$BEACON_NODE/eth/v1/beacon/states/head/validators" -d id="$VALIDATORS" \
      | jq -r '
          .data | sort_by(.index | tonumber)
          | map([
              .index,
              ("'"'"'" + .validator.pubkey + "'"'"'"),
              .validator.activation_eligibility_epoch,
              .validator.activation_epoch,
              .validator.exit_epoch
            ] | "(" + join(", ") + ")")
          | join(", ")
        '
    )"'))
  insert into "Validator" ("index", "pubkey", "entry_epoch", "active_epoch", "exit_epoch")
  select "data"."index", "data"."pubkey", "data"."entry_epoch", "data"."active_epoch", "data"."exit_epoch"
  from "data"
  where 1 = 1
  on conflict ("index") do update set
    "pubkey" = "excluded"."pubkey",
    "entry_epoch" = "excluded"."entry_epoch",
    "active_epoch" = "excluded"."active_epoch",
    "exit_epoch" = "excluded"."exit_epoch"
  ;
' | sqlite3 -bail "$DBFILE"

while true; do
  lastEpoch=$(( ($(date +%s) - $GENESIS_TIME) / (12 * 32) ))

  # Get the next epoch we need to process from the database, the minumum of the most
  # recent epoch we have data for each validator or its activation epoch
  epoch=$(
    echo '
      select coalesce(min("epoch_index"), 0) from (
        select coalesce(max("vr"."epoch_index") + 1, "v"."active_epoch") as "epoch_index"
        from "Validator" as "v"
        left join "ValidatorReward" as "vr" on "vr"."validator_index" = "v"."index"
        group by "v"."index", "v"."active_epoch"
      )
    ' | sqlite3 "$DBFILE"
  )


  if [ $epoch -ge $lastEpoch ]; then
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

    with "data" ("validator_index", "balance") as
      (select * from (values '"$(
        curl -sSG "$BEACON_NODE/eth/v1/beacon/states/$rewardSlot/validator_balances" -d id="$VALIDATORS" \
        | jq --raw-output '.data | sort_by(.index | tonumber) | map("(\(.index), \(.balance))") | join(", ")'
      )"'))
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
    where "v"."active_epoch" <= '$epoch'
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

  epoch=$(($epoch + 1))
  sleep 1
done

echo 'vacuum;' | sqlite3 "$DBFILE";