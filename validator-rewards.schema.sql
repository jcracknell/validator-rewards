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
  "status" text not null,
  "active_epoch" integer not null,
  "withdrawable_epoch" integer not null,
  "data" text not null,
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

drop view if exists "DailyValidatorReward";
create view "DailyValidatorReward" as
select
  "r"."day",
  "r"."validator_index",
  "v"."pubkey",
  sum("r"."reward") / 1000000000.0 as "reward_eth",
  sum("r"."reward") as "reward_gwei"
from
  (
    select
      strftime('%Y-%m-%d', "e"."end_time", 'unixepoch', 'localtime') as "day",
      "vr"."validator_index",
      "vr"."reward"
    from "Epoch" as "e"
    join "ValidatorReward" as "vr" on "vr"."epoch_index" = "e"."index"
  ) as "r"
  join "Validator" as "v" on "v"."index" = "r"."validator_index"
group by "r"."day", "r"."validator_index", "v"."pubkey"
order by "r"."day" asc, "r"."validator_index" asc;

drop view if exists "MonthlyValidatorReward";
create view "MonthlyValidatorReward" as
select
  "r"."month",
  "r"."validator_index",
  "v"."pubkey",
  sum("r"."reward") / 1000000000.0 as "reward_eth",
  sum("r"."reward") as "reward_gwei"
from
  (
    select
      strftime('%Y-%m', "e"."end_time", 'unixepoch', 'localtime') as "month",
      "vr"."validator_index",
      "vr"."reward"
    from "Epoch" as "e"
    join "ValidatorReward" as "vr" on "vr"."epoch_index" = "e"."index"
  ) as "r"
  join "Validator" as "v" on "v"."index" = "r"."validator_index"
group by "r"."month", "r"."validator_index", "v"."pubkey"
order by "r"."month" asc, "r"."validator_index" asc;

end transaction;
