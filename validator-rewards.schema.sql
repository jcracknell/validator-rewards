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

drop view if exists "DailyReward";
create view "DailyReward" as
select
  "g"."day",
  sum("g"."reward") / 1000000000.0 as "reward_eth",
  sum("g"."reward") as "reward_gwei"
from
  (
    select strftime('%Y-%m-%d', "e"."end_time", 'unixepoch', 'localtime') as "day", "vr"."reward"
    from "Epoch" as "e"
    join "ValidatorReward" as "vr" on "vr"."epoch_index" = "e"."index"
  ) as "g"
group by "g"."day"
order by "g"."day" asc;

drop view if exists "MonthlyReward";
create view "MonthlyReward" as
select
  "g"."month",
  sum("g"."reward") / 1000000000.0 as "reward_eth",
  sum("g"."reward") as "reward_gwei"
from
  (
    select strftime('%Y-%m', "e"."end_time", 'unixepoch', 'localtime') as "month", "vr"."reward"
    from "Epoch" as "e"
    join "ValidatorReward" as "vr" on "vr"."epoch_index" = "e"."index"
  ) as "g"
group by "g"."month"
order by "g"."month" asc;

end transaction;
