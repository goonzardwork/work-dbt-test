
  create view "postgres"."bi"."stg_fund_ledger__dbt_tmp"
    
    
  as (
    with raw as (
  select
    "proc_dt",
    "fund_cd",
    "acct_cd",
    "dc_div",
    "pre_bal"::numeric,
    "debt_amt"::numeric,
    "credit_amt"::numeric
  from "postgres"."igisam"."fngl"
  where "proc_dt" = current_date - interval '1 day'  -- 🟡 YESTERDAY ONLY
)

select * from raw
  );