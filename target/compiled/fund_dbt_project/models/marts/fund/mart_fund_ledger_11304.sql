with filtered as (
  select
    proc_dt,
    fund_cd,
    acct_cd,
    dc_div,
    pre_bal,
    debt_amt,
    credit_amt
  from "postgres"."bi"."stg_fund_ledger"
  where acct_cd in ('11304')
),

calc as (
  select
    proc_dt,
    fund_cd,
    acct_cd,
    case
      when dc_div = 'D' then pre_bal + debt_amt - credit_amt
      when dc_div = 'C' then pre_bal + credit_amt - debt_amt
      else null
    end as value
  from filtered
)

select * from calc