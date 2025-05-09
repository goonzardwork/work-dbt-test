
  
    

  create  table "postgres"."bi_staging"."stg_fund_master__dbt_tmp"
  
  
    as
  
  (
    with afn as (
    select * from "postgres"."igisbiz"."afndbascm"
),
fn as (
    select * from "postgres"."igisam"."fn"
),
mtfn as (
    select distinct "FUND_CD" from "postgres"."igisam"."mtfn"
)

select
    -- FUND identifiers and names
    afn.FUND_CD,
    afn.FUND_NM,
    afn.FUND_NUM,
    afn.FUND_NM_FULL,
    afn.FUND_ANM,
    afn.FUND_ENM,
    afn.FUND_ENG_ANM,

    -- Status and classification
    afn.MNGT_STAT,
    
(
    select code_nm
    from "postgres"."igisdms"."mdbmcodef" code
    where code.code_grp_id = 'MNGT_STAT'
      and code.code_id = afn.MNGT_STAT
    limit 1
)
 as MNGT_STAT_TXT,
    coalesce(afn.BLND_YN, 'N') as BLND_YN,
    afn.FUND_CLS,
    
(
    select code_nm
    from "postgres"."igisdms"."mdbmcodef" code
    where code.code_grp_id = 'FUND_CLS'
      and code.code_id = afn.FUND_CLS
    limit 1
)
 as FUND_CLS_TXT,
    afn.FUND_KICD,
    
(
    select code_nm
    from "postgres"."igisdms"."mdbmcodef" code
    where code.code_grp_id = 'AUREM_FUND_KICD'
      and code.code_id = afn.FUND_KICD
    limit 1
)
 as FUND_KICD_TXT,
    afn.INVT_TYPE,
    
(
    select code_nm
    from "postgres"."igisdms"."mdbmcodef" code
    where code.code_grp_id = 'INVT_TYPE'
      and code.code_id = afn.INVT_TYPE
    limit 1
)
 as INVT_TYPE_TXT,
    afn.INVT_ASET_TYPE,
    
(
    select code_nm
    from "postgres"."igisdms"."mdbmcodef" code
    where code.code_grp_id = 'INVT_ASET_TYPE'
      and code.code_id = afn.INVT_ASET_TYPE
    limit 1
)
 as INVT_ASET_TYPE_TXT,
    afn.INVT_TRGT_LCTN_AREA,
    
(
    select code_nm
    from "postgres"."igisdms"."mdbmcodef" code
    where code.code_grp_id = 'INVT_TRGT_LCTN_AREA'
      and code.code_id = afn.INVT_TRGT_LCTN_AREA
    limit 1
)
 as INVT_TRGT_LCTN_AREA_TXT,
    afn.INVT_STTG,
    
(
    select code_nm
    from "postgres"."igisdms"."mdbmcodef" code
    where code.code_grp_id = 'INVT_STTG'
      and code.code_id = afn.INVT_STTG
    limit 1
)
 as INVT_STTG_TXT,

    -- Investment characteristics
    coalesce(afn.DVLP_YN, 'N') as DVLP_YN,
    afn.INVT_TRGT_ASET,
    
(
    select code_nm
    from "postgres"."igisdms"."mdbmcodef" code
    where code.code_grp_id = 'AUREM_INVT_TRGT_ASET'
      and code.code_id = afn.INVT_TRGT_ASET
    limit 1
)
 as INVT_TRGT_ASET_TXT,
    afn.DMAB_CLS,
    
(
    select code_nm
    from "postgres"."igisdms"."mdbmcodef" code
    where code.code_grp_id = 'DMAB_CLS'
      and code.code_id = afn.DMAB_CLS
    limit 1
)
 as DMAB_CLS_TXT,
    afn.PO_PE_CLS,
    
(
    select code_nm
    from "postgres"."igisdms"."mdbmcodef" code
    where code.code_grp_id = 'PO_PE_CLS'
      and code.code_id = afn.PO_PE_CLS
    limit 1
)
 as PO_PE_CLS_TXT,
    afn.MOJA_CLS,
    
(
    select code_nm
    from "postgres"."igisdms"."mdbmcodef" code
    where code.code_grp_id = 'MOJA_CLS'
      and code.code_id = afn.MOJA_CLS
    limit 1
)
 as MOJA_CLS_TXT,
    afn.MULTI_CLAS_CLS,
    
(
    select code_nm
    from "postgres"."igisdms"."mdbmcodef" code
    where code.code_grp_id = 'MULTI_CLAS_CLS'
      and code.code_id = afn.MULTI_CLAS_CLS
    limit 1
)
 as MULTI_CLAS_CLS_TXT,

    -- Fund structure
    afn.FUND_STRR,
    
(
    select code_nm
    from "postgres"."igisdms"."mdbmcodef" code
    where code.code_grp_id = 'FUND_STRR'
      and code.code_id = afn.FUND_STRR
    limit 1
)
 as FUND_STRR_TXT,
    afn.LOAN_INVT_TYPE,
    
(
    select code_nm
    from "postgres"."igisdms"."mdbmcodef" code
    where code.code_grp_id = 'LOAN_INVT_TYPE'
      and code.code_id = afn.LOAN_INVT_TYPE
    limit 1
)
 as LOAN_INVT_TYPE_TXT,

    -- Service providers
    afn.AGCO,
    
(
    select code_nm
    from "postgres"."igisdms"."mdbmcodef" code
    where code.code_grp_id = 'OTHR_AGCO_CLS'
      and code.code_id = afn.AGCO
    limit 1
)
 as AGCO_TXT,
    afn.CNCO,
    
(
    select code_nm
    from "postgres"."igisdms"."mdbmcodef" code
    where code.code_grp_id = 'CNCO_CODE'
      and code.code_id = afn.CNCO
    limit 1
)
 as CNCO_NM,

    -- Key dates
    afn.CNRC_STTL_DT,
    afn.FST_ESDT,
    afn.MNGT_BGDT,
    afn.MTRT_PRDT,
    afn.MNGT_EXDT,
    afn.FUND_LQDT,

    -- Status flags
    afn.FUND_CANC_YN,
    afn.THCO_FUND_FOF_INCU_YN,
    afn.SD_YN,
    afn.SD_STTL_DT,
    afn.CPCL_MNNR_YN,

    -- Management details
    afn.INVT_INCR_EMPL_NO,
    afn.INVT_PTPR_EMPL_NO,
    afn.MANG_INCR_EMPL_NO,
    afn.MANG_PTPR_EMPL_NO,
    afn.MANG_DEPT_CD,

    -- Compliance and codes
    afn.ACNG_AUDT_YN,
    afn.FSS_ISCD,
    afn.KOFIA_ISCD,
    afn.KSD_FUND_CD,
    afn.KSD_ISCD,

    -- Fund classification depth
    afn.ACNG_PRD,
    afn.FUND_1_DIV, afn.FUND_2_DIV, afn.FUND_3_DIV, afn.FUND_4_DIV,
    afn.FUND_5_DIV, afn.FUND_6_DIV, afn.FUND_7_DIV, afn.FUND_8_DIV,
    afn.FUND_9_DIV, afn.FUND_10_DIV, afn.FUND_11_DIV, afn.FUND_12_DIV,
    afn.FUND_13_DIV, afn.FUND_14_DIV,

    -- Risk, currency, and flags
    afn.STND_CRNC,
    afn.FUND_RISK_GRAD,
    afn.CPLX_FUND_YN,
    afn.FX_HEDG_MTHD,
    coalesce(afn.IPUT_NTRGT, 'N') as IPUT_NTRGT,
    coalesce(afn.KMS_TRGT_YN, 'N') as KMS_TRGT_YN,
    coalesce(afn.AUTO_TRGT_YN, 'N') as AUTO_TRGT_YN,
    coalesce(afn.USE_YN, 'N') as USE_YN,
    coalesce(afn.INFO_SECU_YN, 'N') as INFO_SECU_YN,
    coalesce(afn.OVRS_FUND_YN, 'N') as OVRS_FUND_YN,
    afn.UNUL_MATR,
    case 
        when mtfn."FUND_CD" is not null then 'Y'
        else 'N'
    end as FRN_CCY_YN,

    -- FN-sourced fields
    fn.NEXT_FEE_WITHDRAW_DT_1 as NEXT_FEE_WITHDRAW_DT,
    fn.PRE_FEE_WITHDRAW_DT_1 as PRE_FEE_WITHDRAW_DT,
    fn.NEXT_STTL_DT,
    fn.PRE_STTL_DT,
    fn.STTL_PRD

from afn
left join fn
    on afn.FUND_CD = fn.FUND_CD
left join "postgres"."igisam"."mtfn" mtfn
    on afn.FUND_CD = mtfn."FUND_CD"
  );
  