with afn as (
    select * from {{ source('fund_master_igisbiz', 'afndbascm') }}
),
fn as (
    select * from {{ source('fund_master_igisam', 'fn') }}
),
mtfn as (
    select distinct "FUND_CD" from {{ source('fund_master_igisam', 'mtfn') }}
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
    {{ code_name_joined('MNGT_STAT', 'afn.MNGT_STAT') }} as MNGT_STAT_TXT,
    coalesce(afn.BLND_YN, 'N') as BLND_YN,
    afn.FUND_CLS,
    {{ code_name_joined('FUND_CLS', 'afn.FUND_CLS') }} as FUND_CLS_TXT,
    afn.FUND_KICD,
    {{ code_name_joined('AUREM_FUND_KICD', 'afn.FUND_KICD') }} as FUND_KICD_TXT,
    afn.INVT_TYPE,
    {{ code_name_joined('INVT_TYPE', 'afn.INVT_TYPE') }} as INVT_TYPE_TXT,
    afn.INVT_ASET_TYPE,
    {{ code_name_joined('INVT_ASET_TYPE', 'afn.INVT_ASET_TYPE') }} as INVT_ASET_TYPE_TXT,
    afn.INVT_TRGT_LCTN_AREA,
    {{ code_name_joined('INVT_TRGT_LCTN_AREA', 'afn.INVT_TRGT_LCTN_AREA') }} as INVT_TRGT_LCTN_AREA_TXT,
    afn.INVT_STTG,
    {{ code_name_joined('INVT_STTG', 'afn.INVT_STTG') }} as INVT_STTG_TXT,

    -- Investment characteristics
    coalesce(afn.DVLP_YN, 'N') as DVLP_YN,
    afn.INVT_TRGT_ASET,
    {{ code_name_joined('AUREM_INVT_TRGT_ASET', 'afn.INVT_TRGT_ASET')}} as INVT_TRGT_ASET_TXT,
    afn.DMAB_CLS,
    {{ code_name_joined('DMAB_CLS', 'afn.DMAB_CLS') }} as DMAB_CLS_TXT,
    afn.PO_PE_CLS,
    {{ code_name_joined('PO_PE_CLS', 'afn.PO_PE_CLS') }} as PO_PE_CLS_TXT,
    afn.MOJA_CLS,
    {{ code_name_joined('MOJA_CLS', 'afn.MOJA_CLS') }} as MOJA_CLS_TXT,
    afn.MULTI_CLAS_CLS,
    {{ code_name_joined('MULTI_CLAS_CLS', 'afn.MULTI_CLAS_CLS') }} as MULTI_CLAS_CLS_TXT,

    -- Fund structure
    afn.FUND_STRR,
    {{ code_name_joined('FUND_STRR', 'afn.FUND_STRR') }} as FUND_STRR_TXT,
    afn.LOAN_INVT_TYPE,
    {{ code_name_joined('LOAN_INVT_TYPE', 'afn.LOAN_INVT_TYPE') }} as LOAN_INVT_TYPE_TXT,

    -- Service providers
    afn.AGCO,
    {{ code_name_joined('OTHR_AGCO_CLS', 'afn.AGCO') }} as AGCO_TXT,
    afn.CNCO,
    {{ code_name_joined('CNCO_CODE', 'afn.CNCO') }} as CNCO_NM,

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
left join {{ source('fund_master_igisam', 'mtfn') }} mtfn
    on afn.FUND_CD = mtfn."FUND_CD"