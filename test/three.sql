INSERT INTO ads.ads_secu_finmgt_share_change (
        data_id,
        task_id,
        publish_date,
        start_date,
        stat_period,
        end_date,
        secu_finmgt_code,
        info_source,
        final_share,
        final_share_merge,
        init_share,
        cur_purchase,
        cur_redeem
        )
WITH parsed_base AS (
        SELECT
                t1.task_id,
        t1.data_id,
        t1.issuer,
        t1.publish_date,
        t1.finmgt_product_code,
        CASE
                WHEN report_type in ('一季报', '1季报')  THEN CONCAT(substring(fiscal_year,1,4),'-01-01')
WHEN report_type in ('二季报', '2季报') THEN CONCAT(substring(fiscal_year,1,4),'-04-01')
WHEN report_type in ('三季报', '3季报') THEN CONCAT(substring(fiscal_year,1,4),'-07-01')
WHEN report_type in ('四季报', '4季报') THEN CONCAT(substring(fiscal_year,1,4),'-10-01')
WHEN report_type in ('中期报告', '半年度报告') THEN CONCAT(substring(fiscal_year,1,4),'-01-01')
WHEN report_type = '年度报告' THEN CONCAT(substring(fiscal_year,1,4),'-01-01')
END AS start_date,
CASE
WHEN report_type in ('一季报', '1季报') THEN 3
WHEN report_type in ('二季报', '2季报') THEN 3
WHEN report_type in ('三季报', '3季报') THEN 3
WHEN report_type in ('四季报', '4季报') THEN 3
WHEN report_type in ('中期报告', '半年度报告') THEN 6
WHEN report_type = '年度报告' THEN 12
END AS stat_period,
CASE
WHEN report_type in ('一季报', '1季报') THEN CONCAT(substring(fiscal_year,1,4),'-03-31')
WHEN report_type in ('二季报', '2季报') THEN CONCAT(substring(fiscal_year,1,4),'-06-30')
WHEN report_type in ('三季报', '3季报') THEN CONCAT(substring(fiscal_year,1,4),'-09-30')
WHEN report_type in ('四季报', '4季报') THEN CONCAT(substring(fiscal_year,1,4),'-12-31')
WHEN report_type in ('中期报告', '半年度报告') THEN CONCAT(substring(fiscal_year,1,4),'-06-30')
WHEN report_type = '年度报告' THEN CONCAT(substring(fiscal_year,1,4),'-12-31')
END AS end_date,
t2.share_flag,
t2.share_code,
t2.share_net_value,
t2.share_accumulated_net_value,
t2.per_10k_yield,
t2.`7d_annual_yield_rate`,
t2.asset_net_value,
t2.total_shares,
        '定期报告' as info_source
FROM (
        SELECT
                task_id,
        data_id,
        issuer,
        pushtime as publish_date,
        lccpdm as finmgt_product_code,
        bglx as report_type,
        bgnd as fiscal_year
                FROM stg.stg_dc_dc_wr_zoom000005_hxlcdb_python_extract1_real
                WHERE task_id=@@TASK_ID
                AND issuer= '信银理财有限责任公司'
)t1
LEFT JOIN (
        SELECT
                data_id,
        MAX(CASE WHEN column_name = '份额标识' THEN json_raw_data END) AS share_flag,
MAX(CASE WHEN column_name = '份额代码' THEN json_raw_data END) AS share_code,
MAX(CASE WHEN column_name = '份额净值' THEN CAST(json_raw_data as decimal(18,9)) END) AS share_net_value,
MAX(CASE WHEN column_name = '份额累计净值' THEN CAST(json_raw_data as decimal(18,9)) END) AS share_accumulated_net_value,
MAX(CASE WHEN column_name = '每万份收益' THEN CAST(json_raw_data as decimal(18,9)) END) AS per_10k_yield,
MAX(CASE WHEN column_name = '七日年化收益率' THEN CAST(json_raw_data as decimal(18,9))/100 END) AS `7d_annual_yield_rate`,
MAX(CASE WHEN column_name = '资产净值' THEN CAST(json_raw_data as decimal(18,4)) END) AS asset_net_value,
MAX(CASE WHEN column_name = '份额份数' THEN CAST(json_raw_data AS decimal(19,2)) END) AS total_shares
FROM (
        SELECT
                data_id,
        row_idx,
        json_extract_string(
                json_parse(json_extract_string(jz_and_fe,'$.raw_data')),
CONCAT('$[',row_idx,'][',column_tab.column_idx,']')) as json_raw_data,
json_extract_string(
        jz_and_fe,
        CONCAT('$.table_head[',column_tab.column_idx,']')) as column_name
FROM (
        SELECT
                data_id,
        jz_and_fe
                FROM stg.stg_dc_dc_wr_zoom000005_hxlcdb_python_extract1_real
                WHERE task_id=@@TASK_ID
                AND issuer= '信银理财有限责任公司'
)t_temp
LATERAL VIEW explode_numbers(json_length(json_parse(json_extract_string(t_temp.jz_and_fe,'$.raw_data')))) row_tab as row_idx
LATERAL VIEW explode_numbers(json_length(json_extract(t_temp.jz_and_fe, '$.table_head'))) column_tab as column_idx
             )t
GROUP BY data_id,row_idx
    )t2 ON t2.data_id = t1.data_id
)
SELECT DISTINCT
data_id,
task_id,
publish_date,
start_date,
stat_period,
end_date,
secu_finmgt_code,
info_source,
CASE
WHEN share_type = 1
THEN total_shares
WHEN share_type = 2 AND share_flag <> ''
THEN total_shares
WHEN share_type = 3
THEN MAX(CASE WHEN share_flag <> '' THEN total_shares END) OVER (PARTITION BY data_id, share_code)
ELSE NULL
END AS final_share,
CASE
WHEN share_type = 1
THEN NULL
WHEN share_type = 2 AND share_flag = ''
THEN total_shares
WHEN share_type = 3
THEN MAX(CASE WHEN share_flag = '' THEN total_shares END) OVER (PARTITION BY data_id, share_code)
ELSE NULL
END AS final_share_merge,
NULL AS init_share,
NULL AS cur_purchase,
NULL AS cur_redeem
FROM (
                SELECT
                        pb.task_id,
                pb.data_id,
                pb.issuer,
                pb.publish_date,
                pb.finmgt_product_code,
                pb.start_date,
                pb.stat_period,
                pb.end_date,
                pb.share_flag,
                pb.share_code,
                pb.share_net_value,
                pb.share_accumulated_net_value,
                pb.per_10k_yield,
                pb.`7d_annual_yield_rate`,
                pb.asset_net_value,
                pb.total_shares,
                pb.info_source,
                t3.secu_finmgt_code,
                CASE
                        WHEN ss.cnt_total = 1 THEN 1
                        WHEN ss.cnt_total = ss.cnt_distinct_share THEN 2
                        ELSE 3
                        END AS share_type
                        FROM parsed_base pb
                        LEFT JOIN (
                        SELECT
                        INBBM as secu_finmgt_code,
                CPDM
                        FROM (
                        SELECT
                        CPDM,
                INBBM,
                row_number() OVER (
PARTITION BY INBBM
ORDER BY XGSJ DESC
                ) as rn
FROM stg.stg_jyprime_usrlccpjbxxb
WHERE FXJG in (3680,53004328)
                  ) t3_temp
WHERE rn = 1
         )t3 ON pb.share_code = t3.CPDM
LEFT JOIN (
        SELECT
                data_id,
        COUNT(*) AS cnt_total,
COUNT(DISTINCT share_code) AS cnt_distinct_share
FROM parsed_base
GROUP BY data_id
         ) ss ON pb.data_id = ss.data_id
     )t4;