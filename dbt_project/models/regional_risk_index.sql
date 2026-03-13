{{ config(materialized='table') }}

with towers as (
    select *
    from read_csv_auto('/home/waly/tower-health-stream/data/towers_clean.csv')
),

area_stats as (
    select
        area,
        count(*)                                    as total_towers,
        sum(case when radio = 'LTE'  then 1 else 0 end) as lte_count,
        sum(case when radio = 'UMTS' then 1 else 0 end) as umts_count,
        sum(case when radio = 'GSM'  then 1 else 0 end) as gsm_count,
        round(
            sum(case when radio = 'LTE' then 1 else 0 end) * 100.0 / count(*), 2
        )                                           as lte_coverage_pct
    from towers
    group by area
),

risk_scored as (
    select
        area,
        total_towers,
        lte_count,
        umts_count,
        gsm_count,
        lte_coverage_pct,
        case
            when lte_coverage_pct >= 70 then 'LOW'
            when lte_coverage_pct >= 40 then 'MEDIUM'
            else 'HIGH'
        end as risk_level,
        round(100 - lte_coverage_pct, 2) as risk_score
    from area_stats
)

select *
from risk_scored
order by risk_score desc