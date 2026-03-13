{{ config(materialized='table') }}

with towers as (
    select *
    from read_csv_auto('/home/waly/tower-health-stream/data/towers_clean.csv')
),

operator_stats as (
    select
        operator,
        count(*)                                         as total_towers,
        sum(case when radio = 'LTE'  then 1 else 0 end) as lte_towers,
        sum(case when radio = 'UMTS' then 1 else 0 end) as umts_towers,
        sum(case when radio = 'GSM'  then 1 else 0 end) as gsm_towers,
        round(
            sum(case when radio = 'LTE' then 1 else 0 end) * 100.0 / count(*), 2
        )                                                as lte_pct,
        round(
            sum(case when radio = 'UMTS' then 1 else 0 end) * 100.0 / count(*), 2
        )                                                as umts_pct,
        round(
            sum(case when radio = 'GSM' then 1 else 0 end) * 100.0 / count(*), 2
        )                                                as gsm_pct,
        count(distinct area)                             as areas_covered
    from towers
    group by operator
),

ranked as (
    select
        operator,
        total_towers,
        lte_towers,
        umts_towers,
        gsm_towers,
        lte_pct,
        umts_pct,
        gsm_pct,
        areas_covered,
        rank() over (order by lte_pct desc) as performance_rank
    from operator_stats
)

select *
from ranked
order by performance_rank