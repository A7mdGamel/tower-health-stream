{{ config(materialized='table') }}

with events as (
    select *
    from read_csv_auto('/home/waly/tower-health-stream/data/towers_clean.csv')
),

scored as (
    select
        tower_id,
        operator,
        radio,
        lat,
        lon,
        area,
        case
            when radio = 'LTE'  then 4
            when radio = 'UMTS' then 3
            when radio = 'GSM'  then 2
            else 1
        end as radio_score,
        case operator
            when 'Vodafone' then 1
            when 'e&'       then 2
            when 'Orange'   then 3
            when 'WE'       then 4
        end as operator_rank
    from events
)

select
    tower_id,
    operator,
    radio,
    lat,
    lon,
    area,
    radio_score,
    operator_rank,
    round(radio_score * 25.0, 2) as health_score
from scored
order by health_score desc