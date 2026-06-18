-- Pipeline health metrics derived from the clean leads mart
-- Raw layer is append-only with full history — stats here reflect deduplicated leads only

with leads as (
    select * from {{ ref('leads') }}
),

business_type_breakdown as (
    select
        object_agg(business_type, total_leads::variant) as leads_by_business_type
    from (
        select business_type, count(*) as total_leads
        from leads
        group by business_type
    )
),

stats as (
    select
        -- Volume
        count(*)                                            as total_leads,

        -- Phone coverage
        count(phone_clean)                                  as leads_with_phone,
        count(*) - count(phone_clean)                       as leads_without_phone,
        round(count(phone_clean) / count(*) * 100, 1)      as phone_coverage_pct,

        -- Quality
        round(avg(rating), 2)                               as avg_rating,

        -- Geography
        count(distinct area)                                as areas_covered,

        -- Business types
        count(distinct business_type)                       as business_types_count,

        -- Pipeline run
        max(ingested_at)                                    as last_pipeline_run

    from leads
)

select
    s.*,
    b.leads_by_business_type
from stats s
cross join business_type_breakdown b
