with leads as (
    select * from {{ ref('leads') }}
)

select
    area,
    business_type,
    count(*)                                                as total_leads,
    count(phone_clean)                                      as leads_with_phone,
    count(*) - count(phone_clean)                           as leads_without_phone,
    round(count(phone_clean) / count(*) * 100, 1)          as phone_coverage_pct,
    round(avg(rating), 2)                                   as avg_rating,
    round(avg(review_count), 0)                             as avg_review_count

from leads
group by area, business_type
order by total_leads desc
