with stg as (
    select * from {{ ref('stg_places') }}
),

-- Deduplicate by place_id — keep the latest ingested record
-- Overlapping H3 hexagons cause the same place to appear multiple times
deduped as (
    select *
    from stg
    qualify row_number() over (
        partition by place_id
        order by ingested_at desc
    ) = 1
),

-- Filter out permanently closed businesses — not worth reaching out to
-- Keep OPERATIONAL and CLOSED_TEMPORARILY (may reopen)
filtered as (
    select *
    from deduped
    where business_status != 'PERMANENTLY_CLOSED'
)

select
    *,
    -- has_phone excludes toll-free IVR numbers (1800/1860) — not callable by sales team
    case
        when phone_clean is not null
         and replace(phone_clean, '+91', '') not like '1800%'
         and replace(phone_clean, '+91', '') not like '1860%'
        then true else false
    end as has_phone,

    -- Priority tier — call order for sales team
    -- Anchored to data percentiles: p50 rating=4.2, p50 reviews=51, p25 reviews=6
    -- Requires minimum 10 reviews before trusting any rating (noise floor)
    case
        -- No signal: null rating or below noise floor (<10 reviews)
        when rating is null or review_count < 10              then 'LOW'
        -- HIGH: above median rating (p50=4.2) AND established (>p60 reviews=300)
        when rating >= 4.2 and review_count >= 300            then 'HIGH'
        -- MEDIUM: good rating with minimum evidence
        when rating >= 4.0 and review_count >= 10             then 'MEDIUM'
        -- MEDIUM: established business (>=p50 reviews) with decent rating
        when rating >= 3.5 and review_count >= 50             then 'MEDIUM'
        else                                                       'LOW'
    end as priority_tier

from filtered
