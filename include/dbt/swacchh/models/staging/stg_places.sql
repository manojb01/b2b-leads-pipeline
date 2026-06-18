with source as (
    select * from {{ source('raw', 'places') }}
),

flattened as (
    select
        -- Identity
        raw_data:id::varchar                                as place_id,
        raw_data:displayName:text::varchar                  as name,

        -- Address fields
        -- Pattern: <street details>, <area>, <city>, <state> <pincode>, India
        -- Counting from the right: -1=India, -2=state+pin, -3=city, -4=area
        raw_data:formattedAddress::varchar                  as address,
        trim(split_part(raw_data:formattedAddress::varchar, ',', -4))
                                                            as _area_raw,
        trim(split_part(raw_data:formattedAddress::varchar, ',', -3))
                                                            as city,
        trim(split_part(
            split_part(raw_data:formattedAddress::varchar, ',', -2),
            ' ', 1
        ))                                                  as state,
        regexp_substr(
            raw_data:formattedAddress::varchar, '[0-9]{6}'
        )                                                   as pincode,

        -- Contact
        raw_data:internationalPhoneNumber::varchar          as phone,
        replace(
            raw_data:internationalPhoneNumber::varchar, ' ', ''
        )                                                   as phone_clean,
        raw_data:websiteUri::varchar                        as website,
        raw_data:googleMapsUri::varchar                     as google_maps_url,

        -- Ratings
        raw_data:rating::float                              as rating,
        raw_data:userRatingCount::int                       as review_count,

        -- Status
        raw_data:businessStatus::varchar                    as business_status,

        -- Location
        raw_data:location:latitude::float                   as latitude,
        raw_data:location:longitude::float                  as longitude,

        -- Search metadata
        raw_data:_business_type::varchar                    as business_type,
        raw_data:_search_lat::float                         as search_lat,
        raw_data:_search_lng::float                         as search_lng,

        -- Audit
        ingested_at

    from source
),

final as (
    select
        place_id,
        name,

        -- Fallback: if the 4th-from-right segment is a generic street descriptor,
        -- step back one more position to get the actual neighbourhood name.
        -- Happens when Google Places encodes sub-locality types (Line, Road, Lane)
        -- as the penultimate address component instead of the area name.
        case
            when _area_raw in ('Line', 'Road', 'Lane', 'Main', 'Cross', 'Street', 'Extension')
            then trim(split_part(address, ',', -5))
            else _area_raw
        end                                                 as area,

        city,
        state,
        pincode,
        address,
        phone,
        phone_clean,
        website,
        google_maps_url,
        rating,
        review_count,
        business_status,
        latitude,
        longitude,
        business_type,
        search_lat,
        search_lng,
        ingested_at

    from flattened
)

select * from final
