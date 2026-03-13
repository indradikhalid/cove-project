with rooms_availability as (
    select
        r._id as room_id,
        r.propertyId as property_id,
        p.name as property_name,
        p.lease_start_date,
        least(
            p.lease_end_date,
            coalesce(date(r.deletedAt), p.lease_end_date),
            coalesce(date(p.deletedAt), p.lease_end_date)
        ) as available_until_date
    from {{ source('cove_staging', 'rooms') }} r
    join {{ source('cove_staging', 'properties') }} p on r.propertyId = p._id
),

date_array as (
    select d
    from unnest(
        generate_date_array(
            (select min(cast(lease_start_date as date)) from {{ source('cove_staging', 'properties') }}),
            (select max(cast(lease_end_date as date)) from {{ source('cove_staging', 'properties') }}),
            interval 1 day
        )
    ) as d
),

room_date_availability as (
    select
        ra.room_id,
        ra.property_id,
        ra.property_name,
        da.d as date
    from rooms_availability ra
    cross join date_array da
    where da.d >= ra.lease_start_date
      and da.d < ra.available_until_date
),

tenancies as (
    select
        _id as tenancy_id,
        roomId as room_id,
        checkInDate as check_in_date,
        checkOutDate as check_out_date
    from {{ source('cove_staging', 'tenancies') }}
    where status != 'cancelled'
),

room_date_occupancy as (
    select
        rd.property_id,
        rd.property_name,
        rd.room_id,
        rd.date,
        max(case when t.tenancy_id is not null then 1 else 0 end) as is_occupied
    from room_date_availability rd
    left join tenancies t
        on rd.room_id = t.room_id
        and rd.date >= t.check_in_date
        and rd.date < t.check_out_date
    group by 1, 2, 3, 4
)

select
    property_id,
    property_name,
    date_trunc(date, month) as month,
    count(*) as total_available_room_nights,
    sum(is_occupied) as occupied_room_nights,
    round(safe_divide(sum(is_occupied), count(*)) * 100, 2) as occupancy_rate_pct
from room_date_occupancy
group by 1, 2, 3
order by property_id, month
