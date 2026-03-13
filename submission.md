# Submission: Monthly Occupancy Rate by Property

## Approach

ETL Steps:

1. **Extract & Load** -- Python script (`load_to_bq.py`) loads the three JSONL files into BigQuery `cove_staging` dataset as-is, preserving the original column names and structure from the MongoDB export.
2. **Transform** -- dbt project (`dbt_cove/`) reads directly from the `cove_staging` source tables and builds a single mart table (`fact_monthly_occupancy`) that calculates monthly occupancy rate per property. All transformation logic (type casting, filtering cancelled tenancies, occupancy calculation) lives in this mart model.
3. **Visualize** -- Looker Studio connected to `cove_mart.fact_monthly_occupancy`, line chart showing occupancy rate over time per property.

## Assumptions

- Check-out date is **exclusive** (guest's last night is the day before check-out), consistent with nightly billing.
- Lease end date is **inclusive** (last available night).
- `deletedAt` is **exclusive** -- if a room or property has `deletedAt: 2025-12-01`, it is no longer available from that date onward. The last available night is the day before.
- Soft-deleted rooms/properties are still included for historical periods before deletion.
- All tenancy dates are assumed to be in the same timezone (UTC).

## Data Quality Issues Noticed

1. **Cancelled tenancy** -- `t_015` (room r_302) has `status: "cancelled"`. Excluded from occupancy calculation in the mart model.
2. **Overlapping tenancies** -- `t_010` (r_202, Apr 1 - Jul 1) and `t_011` (r_202, Jun 25 - Dec 1) overlap by 6 days. Handled by deduplication (room-night counted as occupied once).
3. **Soft-deleted room** -- `r_201` has `deletedAt: 2025-12-31`. Room availability is capped at the deletion date.
4. **Soft-deleted property** -- `p_003` (Cove Joo Chiat) has `deletedAt: 2025-12-01`. All rooms in this property are only available until that date, even though the lease runs until 2026-12-31.
5. **Inconsistent deletedAt** -- `p_002` has `"deletedAt": null` explicitly, while `p_001` has no `deletedAt` field at all. Both are treated as active (no deletion).


## Occupancy Rate Logic

```
occupancy_rate = occupied_room_nights / total_available_room_nights * 100
```

- A **room-night** is one room available for one night.
- A room is **available** on a given date if the date falls within the property's lease period (`lease_start_date` to `lease_end_date`), capped by soft-deletion dates of the room or property (whichever is earliest).
- A room-night is **occupied** if any active (non-cancelled) tenancy covers that date (`checkInDate <= date < checkOutDate`). Check-out date is exclusive (standard nightly billing).
- Overlapping tenancies on the same room are deduplicated -- a room-night is either occupied or not, regardless of how many tenancies overlap.


## Setup & Run

### Prerequisites
- GCP project with BigQuery enabled
- `gcloud` CLI authenticated (`gcloud auth application-default login`)
- Python 3.8+ with `google-cloud-bigquery`
- dbt-bigquery (`pip install dbt-bigquery`)

### Steps

```bash
# 1. Load raw data into BigQuery (cove_staging dataset)
pip install google-cloud-bigquery
python load_to_bq.py

# 2. Set up dbt profile
mkdir -p ~/.dbt
cp dbt_cove/profiles.yml.example ~/.dbt/profiles.yml

# 3. Run dbt (creates cove_mart.fact_monthly_occupancy)
cd dbt_cove/
dbt run