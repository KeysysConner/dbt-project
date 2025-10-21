{{
  config(
    materialized='incremental',
    schema='SILVER',
    incremental_strategy='append'
  )
}}

-- This block runs before the main query --
{% if is_incremental() %}
  {% set max_loaded_at_query %}
    select max(loaded_at) from {{ this }}
  {% endset %}
  {% set max_loaded_at = run_query(max_loaded_at_query).columns[0].values()[0] %}
{% endif %}

with source_data as (
    select * from {{ ref('stg_hospital_enrollments') }}
),

final as (
    select
        -- Primary Keys
        enrollment_id,
        npi,
        ccn,
        associate_id,
        cah_or_hospital_ccn,

        -- Foreign Key
        provider_type_code,

        -- Org Details
        organization_name,
        doing_business_as_name,
        organization_type_structure,
        organization_other_type_text,
        is_proprietary,
        incorporation_date,
        incorporation_state,
        
        -- Location Details
        address_line_1,
        address_line_2,
        city,
        state,
        zip_code,
        enrollment_state,
        practice_location_type,
        location_other_type_text,

        -- Other Flags
        is_multiple_npi,
        is_reh_conversion,
        reh_conversion_date,

        -- Metadata for tracking
        data_source_id,
        loaded_at

    from
        source_data

    -- This clause is now at the end and uses the Jinja variable
    -- It will compile to: where loaded_at > '2025-10-21 16:30:00.000'
    {% if is_incremental() %}
    where loaded_at > '{{ max_loaded_at }}'
    {% endif %}
)

select * from final