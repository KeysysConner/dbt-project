{{
  config(
    materialized='incremental',
    schema='SILVER',
    incremental_strategy='append'
  )
}}

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
    {{ ref('stg_hospital_enrollments') }}

{% if is_incremental() %}

  -- This creates a variable holding the max timestamp from the table *before* the run
  {% set max_loaded_at = "(select max(loaded_at) from {{ this }})" %}

  -- This filters the new data to only append rows newer than that timestamp
  where loaded_at > {{ max_loaded_at }}

{% endif %}