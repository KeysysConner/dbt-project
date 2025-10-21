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

  -- This filter appends only new records based on the raw file load time
  where loaded_at > (select max(loaded_at) from {{ this }})

{% endif %}