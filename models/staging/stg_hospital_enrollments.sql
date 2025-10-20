{{
  config(
    materialized='view'
  )
}}

with source as (

    select * from {{ source('raw_staging', 'POC_RAW_DATA') }}
    -- This filter ensures you are only processing hospital enrollment files
    where lower(file_name) like '%hospital_enrollments%'

),

renamed_and_casted as (

    select
        -- Identifiers
        raw_content:"ENROLLMENT ID"::string as enrollment_id,
        raw_content:"NPI"::string as npi,
        raw_content:"CCN"::string as ccn,
        raw_content:"ASSOCIATE ID"::string as associate_id,
        raw_content:"CAH OR HOSPITAL CCN"::string as cah_or_hospital_ccn,

        -- Provider Type
        raw_content:"PROVIDER TYPE CODE"::string as provider_type_code,
        raw_content:"PROVIDER TYPE TEXT"::string as provider_type_text,

        -- Organization Details
        raw_content:"ORGANIZATION NAME"::string as organization_name,
        raw_content:"DOING BUSINESS AS NAME"::string as doing_business_as_name,
        raw_content:"ORGANIZATION TYPE STRUCTURE"::string as organization_type_structure,
        raw_content:"ORGANIZATION OTHER TYPE TEXT"::string as organization_other_type_text,
        (raw_content:"PROPRIETARY NONPROFIT"::string = 'P') as is_proprietary,
        try_to_date(raw_content:"INCORPORATION DATE"::string, 'MM/DD/YYYY') as incorporation_date,
        raw_content:"INCORPORATION STATE"::string as incorporation_state,
        
        -- Location Details
        raw_content:"ADDRESS LINE 1"::string as address_line_1,
        raw_content:"ADDRESS LINE 2"::string as address_line_2,
        raw_content:"CITY"::string as city,
        raw_content:"STATE"::string as state,
        raw_content:"ZIP CODE"::string as zip_code,
        raw_content:"ENROLLMENT STATE"::string as enrollment_state,
        raw_content:"PRACTICE LOCATION TYPE"::string as practice_location_type,
        raw_content:"LOCATION OTHER TYPE TEXT"::string as location_other_type_text,

        -- Flags
        (raw_content:"MULTIPLE NPI FLAG"::string = 'Y') as is_multiple_npi,
        (raw_content:"REH CONVERSION FLAG"::string = 'Y') as is_reh_conversion,
        try_to_date(raw_content:"REH CONVERSION DATE"::string, 'MM/DD/YYYY') as reh_conversion_date,

        -- Subgroup Flags (to be unpivoted later)
        (raw_content:"SUBGROUP - GENERAL"::string = 'Y') as is_subgroup_general,
        (raw_content:"SUBGROUP - ACUTE CARE"::string = 'Y') as is_subgroup_acute_care,
        (raw_content:"SUBGROUP - ALCOHOL DRUG"::string = 'Y') as is_subgroup_alcohol_drug,
        (raw_content:"SUBGROUP - CHILDRENS"::string = 'Y') as is_subgroup_childrens,
        (raw_content:"SUBGROUP - LONG-TERM"::string = 'Y') as is_subgroup_long_term,
        (raw_content:"SUBGROUP - PSYCHIATRIC"::string = 'Y') as is_subgroup_psychiatric,
        (raw_content:"SUBGROUP - REHABILITATION"::string = 'Y') as is_subgroup_rehabilitation,
        (raw_content:"SUBGROUP - SHORT-TERM"::string = 'Y') as is_subgroup_short_term,
        (raw_content:"SUBGROUP - SWING-BED APPROVED"::string = 'Y') as is_subgroup_swing_bed_approved,
        (raw_content:"SUBGROUP - PSYCHIATRIC UNIT"::string = 'Y') as is_subgroup_psychiatric_unit,
        (raw_content:"SUBGROUP - REHABILITATION UNIT"::string = 'Y') as is_subgroup_rehabilitation_unit,
        (raw_content:"SUBGROUP - SPECIALTY HOSPITAL"::string = 'Y') as is_subgroup_specialty_hospital,
        (raw_content:"SUBGROUP - OTHER"::string = 'Y') as is_subgroup_other,
        raw_content:"SUBGROUP - OTHER TEXT"::string as subgroup_other_text,

        -- Metadata
        file_name,
        loaded_at

    from source

)

select * from renamed_and_casted