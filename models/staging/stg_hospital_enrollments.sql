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

-- STEP 1: Safely parse the JSON using TRY_PARSE_JSON
-- This will return NULL if the JSON is malformed (e.g., "multiple documents")
safely_parsed as (

    select
        TRY_PARSE_JSON(raw_content) as parsed_content,
        file_name,
        loaded_at
    from source
),

-- STEP 2: Filter out rows that failed parsing
-- and then extract fields from the valid JSON.
renamed_and_casted as (

    select
        -- Identifiers
        parsed_content:"ENROLLMENT ID"::string as enrollment_id,
        parsed_content:"NPI"::string as npi,
        parsed_content:"CCN"::string as ccn,
        parsed_content:"ASSOCIATE ID"::string as associate_id,
        parsed_content:"CAH OR HOSPITAL CCN"::string as cah_or_hospital_ccn,

        -- Provider Type
        parsed_content:"PROVIDER TYPE CODE"::string as provider_type_code,
        parsed_content:"PROVIDER TYPE TEXT"::string as provider_type_text,

        -- Organization Details
        parsed_content:"ORGANIZATION NAME"::string as organization_name,
        parsed_content:"DOING BUSINESS AS NAME"::string as doing_business_as_name,
        parsed_content:"ORGANIZATION TYPE STRUCTURE"::string as organization_type_structure,
        parsed_content:"ORGANIZATION OTHER TYPE TEXT"::string as organization_other_type_text,
        (parsed_content:"PROPRIETARY NONPROFIT"::string = 'P') as is_proprietary,
        try_to_date(parsed_content:"INCORPORATION DATE"::string, 'MM/DD/YYYY') as incorporation_date,
        parsed_content:"INCORPORATION STATE"::string as incorporation_state,
        
        -- Location Details
        parsed_content:"ADDRESS LINE 1"::string as address_line_1,
        parsed_content:"ADDRESS LINE 2"::string as address_line_2,
        parsed_content:"CITY"::string as city,
        parsed_content:"STATE"::string as state,
        parsed_content:"ZIP CODE"::string as zip_code,
        parsed_content:"ENROLLMENT STATE"::string as enrollment_state,
        parsed_content:"PRACTICE LOCATION TYPE"::string as practice_location_type,
        parsed_content:"LOCATION OTHER TYPE TEXT"::string as location_other_type_text,

        -- Flags
        (parsed_content:"MULTIPLE NPI FLAG"::string = 'Y') as is_multiple_npi,
        (parsed_content:"REH CONVERSION FLAG"::string = 'Y') as is_reh_conversion,
        try_to_date(parsed_content:"REH CONVERSION DATE"::string, 'MM/DD/YYYY') as reh_conversion_date,

        -- Subgroup Flags (to be unpivoted later)
        (parsed_content:"SUBGROUP - GENERAL"::string = 'Y') as is_subgroup_general,
        (parsed_content:"SUBGROUP - ACUTE CARE"::string = 'Y') as is_subgroup_acute_care,
        (parsed_content:"SUBGROUP - ALCOHOL DRUG"::string = 'Y') as is_subgroup_alcohol_drug,
        (parsed_content:"SUBGROUP - CHILDRENS"::string = 'Y') as is_subgroup_childrens,
        (parsed_content:"SUBGROUP - LONG-TERM"::string = 'Y') as is_subgroup_long_term,
        (parsed_content:"SUBGROUP - PSYCHIATRIC"::string = 'Y') as is_subgroup_psychiatric,
        (parsed_content:"SUBGROUP - REHABILITATION"::string = 'Y') as is_subgroup_rehabilitation,
        (parsed_content:"SUBGROUP - SHORT-TERM"::string = 'Y') as is_subgroup_short_term,
        (parsed_content:"SUBGROUP - SWING-BED APPROVED"::string = 'Y') as is_subgroup_swing_bed_approved,
        (parsed_content:"SUBGROUP - PSYCHIATRIC UNIT"::string = 'Y') as is_subgroup_psychiatric_unit,
        (parsed_content:"SUBGROUP - REHABILITATION UNIT"::string = 'Y') as is_subgroup_rehabilitation_unit,
        (parsed_content:"SUBGROUP - SPECIALTY HOSPITAL"::string = 'Y') as is_subgroup_specialty_hospital,
        (parsed_content:"SUBGROUP - OTHER"::string = 'Y') as is_subgroup_other,
        parsed_content:"SUBGROUP - OTHER TEXT"::string as subgroup_other_text,

        -- Metadata
        file_name,
        loaded_at

    from safely_parsed
    where parsed_content is not null -- This is the filter that will skip the bad rows

)

select * from renamed_and_casted