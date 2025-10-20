select distinct
    provider_type_code,
    provider_type_text
from
    {{ ref('stg_hospital_enrollments') }}
where
    provider_type_code is not null