{% set subgroup_columns = [
    ('is_subgroup_general', 'General'),
    ('is_subgroup_acute_care', 'Acute Care'),
    ('is_subgroup_alcohol_drug', 'Alcohol Drug'),
    ('is_subgroup_childrens', 'Childrens'),
    ('is_subgroup_long_term', 'Long-Term'),
    ('is_subgroup_psychiatric', 'Psychiatric'),
    ('is_subgroup_rehabilitation', 'Rehabilitation'),
    ('is_subgroup_short_term', 'Short-Term'),
    ('is_subgroup_swing_bed_approved', 'Swing-Bed Approved'),
    ('is_subgroup_psychiatric_unit', 'Psychiatric Unit'),
    ('is_subgroup_rehabilitation_unit', 'Rehabilitation Unit'),
    ('is_subgroup_specialty_hospital', 'Specialty Hospital')
] %}

with source as (
    select
        npi,
        ccn,
        -- Bring all subgroup flags
        is_subgroup_general,
        is_subgroup_acute_care,
        is_subgroup_alcohol_drug,
        is_subgroup_childrens,
        is_subgroup_long_term,
        is_subgroup_psychiatric,
        is_subgroup_rehabilitation,
        is_subgroup_short_term,
        is_subgroup_swing_bed_approved,
        is_subgroup_psychiatric_unit,
        is_subgroup_rehabilitation_unit,
        is_subgroup_specialty_hospital,
        is_subgroup_other,
        subgroup_other_text
    from
        {{ ref('stg_cms_enrollments') }}
),

unpivoted as (
    
    {% for flag_col, type_name in subgroup_columns %}
    
    select
        npi,
        ccn,
        '{{ type_name }}' as subgroup_type,
        null as subgroup_text
    from source
    where {{ flag_col }} = true
    
    {{ "union all" if not loop.last or "is_subgroup_other" }}

    {% endfor %}

    select
        npi,
        ccn,
        'Other' as subgroup_type,
        subgroup_other_text
    from source
    where is_subgroup_other = true
)

select * from unpivoted