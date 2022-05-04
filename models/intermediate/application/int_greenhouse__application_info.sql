with application as (

    select *
    from {{ ref('int_greenhouse__application_users') }}
),

candidate as (

    select *
    from {{ ref('int_greenhouse__candidate_users') }}
),

candidate_tag as (

    select *
    from {{ ref('int_greenhouse__candidate_tags') }}
),

job_stage as (

    select *
    from {{ var('job_stage') }}
),

source as (

    select *
    from {{ var('source') }}
),

rejection_reason as (

    select 
        *
    from {{ var('rejection_reason')}}
),

offer as (

    select *
    from {{ var('offer')}}
),

activity as (

    select 
        candidate_id,
        count(*) as count_activities

    from {{ var('activity') }}
    group by 1
),

-- note: prospect applications can have multiple jobs, while canddiate ones are 1:1
job as (

    select *
    from {{ ref('int_greenhouse__job_info') }}
),

job_application as (

    select *
    from {{ var('job_application') }}
),

{% if var('greenhouse_using_eeoc', true) %}
eeoc as (

    select *
    from {{ var('eeoc') }}
),
{% endif %}

{% if var('greenhouse_using_prospects', true) %}
prospect_pool as (

    select *
    from {{ var('prospect_pool') }}
),

prospect_stage as (

    select *
    from {{ var('prospect_stage') }}
),
{% endif %}

join_info as (

    select 
        application.*,
        -- remove/rename overlapping columns + get custom columns
        {% if target.type == 'snowflake'%}
        {{ dbt_utils.star(from=ref('int_greenhouse__candidate_users'), 
            except=["CANDIDATE_ID", "NEW_CANDIDATE_ID", "CREATED_AT", "_FIVETRAN_SYNCED", "LAST_ACTIVITY_AT"], 
            relation_alias="candidate") }}

        {% else %}
        {{ dbt_utils.star(from=ref('int_greenhouse__candidate_users'), 
            except=["candidate_id", "new_candidate_id", "created_at", "_fivetran_synced", "last_activity_at"], 
            relation_alias="candidate") }}
        
        {% endif %}
        ,
        candidate.created_at as candidate_created_at,
        candidate_tag.tags as candidate_tags,
        job_stage.stage_name as current_job_stage,
        source.source_name as sourced_from,
        source.source_type_name as sourced_from_type,

        rejection_reason.rejection_reason_type_name,
        rejection_reason.reason as rejection_reason,

            offer.CUSTOM_ANNUAL_BONUS as offer_CUSTOM_ANNUAL_BONUS
        ,   offer.CUSTOM_ANNUAL_VARIABLE_COMP_AMOUNT as offer_CUSTOM_ANNUAL_VARIABLE_COMP_AMOUNT
        ,   offer.CUSTOM_ANNUAL_COMPENSATION_OFFER_1616528073_398702 as offer_CUSTOM_ANNUAL_COMPENSATION_OFFER_1616528073_398702
        ,   offer.CUSTOM_CAR_ALLOWANCE as offer_CUSTOM_CAR_ALLOWANCE
        ,   offer.CUSTOM_DRAW_PERIOD as offer_CUSTOM_DRAW_PERIOD
        ,   offer.CUSTOM_EMPLOYMENT_TYPE as offer_CUSTOM_EMPLOYMENT_TYPE
        ,   offer.CUSTOM_END_DATE as offer_CUSTOM_END_DATE
        ,   offer.CUSTOM_FINAL_OFFER_LETTER_TITLE as offer_CUSTOM_FINAL_OFFER_LETTER_TITLE
        ,   offer.CUSTOM_HIRE_TYPE as offer_CUSTOM_HIRE_TYPE
        ,   offer.CUSTOM_HOURLY_RATE_INTERNS_ONLY as offer_CUSTOM_HOURLY_RATE_INTERNS_ONLY
        ,   offer.CUSTOM_HOUSING_STIPEND as offer_CUSTOM_HOUSING_STIPEND
        ,   offer.CUSTOM_IMMIGRATION_STATUS as offer_CUSTOM_IMMIGRATION_STATUS
        ,   offer.CUSTOM_INTERNAL_LOCATION as offer_CUSTOM_INTERNAL_LOCATION
        ,   offer.CUSTOM_JOB_LEVEL as offer_CUSTOM_JOB_LEVEL
        ,   offer.CUSTOM_JOB_PROFILE as offer_CUSTOM_JOB_PROFILE
        ,   offer.CUSTOM_MANAGERS_NAME as offer_CUSTOM_MANAGERS_NAME
        ,   offer.CUSTOM_MONTHLY_DRAW_AMOUNT as offer_CUSTOM_MONTHLY_DRAW_AMOUNT
        ,   offer.CUSTOM_PERFORMANCE_BONUS_CS_FIELD_MARKETING_SALES_OPS_ as offer_CUSTOM_PERFORMANCE_BONUS_CS_FIELD_MARKETING_SALES_OPS_
        ,   offer.CUSTOM_RELOCATION_BONUS_AMOUT as offer_CUSTOM_RELOCATION_BONUS_AMOUT
        ,   offer.CUSTOM_SIGN_ON_BONUS as offer_CUSTOM_SIGN_ON_BONUS
        ,   offer.CUSTOM_STOCK_OPTIONS_AMOUNT as offer_CUSTOM_STOCK_OPTIONS_AMOUNT
        ,   offer.CUSTOM_TARGET_ACCEPTANCE_WINDOW as offer_CUSTOM_TARGET_ACCEPTANCE_WINDOW
        ,   offer.CUSTOM_VARIABLE_COMP_PAYMENT_FREQUENCY as offer_CUSTOM_VARIABLE_COMP_PAYMENT_FREQUENCY
        ,   offer.CUSTOM_WORKED_AT_COMPETITOR_IN_LAST_1_YEAR as offer_CUSTOM_WORKED_AT_COMPETITOR_IN_LAST_1_YEAR
        ,   offer.MAX_OFFER_VERSION as offer_max
        ,   offer.SENT_AT as offer_sent_at
        ,   offer.STARTS_AT as offer_starts_at
        ,   offer.STATUS as offer_status
        ,   offer.VERSION as offer_version
        ,   offer.CREATED_AT as offer_created_at
        ,   offer.UPDATED_AT as offer_updated_at
        ,   offer.RESOLVED_AT as offer_resolved_at

        ,   activity.count_activities

        ,   job.job_title
        ,   job.status as job_status
        ,   job.hiring_managers
        ,   job.job_id
        , job.requisition_id as job_requisition_id
        , job.sourcers as job_sourcers

        {% if var('greenhouse_using_job_office', True) %}
        ,
        job.offices as job_offices
        {% endif %}

        {% if var('greenhouse_using_job_department', True) %}
        ,
        job.departments as job_departments
        ,job.parent_departments as job_parent_departments
        {% endif %}

        {% if var('greenhouse_using_prospects', true) %}
        ,
        prospect_pool.prospect_pool_name as prospect_pool,
        prospect_stage.prospect_stage_name as prospect_stage
        {% endif %}

        {% if var('greenhouse_using_eeoc', true) %}
        ,
        eeoc.gender_description as candidate_gender,
        eeoc.disability_status_description as candidate_disability_status,
        eeoc.race_description as candidate_race,
        eeoc.veteran_status_description as candidate_veteran_status
        {% endif %}


    from application
    left join candidate
        on application.candidate_id = candidate.candidate_id
    left join candidate_tag
        on application.candidate_id = candidate_tag.candidate_id
    left join job_stage
        on application.current_stage_id = job_stage.job_stage_id
    left join source
        on application.source_id = source.source_id
    left join rejection_reason
        on application.rejected_reason_id = rejection_reason.rejection_reason_type_id
    left join offer
        on application.application_id = offer.application_id
    left join activity
        on activity.candidate_id = candidate.candidate_id
    left join job_application
        on application.application_id = job_application.application_id
    left join job
        on job_application.job_id = job.job_id

    {% if var('greenhouse_using_eeoc', true) %}
    left join eeoc 
        on eeoc.application_id = application.application_id
    {% endif -%}


    {% if var('greenhouse_using_prospects', true) %}
    left join prospect_pool 
        on prospect_pool.prospect_pool_id = application.prospect_pool_id
    left join prospect_stage
        on prospect_stage.prospect_stage_id = application.prospect_stage_id
    {% endif %}
),

final as (

    select 
        *,
        {{ dbt_utils.surrogate_key(['application_id', 'job_id']) }} as application_job_key
    
    from join_info
)

select *
from final