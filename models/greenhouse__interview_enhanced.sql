with interview as (

    select *
    from {{ ref('int_greenhouse__interview_users') }}
),

job_stage as (

    select *
    from {{ var('job_stage') }}
),

-- this has job info!
application as (

    select *
    from {{ ref('int_greenhouse__application_info') }}
),

final as (

    select
        interview.*,
        application.full_name as candidate_name,
        job_stage.stage_name as job_stage,
        application.current_job_stage as application_current_job_stage,
        application.status as current_application_status,
        application.job_title,
        application.job_id,

        application.hiring_managers like ('%' || interview.interviewer_name || '%')  as interviewer_is_hiring_manager,
        application.hiring_managers,
        application.recruiter_name

        {% if var('greenhouse_using_job_office', True) %}
        ,
        application.job_offices
        {% endif %}

        {% if var('greenhouse_using_job_department', True) %}
        ,
        application.job_departments,
        application.job_parent_departments
        {% endif %}

        {% if var('greenhouse_using_eeoc', true) %}
        ,
        application.candidate_gender,
        application.candidate_disability_status,
        application.candidate_race,
        application.candidate_veteran_status
        {% endif %}

    from interview
    left join job_stage 
        on interview.job_stage_id = job_stage.job_stage_id
    left join 
        application on interview.application_id = application.application_id
)

select 
*
, getdate()                                         as snapshot_datetime
, {{ dbt_utils.surrogate_key(
    [    'APPLICATION_ID'
      ,  'SCHEDULED_INTERVIEW_ID'
      ,  'START_AT'
      ,  'STATUS'
      ,  'INTERVIEWER_USER_ID'
      ,  'JOB_ID']
                    ) }}                            as primary_key
from final