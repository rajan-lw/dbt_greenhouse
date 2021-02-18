with interview as (

    select *
    from {{ ref('int_greenhouse__interview_users') }}
),

job_stage as (

    select *
    from {{ var('job_stage') }}
),

-- this has job stuff in it
application as (

    select *
    from {{ ref('int_greenhouse__application_info') }}
),

final as (

    select
        interview.*,
        job_stage.stage_name as job_stage,
        application.status as current_application_status,
        application.job_title,
        application.job_office,
        application.job_department,
        application.job_parent_department,

        application.hiring_managers like ('%' || (interview.interviewer_first_name || ' ' || interview.interviewer_last_name) || '%')  as interviewer_is_hiring_manager,
        job_stage.stage_name = application.current_job_stage as has_advanced_since_interview,

        application.recruiter_first_name,
        application.recruiter_last_name,

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

select * from final