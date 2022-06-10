with candidate as (

    select *
    from {{ var('candidate') }}
),

-- candidates can have multiple phones + emails
phones as (

    select 
        candidate_id,
        {{ fivetran_utils.string_agg("phone_type || ': ' || phone_number" , "', '") }} as phone

    from {{ var('phone_number') }}

    group by 1
),

emails as (

    select 
        candidate_id,
        {{ fivetran_utils.string_agg("'<' || email || '>'" , "', '") }} as email

    from {{ var('email_address') }}

    group by 1
),

-- getting the last resume uploaded
order_resumes as (

    select 
        *,
        row_number() over(partition by candidate_id order by index desc) as resume_row_num
    from {{ var('attachment') }}

    where lower(type) = 'resume'
),

latest_resume as (

    select *
    from order_resumes 
    where resume_row_num = 1
),

order_links as (

    select 
        candidate_id,
        lower(url) as url,
        row_number() over(partition by candidate_id, lower(url) like '%linkedin%' order by index desc) as linkedin_row_num,
        row_number() over(partition by candidate_id, lower(url) like '%github%' order by index desc) as github_row_num

    from {{ var('social_media_address') }}

    where lower(url) like '%linkedin%' or lower(url) like '%github%'

),

latest_links as (

    select
        candidate_id,
        max(case when linkedin_row_num = 1 and url like '%linkedin%' then url end) as linkedin_url,
        max(case when github_row_num = 1 and url like '%github%' then url end) as github_url

    from order_links
    
    where (linkedin_row_num = 1 and url like '%linkedin%') or 
        (github_row_num = 1 and url like '%github%')
    
    group by 1
),

order_education as (

  select 
         candidate_id,degree,school_name,discipline,
        case 
            when degree is NULL then 0
            when degree like 'Other' then 0
            when degree like 'High School' then 1
            when degree like 'Associate%' then 2
            when degree like 'Bachelor%' then 3
            when degree like 'Engineer%' then 3
            when degree like 'Master%' then 4
            when degree like '%Doctor%' then 5
                end                             as highest_degree,
            row_number() over(partition by candidate_id order by highest_degree desc) as degree_row_num,
            rank() over (partition by candidate_id order by highest_degree desc) as degree_rank
    from {{ var('education') }}
),

highest_education as (
    select *
    from order_education
   where degree_rank = 1

),

latest_education as (

    select *
    from highest_education 
    where degree_row_num = 1
),

join_candidate_info as (

    select 
        candidate.*,
        phones.phone as phone,
        emails.email as email,
        latest_resume.url as resume_url,
        latest_links.linkedin_url,
        latest_links.github_url,
        latest_education.school_name,
        latest_education.degree as highest_degree,
        latest_education.discipline
    
    from 
    candidate
    left join phones
        on candidate.candidate_id = phones.candidate_id
    left join emails 
        on candidate.candidate_id = emails.candidate_id
    left join latest_resume
        on candidate.candidate_id = latest_resume.candidate_id
    left join latest_links
        on candidate.candidate_id = latest_links.candidate_id
    left join latest_education
        on candidate.candidate_id = latest_education.candidate_id
)

select *
from join_candidate_info