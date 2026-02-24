with src as (
    select * from {{ source('kaggle_netflix', 'NETFLIX_TITLES') }}
)

select
    show_id,
    type,
    title,
    country,
    try_to_date(date_added, 'MON DD, YYYY') as date_added,
    release_year,
    rating,
    duration,
    listed_in as genres,
    description
from src