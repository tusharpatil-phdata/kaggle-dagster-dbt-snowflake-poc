select
    country,
    release_year,
    count(*) as title_count,
    sum(case when type = 'Movie' then 1 else 0 end) as movie_count,
    sum(case when type = 'TV Show' then 1 else 0 end) as tvshow_count
from {{ ref('stg_netflix_titles') }}
where country is not null
group by country, release_year