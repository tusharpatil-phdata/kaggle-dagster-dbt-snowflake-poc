select
    country,
    release_year,
    title_count,
    movie_count,
    tvshow_count,
    movie_count::float / nullif(title_count, 0) as pct_movies,
    tvshow_count::float / nullif(title_count, 0) as pct_tvshows
from {{ ref('int_netflix_country_year_stats') }}