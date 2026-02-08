select
    {{ dbt_utils.generate_surrogate_key(['film.film_id']) }} as film_key,
    film.film_id,
    film.title,
    film.release_year,
    film.length,
    film.rating,
    film.rental_duration,
    category.name as category_name
from {{ ref('dvd_film') }} film
left join {{ ref('dvd_film_category') }} film_category on film.film_id = film_category.film_id
left join {{ ref('dvd_category') }} category on film_category.category_id = category.category_id
