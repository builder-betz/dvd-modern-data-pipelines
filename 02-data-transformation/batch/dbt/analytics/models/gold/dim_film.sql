select
    {{ dbt_utils.generate_surrogate_key(['film.film_id']) }} as film_key,
    film.film_id,
    film.title,
    cast(film.release_year as int),
    cast(film.length as int),
    film.rating,
    cast(film.rental_duration as int),
    category.name as category_name
from {{ ref('dvd_film') }} film
left join {{ ref('dvd_film_category') }} film_category on film.film_id = film_category.film_id
left join {{ ref('dvd_category') }} category on film_category.category_id = category.category_id
