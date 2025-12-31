select 
  {{ dbt_utils.generate_surrogate_key(['address.address_id']) }} as address_key,
  address.address_id,
  city.city as city_name,
  country.country as country_name
from {{ ref('dvd_address') }} address
left join {{ ref('dvd_city') }} city 
  on address.city_id = city.city_id
left join {{ ref('dvd_country') }} country 
  on city.country_id = country.country_id 