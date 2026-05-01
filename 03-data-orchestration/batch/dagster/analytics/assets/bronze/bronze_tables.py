BRONZE_TABLES = [

    {"name": "address",
         "columns": [
         "address_id", "address", "address2",
         "district", "city_id", "postal_code",
         "phone", "last_update"
     ]},

    {"name": "category",
     "columns": ["category_id", "name", "last_update"]},

    {"name": "city",
     "columns": ["city_id", "city", "country_id", "last_update"]},

    {"name": "country",
     "columns": [
        "country_id", "country", "last_update"
     ]},



    {"name": "customer",
     "columns": [
         "customer_id", "store_id",
         "first_name", "last_name", "email",
         "address_id", "activebool",
         "create_date", "last_update", "active"
     ]},

    {"name": "film",
     "columns": [
         "film_id", "title", "description",
         "release_year", "language_id",
         "rental_duration", "rental_rate",
         "length", "replacement_cost",
         "rating", "fulltext", "special_features", "last_update"
     ]},

    {"name": "film_category",
     "columns": ["film_id", "category_id", "last_update"]},

    {"name": "inventory",
     "columns": [
         "inventory_id", "film_id",
         "store_id", "last_update"
     ]},

    {"name": "rental",
     "columns": [
         "rental_id", "rental_date",
         "inventory_id", "customer_id",
         "return_date", "staff_id",
         "last_update"
     ]},

]
