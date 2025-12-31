select
      $rowtime
    , `event_id`
    , `event_ts`
    , `user_id`
    , `session_id`
    , `film_id`
    , `event_type`
    , `page`
    , `position`
    , `device`
    , `referrer`
from `default`.`cluster_0`.`dvd_clicks`
limit 20;