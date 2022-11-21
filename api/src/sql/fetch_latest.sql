  SELECT user_pseudo_id,
    event_name,
    event_timestamp,
    traffic_source,
    geo,
    device,
    (select as struct
        (select params.value.string_value from unnest(event_params) as params where params.key = 'label') as label,
        (select as struct
            (select case
                    when event_name = 'FirstOpenRefer' then (select params.value.string_value from unnest(event_params) as params where params.key = 'referrer')
                    else (select params.value.string_value from unnest(event_params) as params where params.key = 'value')
                    end) as string_value,
            (select case
                    when event_name = 'GamePlay' then (select CAST(REGEXP_EXTRACT(params.value.string_value, r'_(\d+)$') AS INT64) from unnest(event_params) as params where params.key = 'action')
                    else (select params.value.int_value from unnest(event_params) as params where params.key = 'value')
                    end) as int_value,
            (select params.value.float_value from unnest(event_params) as params where params.key = 'value') as float_value,
            (select params.value.double_value from unnest(event_params) as params where params.key = 'value') as double_value
        ) as value,
        (select case
            when event_name = 'GamePlay' then (select REGEXP_EXTRACT(params.value.string_value, r'([^_]+)(?:_Level)?_\d+$') from unnest(event_params) as params where params.key = 'action')
            else (select params.value.string_value from unnest(event_params) as params where params.key = 'action')
            end
        ) as action
    ) as event_params
    FROM `{{dataset}}.events_*` as events
WHERE (_TABLE_SUFFIX BETWEEN FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL @range DAY))
       AND FORMAT_DATE("%Y%m%d", CURRENT_DATE())
       OR _TABLE_SUFFIX LIKE 'intraday%')
AND event_timestamp >= @cursor
AND traffic_source.source = @traffic_source
AND geo.country = @country
ORDER BY event_timestamp
LIMIT @limit
