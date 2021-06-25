WITH
  LITERACY_EVENTS AS (
  SELECT
    user_pseudo_id,
    event_date,
    event_timestamp,
    geo.continent,
    geo.country,
    geo.region,
    geo.city,
    device.advertising_id,
    app_info.id as app_package_name,
    props.key as referral_type,
    props.value.string_value as referral_source,
    event_name,
    event_params
  FROM
    @table,
    UNNEST(user_properties) as props
  WHERE
    _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
    AND FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
    AND event_timestamp > @cursor
    AND app_info.id = @pkg_id
    AND (props.value.string_value LIKE @ref_id OR @ref_id = '')
    AND (event_name = 'GamePlay'
    OR event_name = 'SubSkills'
    OR event_name = 'TimeTracking')
  ORDER BY
    event_timestamp DESC),

PARSED_ACTION AS (
  SELECT
    * EXCEPT(key, value),
    params.value.string_value as action
  FROM (
    SELECT
       * EXCEPT (key, value),
       p.value.string_value as label
      FROM
        LITERACY_EVENTS,
        UNNEST(event_params)as p
      WHERE p.key = "label"),
    UNNEST(event_params) as params
  WHERE
    params.key = "action"
),

STRING_VALS AS (
  SELECT
    * EXCEPT (event_params, key, value),
    params.value.string_value as value,
    "categorical" as type
  FROM
    PARSED_ACTION,
    UNNEST(event_params) as params
  WHERE
    params.key = "value"
    AND params.value.string_value IS NOT NULL
),
INT_VALS AS (
  SELECT
    * EXCEPT (event_params, key, value),
    CAST(params.value.int_value AS STRING) as value,
    "continuous" as type
  FROM
    PARSED_ACTION,
    UNNEST(event_params) as params
  WHERE
    params.key = "value"
    AND params.value.int_value IS NOT NULL
),
FLOAT_VALS AS (
  SELECT
    * EXCEPT (event_params, key, value),
    CAST(params.value.float_value AS STRING) as value,
    "continuous" as type
  FROM
    PARSED_ACTION,
    UNNEST(event_params) as params
  WHERE
    params.key = "value"
    AND  params.value.float_value IS NOT NULL
),
DOUBLE_VALS AS (
  SELECT
    * EXCEPT (event_params, key, value),
    CAST(params.value.double_value AS STRING) as value,
    "continuous" as type
  FROM
    PARSED_ACTION,
    UNNEST(event_params) as params
  WHERE
    params.key = "value"
    AND params.value.double_value IS NOT NULL
)

SELECT
  *
FROM STRING_VALS
UNION ALL (SELECT * FROM INT_VALS)
UNION ALL (SELECT * FROM FLOAT_VALS)
UNION ALL (SELECT * FROM DOUBLE_VALS)
ORDER BY event_timestamp DESC
