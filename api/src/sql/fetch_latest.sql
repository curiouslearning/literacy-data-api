CREATE FUNCTION IF NOT EXISTS `{{dataset}}.getValue`(vals STRUCT<string_value STRING, int_value INT64, float_value FLOAT64, double_value FLOAT64>)
AS(
    CASE
        WHEN
            vals.string_value IS NOT NULL THEN vals.string_value
        WHEN vals.int_value IS NOT NULL THEN CAST(vals.int_value AS STRING)
        WHEN vals.float_value IS NOT NULL THEN CAST(vals.float_value AS STRING)
        WHEN vals.double_value IS NOT NULL then CAST(vals.double_value AS STRING)
        ELSE NULL
    END
);
WITH
  APP_INITIALIZED AS (
  SELECT
    params.value.string_value as attribution_id,
    props.value.string_value as uuid,
    CAST(event_timestamp as STRING) as val,
    "app_initialized" as action,
    "timestamp" as label,
    * EXCEPT(user_properties, key, value)
  FROM
    `{{dataset}}.events_*`,
    UNNEST(event_params) AS params,
    UNNEST(user_properties) AS props,
  WHERE
    _TABLE_SUFFIX BETWEEN '20210801' AND FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
    AND event_name = 'app_initialized'
    AND params.value.string_value = @ref_id OR @ref_id='')
    AND props.key = 'uuid0'
  INIT_SCREEN AS (
    SELECT
      attribution_id,
      uuid,
      action,
      label,
      val,
      `{{dataset}}.getValue`(params.value) as screen,
      * EXCEPT(attribution_id, uuid val, action, label, event_params, key, value)
    FROM
      APP_INITIALIZED,
      UNNEST(APP_INITIALIZED.event_params) as params
    WHERE
      params.key = 'screen'
  ),

  DEFAULT_SCREEN AS (
    SELECT
        attribution_id,
        uuid,
        action,
        label,
        val,
        "Splash Screen" as screen,
        * EXCEPT(attribution_id, uuid, val, action, label, event_params)
    FROM
        APP_INITIALIZED
    WHERE
        NOT EXISTS(
          SELECT
            *
          FROM
            UNNEST(event_params) as params
          WHERE params.key = 'firebase_screen'
        )
),

  LITERACY_DATA AS (
  SELECT
    *
  FROM
    `{{dataset}}.events_*`
  WHERE
    _TABLE_SUFFIX BETWEEN FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL @range DAY))
    AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
    AND app_info.id = @pkg_id
    AND event_timestamp > @cursor
    AND (event_name = 'SubSkill'
      OR event_name = 'TimeTracking'
      OR event_name = 'GamePlay')
    AND (device.advertising_id = @user_id OR user_pseudo_id = @user_id OR @user_id = '')
    ),

FILTERED_LIT_DATA AS (
    SELECT
        `{{dataset}}.getValue`(params.value) as action,
        props.value.string_value as uuid,
        * EXCEPT(key, value)
    FROM
        LITERACY_DATA,
        UNNEST(LITERACY_DATA.event_params) as params,
        UNNEST(LITERACY_DATA.user_properties) as props,
    WHERE
        params.key = "action"
        props.key = "uuid0"
        AND (params.value.string_value LIKE CONCAT(@event, '%') OR @event = '')
),

 SCREENS AS (
  SELECT
    `{{dataset}}.getValue`(params.value) as screen,
    action,
    * EXCEPT(action, key, value)
  FROM
    FILTERED_LIT_DATA,
    UNNEST(FILTERED_LIT_DATA.event_params) as params
  WHERE
    params.key = "firebase_screen"
), LABELS AS (
    SELECT
        action,
        `{{dataset}}.getValue`(params.value) as label,
        screen,
        * EXCEPT(action, screen, key, value)
    FROM SCREENS,
    UNNEST(SCREENS.event_params) as params
    WHERE params.key = "label"
),

VALS AS (
    SELECT
        action,
        label,
        `{{dataset}}.getValue`(params.value) as val,
        screen,
        * EXCEPT(label, action, screen, key, value)
    FROM LABELS,
    UNNEST(LABELS.event_params) as params
    WHERE params.key = "value"
)

SELECT
  APP_INITIALIZED.attribution_id,
  VALS.action,
  VALS.label,
  VALS.val,
  VALS.screen,
  VALS.* EXCEPT(action, label, val, screen, event_params, user_properties)
FROM
  VALS
INNER JOIN APP_INITIALIZED on APP_INITIALIZED.user_pseudo_id = VALS.user_pseudo_id
UNION ALL(
  SELECT
    attribution_id,
    action,
    label,
    val,
    screen,
    * EXCEPT(attribution_id, action, screen, label, val)
  FROM
    INIT_SCREEN
)
UNION ALL (
  SELECT
    attribution_id,
    action,
    label,
    val,
    screen,
    * EXCEPT(attribution_id, action, screen, label, val)
  FROM
    DEFAULT_SCREEN
)
ORDER BY event_timestamp DESC
