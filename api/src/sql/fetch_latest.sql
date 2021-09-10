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
    *,
    params.value.string_value as attribution_id
  FROM
    `{{dataset}}.events_*`,
    UNNEST(event_params) AS params
  WHERE
    _TABLE_SUFFIX BETWEEN '20210801' AND FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
    AND event_name = 'app_initialized'
    AND params.value.string_value = @ref_id OR @ref_id = ''),
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
    AND (device.advertising_id = @user_id OR user_pseudo_id = @user_id OR @user_id = '') ),
FILTERED_LIT_DATA AS (
    SELECT
        `{{dataset}}.getValue`(params.value) as action,
        *
    FROM
        LITERACY_DATA,
        UNNEST(LITERACY_DATA.event_params) as params
    WHERE
        params.key = "action"
        AND (params.value.string_value LIKE CONCAT(@event, '%') OR @event = '')
), SCREENS AS (
  SELECT
    `{{dataset}}.getValue`(params.value) as screen,
    *
  FROM
    FILTERED_LIT_DATA,
    UNNEST(FILTERED_LIT_DATA.event_params) as params
  WHERE
    params.key = "firebase_screen"
), LABELS AS (
    SELECT
        `{{dataset}}.getValue`(params.value) as label,
        *
    FROM SCREENS,
    UNNEST(SCREENS.event_params) as params
    WHERE params.key = "label"
),
ACTIONS AS (
    SELECT
        `{{dataset}}.getValue`(params.value) as action,
        *
    FROM LABELS,
    UNNEST(LABELS.event_params) as params
    WHERE params.key = "action"
),
VALS AS (
    SELECT
        `{{dataset}}.getValue`(params.value) as val,
        *
    FROM ACTIONS,
    UNNEST(ACTIONS.event_params) as params
    WHERE params.key = "value"
)
SELECT
  APP_INITIALIZED.attribution_id,
  VALS.* EXCEPT (event_params, user_properties)
FROM
  APP_INITIALIZED
INNER JOIN VALS on APP_INITIALIZED.user_pseudo_id = VALS.user_pseudo_id
ORDER BY event_timestamp DESC
