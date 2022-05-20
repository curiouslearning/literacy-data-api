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
 METADATA AS( 
    SELECT
      params.*
    FROM
      (
        SELECT * EXCEPT(key, val)
        FROM `{{dataset}}.deep_link_metadata`,
        UNNEST(metadata) as fields
        WHERE(
          fields.key = 'utm_campaign'
          AND fields.val = @utm_campaign) OR @utm_campaign = ''
      ) as params
  ),
  UUIDS AS (
    SELECT
      user_pseudo_id,
      event_params
    FROM
      `{{dataset}}.events_*`
    WHERE
      _TABLE_SUFFIX BETWEEN '20211201' AND FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
      AND app_info.id = @pkg_id
      AND event_name = "LogUserUUID"
  ),

  UUID_AND_PROFILE AS (
    SELECT
      (
        SELECT `{{dataset}}.get_value`(params.value)
        FROM UNNEST(event_params) as params
        WHERE params.key = "uuid"
      ) as uuid,
      (
        SELECT `{{dataset}}.get_value`(params.value)
        FROM UNNEST(event_params) as params
        WHERE params.key = "profile"
      ) as profile,
      * EXCEPT(event_params),
    FROM
      UUIDs
    INNER JOIN METADATA on UUID_AND_PROFILE.user_pseudo_id = METADATA.user_pseudo_id
  ),
  APP_INITIALIZED AS (
  SELECT
    params.value.string_value as attribution_id,
    CAST(event_timestamp as STRING) as val,
    "app_initialized" as action,
    "timestamp" as label,
    * EXCEPT(user_properties, key, value)
  FROM
    `{{dataset}}.events_*`,
    UNNEST(event_params) AS params
  WHERE
    _TABLE_SUFFIX BETWEEN '20210801' AND FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
    AND event_name = 'app_initialized'
    AND params.value.string_value = @ref_id OR @ref_id=''),
  INIT_SCREEN AS (
    SELECT
      attribution_id,
      action,
      label,
      val,
      `{{dataset}}.getValue`(params.value) as screen,
      * EXCEPT(attribution_id, val, action, label, event_params, key, value)
    FROM
      APP_INITIALIZED,
      UNNEST(APP_INITIALIZED.event_params) as params
    WHERE
      params.key = 'screen'
  ),
  DEFAULT_SCREEN AS (
    SELECT
        attribution_id,
        action,
        label,
        val,
        "Splash Screen" as screen,
        * EXCEPT(attribution_id, val, action, label, event_params)
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
        (
          SELECT 
            `{{dataset}}.getValue`(params.value)
          FROM
            UNNEST(LITERACY_DATA.event_params) as params
          WHERE
            params.key= "action"
            AND params.value.string_value LIKE CONCAT(@event, '%') OR @event = ''
        ) as action,
        (
          SELECT 
            `{{dataset}}.getValue`(params.value)
          FROM
            UNNEST(LITERACY_DATA.event_params) as params
          WHERE
            params.key= "label"
        ) as label,
        (
          SELECT 
            `{{dataset}}.getValue`(params.value)
          FROM
            UNNEST(LITERACY_DATA.event_params) as params
          WHERE
            params.key= "value"
        ) as value,
        (
          SELECT 
            `{{dataset}}.getValue`(params.value)
          FROM
            UNNEST(LITERACY_DATA.event_params) as params
          WHERE
            params.key= "firebase_screen"
        ) as screen,
        * EXCEPT(event_params)
    FROM
        LITERACY_DATA
)

SELECT
  APP_INITIALIZED.attribution_id,
  FILTERED_LIT_DATA.action,
  FILTERED_LIT_DATA.label,
  FILTERED_LIT_DATA.val,
  FILTERED_LIT_DATA.screen,
  METADATA.metadata,
  UUID_AND_PROFILE.profile,
  UUID_AND_PROFILE.uuid,
  FILTERED_LIT_DATA.* EXCEPT(action, label, val, screen, user_properties)
FROM
  FILTERED_LIT_DATA
INNER JOIN APP_INITIALIZED on APP_INITIALIZED.user_pseudo_id = FILTERED_LIT_DATA.user_pseudo_id
INNER JOIN UUID_AND_PROFILE on UUID_AND_PROFILE.user_pseudo_id = FILTERED_LIT_DATA.user_pseudo_id AND (FILTERED_LIT_DATA.screen = "Splash Screen" OR FILTERED_LIT_DATA.screen LIKE CONCAT("%- Profile ", UUID_AND_PROFILE.profile) OR FILTERED_LIT_DATA.screen LIKE CONCAT("%Profile: ", UUID_AND_PROFILE.profile))
INNER JOIN METADATA on METADATA.user_pseudo_id = FILTERED_LIT_DATA.user_pseudo_id
UNION ALL(
  SELECT
    attribution_id,
    action,
    label,
    val,
    screen,
    METADATA.metadata,
    UUID_AND_PROFILE.profile,
    UUID_AND_PROFILE.uuid,
    INIT_SCREEN.* EXCEPT(attribution_id, action, screen, label, val)
  FROM
    INIT_SCREEN
    INNER JOIN UUID_AND_PROFILE on UUID_AND_PROFILE.user_pseudo_id = INIT_SCREEN.user_pseudo_id AND (INIT_SCREEN.screen = "Splash Screen" OR INIT_SCREEN.screen LIKE CONCAT("%- Profile ", UUID_AND_PROFILE.profile) OR INIT_SCREEN.screen LIKE CONCAT("%Profile: ", UUID_AND_PROFILE.profile))
    INNER JOIN METADATA on METADATA.user_pseudo_id = INIT_SCREEN.user_pseudo_id
)
UNION ALL (
  SELECT
    attribution_id,
    action,
    label,
    val,
    screen,
    METADATA.metadata,
    UUID_AND_PROFILE.profile,
    UUID_AND_PROFILE.uuid,
    DEFAULT_SCREEN.* EXCEPT(attribution_id, action, screen, label, val)
  FROM
    DEFAULT_SCREEN
    INNER JOIN UUID_AND_PROFILE on UUID_AND_PROFILE.user_pseudo_id = DEFAULT_SCREEN.user_pseudo_id AND (DEFAULT_SCREEN.screen = "Splash Screen" OR DEFAULT_SCREEN.screen LIKE CONCAT("%- Profile ", UUID_AND_PROFILE.profile) OR DEFAULT_SCREEN.screen LIKE CONCAT("%Profile: ", UUID_AND_PROFILE.profile))
    INNER JOIN METADATA on METADATA.user_pseudo_id = DEFAULT_SCREEN.user_pseudo_id AND (DEFAULT_SCREEN.screen = "Splash Screen" OR DEFAULT_SCREEN.screen LIKE CONCAT("%- Profile ", UUID_AND_PROFILE.profile) OR DEFAULT_SCREEN.screen LIKE CONCAT("%Profile: ", UUID_AND_PROFILE.profile))) ORDER BY event_timestamp DESC
