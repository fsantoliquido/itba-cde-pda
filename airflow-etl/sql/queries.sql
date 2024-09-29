-- Update e insert para la tabla de videos
UPDATE "2024_franco_santoliquido_schema".BT_YOUTUBE_VIDEO_STATS DW
SET 
    YT_VIEW_COUNT_7D = STG.view_count,
    YT_LIKE_COUNT_7D = STG.like_count,
    YT_COMMENT_COUNT_7D = STG.comment_count,
    AUD_UPD_ID = CURRENT_DATE,
    AUD_UPDATED_FROM = 'ETL_YOUTUBE_CDE_PDA'
FROM 
    "2024_franco_santoliquido_schema".youtube_videos_stg STG
WHERE 
    STG.video_id = DW.YT_VIDEO_ID
    AND STG.published_at >= DATEADD(day, -7, CURRENT_DATE);
   

INSERT INTO "2024_franco_santoliquido_schema".BT_YOUTUBE_VIDEO_STATS (
    YT_CHANNEL_NAME,
    YT_CHANNEL_ID,
    YT_TITLE_NAME,
    YT_DATE_PUBLISHED,
    YT_VIDEO_ID,
    YT_VIDEO_TYPE,
    YT_VIEW_COUNT_7D,
    YT_LIKE_COUNT_7D,
    YT_COMMENT_COUNT_7D,
    YT_DURATION_SECS,
    YT_IS_SHORT_FLAG,
    AUD_UPD_ID,
    AUD_INS_DATE,
    AUD_UPDATED_FROM
)
SELECT 
    STG.channel_name,
    STG.channel_id,
    STG.title,
    STG.published_at,
    STG.video_id,
    STG.video_type,
    STG.view_count,
    STG.like_count,
    STG.comment_count,
    STG.duration_seconds,
    STG.is_short,
    CURRENT_DATE,
    CURRENT_DATE, 
    'ETL_YOUTUBE_CDE_PDA'
FROM 
    "2024_franco_santoliquido_schema".youtube_videos_stg STG
LEFT JOIN 
    "2024_franco_santoliquido_schema".BT_YOUTUBE_VIDEO_STATS DW
    ON STG.video_id = DW.YT_VIDEO_ID
WHERE 
    DW.YT_VIDEO_ID IS NULL;


-- Insert para la tabla de suscriptores

INSERT INTO "2024_franco_santoliquido_schema".LG_CHANNEL_SUBSCRIBERS (
    YT_CHANNEL_NAME,
    YT_CHANNEL_ID,
    YT_DATE_ID,
    YT_SUSCRIBER_COUNT,
    AUD_INS_DATE,
    AUD_UPDATED_FROM
)
SELECT 
    STG.channel_name,
    STG.channel_id,
    STG.consulta_fecha,
    STG.subscriber_count,
    CURRENT_DATE,
    'ETL_YOUTUBE_CDE_PDA'
FROM 
    "2024_franco_santoliquido_schema".youtube_subscribers_stg STG
LEFT JOIN 
    "2024_franco_santoliquido_schema".LG_CHANNEL_SUBSCRIBERS DW
ON 
    STG.channel_id = DW.YT_CHANNEL_ID
    AND STG.consulta_fecha = DW.YT_DATE_ID
WHERE 
    DW.YT_CHANNEL_ID IS NULL;