-- Update e insert para la tabla de videos
UPDATE pda."2024_franco_santoliquido_schema".BT_YOUTUBE_VIDEO_STATS DW
SET 
    YT_VIEW_COUNT_7D = STG.view_count,
    YT_LIKE_COUNT_7D = STG.like_count,
    YT_COMMENT_COUNT_7D = STG.comment_count,
    YT_LIKES_PER_VIEW = STG.likes_per_view,
    YT_COMMENTS_PER_VIEW = STG.comments_per_view,
    AUD_UPD_ID = CURRENT_DATE,
    AUD_UPDATED_FROM = 'ETL_YOUTUBE_CDE_PDA'
FROM 
    pda."2024_franco_santoliquido_schema".youtube_videos_stg STG
WHERE  
    STG.video_id = DW.YT_VIDEO_ID
    AND STG.published_at >= DATEADD(day, -7, CURRENT_DATE)
    ;
   

INSERT INTO pda."2024_franco_santoliquido_schema".BT_YOUTUBE_VIDEO_STATS (
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
    YT_LIKES_PER_VIEW,
    YT_COMMENTS_PER_VIEW,
    AUD_UPD_ID,
    AUD_INS_DATE,
    AUD_UPDATED_FROM
)
SELECT 
    STG.channel_name,
    STG.channel_id,
    STG.title,
    cast(STG.published_at as timestamp),
    STG.video_id,
    STG.video_type,
    STG.view_count,
    STG.like_count,
    STG.comment_count,
    STG.duration_seconds,
    STG.is_short,
    STG.likes_per_view,
    STG.comments_per_view,
    CURRENT_DATE,
    CURRENT_DATE, 
    'ETL_YOUTUBE_CDE_PDA'
FROM 
    pda."2024_franco_santoliquido_schema".youtube_videos_stg STG
LEFT JOIN 
    pda."2024_franco_santoliquido_schema".BT_YOUTUBE_VIDEO_STATS DW
    ON STG.video_id = DW.YT_VIDEO_ID
WHERE 
    DW.YT_VIDEO_ID IS NULL;


-- Insert para la tabla de suscriptores

INSERT INTO pda."2024_franco_santoliquido_schema".LG_CHANNEL_SUBSCRIBERS (
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
    cast(STG.consulta_fecha as date),
    cast(STG.subscriber_count as int),
    CURRENT_DATE,
    'ETL_YOUTUBE_CDE_PDA'
FROM 
    pda."2024_franco_santoliquido_schema".youtube_subscribers_stg STG
LEFT JOIN 
    pda."2024_franco_santoliquido_schema".LG_CHANNEL_SUBSCRIBERS DW
ON 
    STG.channel_id = DW.YT_CHANNEL_ID
    AND cast(STG.consulta_fecha as date)= DW.YT_DATE_ID
WHERE 
    DW.YT_CHANNEL_ID IS NULL
    ;