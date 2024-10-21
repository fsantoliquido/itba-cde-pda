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