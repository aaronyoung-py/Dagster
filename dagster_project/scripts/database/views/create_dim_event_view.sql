DROP VIEW IF EXISTS REFERENCE.DIM_EVENT;

CREATE OR REPLACE VIEW REFERENCE.DIM_EVENT AS (
    WITH EVENTS AS (
        SELECT
            CONCAT(YEAR(cldr.EventDate), cldr.RoundNumber) AS EVENT_CD,
            cldr.RoundNumber AS ROUND_NUMBER,
            cldr.EventDate AS EVENT_DT,
            YEAR(cldr.EventDate) AS EVENT_YEAR,
            cldr.EventName AS EVENT_NAME,
            cldr.Location AS LOCATION,
            CASE
                WHEN (cldr.Location LIKE '%Montr%') THEN 'Montreal'
                WHEN (cldr.Location LIKE '%Paulo%') THEN 'SaoPaulo'
                WHEN (cldr.Location LIKE '%Portim%') THEN 'Portimao'
                WHEN (cldr.Location LIKE '%rburgring%') THEN 'Nurburgring'
                WHEN (cldr.Location = 'Baku') THEN 'Azerbaijan'
                WHEN (cldr.Location = 'Marina Bay') THEN 'Singapore'
                WHEN (cldr.Location = 'Yas Marina') THEN 'YasIsland'
                ELSE REPLACE(cldr.Location, ' ', '')
            END AS FCST_LOCATION,
            trk.TRACK_CD AS TRACK_CD,
            cldr.EventFormat AS EVENT_TYPE,
            CASE
                WHEN (cldr.EventFormat = 'conventional') THEN 1
                WHEN (cldr.EventFormat LIKE '%sprint%') THEN 2
                ELSE -(2)
            END AS EVENT_TYPE_CD,
            cldr.Session1 AS SESSION_ONE_TYPE,
            cldr.Session1DateUtc AS SESSION_ONE_DT,
            cldr.Session2 AS SESSION_TWO_TYPE,
            cldr.Session2DateUtc AS SESSION_TWO_DT,
            cldr.Session3 AS SESSION_THREE_TYPE,
            cldr.Session3DateUtc AS SESSION_THREE_DT,
            cldr.Session4 AS SESSION_FOUR_TYPE,
            cldr.Session4DateUtc AS SESSION_FOUR_DT,
            cldr.Session5 AS SESSION_FIVE_TYPE,
            cldr.Session5DateUtc AS SESSION_FIVE_DT
        FROM REFERENCE.F1_CALENDER cldr
        LEFT JOIN REFERENCE.DIM_TRACK_EVENT trk_evt
            ON CONCAT(YEAR(cldr.EventDate), cldr.RoundNumber) = trk_evt.EVENT_CD
        LEFT JOIN REFERENCE.DIM_TRACK trk
            ON trk_evt.TRACK_ID = trk.TRACK_ID
    )

    SELECT
        a11.EVENT_CD,
        CASE WHEN a13.EVENT_CD IS NULL THEN 'N/A' ELSE a13.EVENT_CD END AS EVENT_CD_LY,
        a11.ROUND_NUMBER,
        a11.EVENT_DT,
        a11.EVENT_YEAR,
        a11.EVENT_NAME,
        a11.LOCATION,
        a11.FCST_LOCATION,
        a11.TRACK_CD,
        a11.EVENT_TYPE,
        a11.EVENT_TYPE_CD,
        a11.SESSION_ONE_TYPE,
        a11.SESSION_ONE_DT,
        a11.SESSION_TWO_TYPE,
        a11.SESSION_TWO_DT,
        a11.SESSION_THREE_TYPE,
        a11.SESSION_THREE_DT,
        a11.SESSION_FOUR_TYPE,
        a11.SESSION_FOUR_DT,
        a11.SESSION_FIVE_TYPE,
        a11.SESSION_FIVE_DT
    FROM EVENTS a11
    LEFT JOIN REFERENCE.DIM_YEAR a12
        ON a11.EVENT_YEAR = a12.YEAR
    LEFT JOIN EVENTS a13
        ON a12.YEAR_LY = a13.EVENT_YEAR
        AND a11.TRACK_CD = a13.TRACK_CD
)