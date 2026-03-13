SELECT
    CONSTRUCTOR_ID,
    CASE
        WHEN NAME = 'Red Bull'
            THEN 'Red Bull Racing'
        WHEN NAME = 'Sauber'
            THEN 'Kick Sauber'
        WHEN NAME = 'RB F1 Team'
            THEN 'Racing Bulls'
        WHEN NAME = 'Alpine F1 Team'
            THEN 'Alpine'
        WHEN NAME = 'Cadillac F1 Team'
            THEN 'Cadillac'
        ELSE NAME
    END AS NAME
FROM REFERENCE.DIM_CONSTRUCTOR