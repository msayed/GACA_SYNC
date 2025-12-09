SELECT 
    lm.FLT AS Flight,
    LTRIM(RTRIM(lm.DEP)) AS ActFrom,
    LTRIM(RTRIM(lm.ARR)) AS ActTo,
    LTRIM(RTRIM(a.ACTYPE)) AS Make,
    lm.REG AS Rego,
    dbo.[GetStartTimev2](lm.DAY, lm.STD) AS DEP,
    dbo.[GetStartTimev2](lm.DAY, BLOF) AS Start,
    DATEDIFF(
        MINUTE,
        dbo.[GetStartTimev2](lm.DAY, lm.STD),
        dbo.[GetStartTimev2](lm.DAY, BLOF)
    ) AS DepDelay,
    dbo.[GetStartTimev2](lm.DAY, STA) AS Arr,
    dbo.[GetStartTimev2](lm.DAY, BLON) AS Stop,
    DATEDIFF(
        MINUTE,
        dbo.[GetStartTimev2](lm.DAY, STA),
        dbo.[GetStartTimev2](lm.DAY, BLON)
    ) AS ArrDelay,
    ISNULL(dbo.LGPAX3(lm.day, lm.dep, lm.FLT, lm.CARRIER, lm.LEGCD), 0) AS ActualYClass,
    0 AS ActualInfant,
    EstimatedTime =
        CASE 
            WHEN DATEDIFF(
                    MINUTE,
                    dbo.[GetStartTimev2](lm.DAY, lm.STD),
                    dbo.[GetStartTimev2](lm.DAY, STA)
                ) < 0
            THEN DATEDIFF(
                    MINUTE,
                    dbo.[GetStartTimev2](lm.DAY, lm.STD),
                    DATEADD(DAY, 1, dbo.[GetStartTimev2](lm.DAY, STA))
                )
            ELSE DATEDIFF(
                    MINUTE,
                    dbo.[GetStartTimev2](lm.DAY, lm.STD),
                    dbo.[GetStartTimev2](lm.DAY, STA)
                )
        END,
    ServiceType =
        CASE 
            WHEN (
                    SELECT ServiceType 
                    FROM dbo.ServicesTypes 
                    WHERE idx = lm.FLTYPE
                 ) = 'J  Scheduled Service   '
            THEN 'J Sched Normal Service'
            ELSE (
                    SELECT ServiceType 
                    FROM dbo.ServicesTypes 
                    WHERE idx = lm.FLTYPE
                 )
        END,
    'FEBRUARY 2018' AS Name,
    LTRIM(RTRIM(ISNULL(memo.AllComments, ''))) AS Comment,
    [dbo].[fn_ToDate](lm.DAY) AS [SectorDate]
FROM LEGMAIN lm
LEFT OUTER JOIN LEGTIMES lt 
    ON lm.DAY = lt.DAY 
   AND lm.DEP = lt.DEP  
   AND lm.FLT = lt.FLT  
   AND lm.LEGCD = lt.LEGCD 
LEFT OUTER JOIN LEGOTHER lo 
    ON lm.DAY = lo.DAY 
   AND lm.DEP = lo.DEP  
   AND lm.FLT = lo.FLT  
   AND lm.LEGCD = lo.LEGCD 
LEFT OUTER JOIN vvAircraft a 
    ON lm.AC = a.AC 
   AND a.REG = lm.REG 
LEFT OUTER JOIN AIRCTYPE ac 
    ON ac.ACTYPE = lm.AC 
LEFT OUTER JOIN (
    SELECT 
        day,
        dep,
        flt,
        legcd,
        STRING_AGG(LTRIM(RTRIM(ISNULL(MEMODATA, ''))), ' | ') 
            WITHIN GROUP (ORDER BY MEMODATA) AS AllComments
    FROM LEGMEMO
    WHERE CATEGORY = 7
    GROUP BY day, dep, flt, legcd
) AS memo
    ON memo.day   = lm.day
   AND memo.dep   = lm.dep
   AND memo.flt   = lm.flt
   AND memo.legcd = lm.legcd
WHERE 
    lm.CANCELLED = 0  
    AND lm.FLTYPE = 1
    --and lm.FLT=346
    and [dbo].[fn_ToDate]  (lm.DAY ) >=@StartDate and [dbo].[fn_ToDate]  (lm.DAY )  <=@ENDDate
    AND NOT (LTRIM(RTRIM(a.ACTYPE)) = '' OR LTRIM(RTRIM(a.ACTYPE)) IS NULL)
    AND NOT (LTRIM(RTRIM(lm.REG)) = '' OR LTRIM(RTRIM(lm.REG)) IS NULL)
ORDER BY 
    
    lm.FLT
    ,dbo.[GetStartTimev2](lm.DAY, lm.STD)
