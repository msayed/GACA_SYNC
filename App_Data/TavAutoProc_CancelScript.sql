select distinct
lm.FLT Flight,
LTRIM(RTRIM(lm.DEP)) as ActFrom,
LTRIM(RTRIM(lm.ARR)) as ActTo ,
LTRIM(RTRIM(a.ACTYPE))  Make,
lm .REG Rego,
dbo. [GetStartTimev2](lm.DAY,lm.STD)  as DEP, 
case when BLOF <0 then 0 else dbo. [GetStartTimev2](lm.DAY,BLOF) end   as Start,
DATEDIFF (mi,dbo. [GetStartTimev2](lm.DAY,lm.STD),case when BLOF <0 then 0 else dbo. [GetStartTimev2](lm.DAY,BLOF) end )as DepDelay,
dbo. [GetStartTimev2](lm.DAY,STA)  as Arr,
dbo. [GetStartTimev2](lm.DAY,BLON)  as Stop,
DATEDIFF (mi,dbo. [GetStartTimev2](lm.DAY,STA) ,dbo. [GetStartTimev2](lm.DAY,BLON))as ArrDelay,
isnull(dbo.LGPAX3(lm.day,lm.dep,lm.FLT,lm.CARRIER,lm.LEGCD ),0) ActualYClass, 0 ActualInfant,
 EstimatedTime=case when ( DATEDIFF (mi,dbo. [GetStartTimev2](lm.DAY,lm.STD),dbo. [GetStartTimev2](lm.DAY,STA))) <0 then  ( DATEDIFF (mi,dbo. [GetStartTimev2](lm.DAY,lm.STD),DATEADD(day, 1, dbo. [GetStartTimev2](lm.DAY,STA)) )) else  DATEDIFF (mi,dbo. [GetStartTimev2](lm.DAY,lm.STD),dbo. [GetStartTimev2](lm.DAY,STA)) end,
 
 ServiceType=(   select ServiceType from dbo.ServicesTypes where idx =lm.FLTYPE),
 '' Name,
LTRIM(RTRIM(isnull(m.MEMODATA,'')))  Comment,

[dbo].[fn_ToDate]  (lm.DAY ) as [SectorDate]


from LEGMAIN lm
left outer join LEGTIMES lt on lm.DAY=lt.DAY and lm.DEP =lt.DEP  and lm.FLT=lt.FLT  and lm.LEGCD =lt.LEGCD 
left outer join LEGOTHER lo on lm.DAY=lo.DAY and lm.DEP =lo.DEP  and lm.FLT=lo.FLT  and lm.LEGCD =lo.LEGCD 
left outer join vvAircraft a on lm.AC=a.AC and a.REG=lm.REG 
left outer join  AIRCTYPE ac on ac.ACTYPE=lm.AC 
left outer join legdelays ld on  ld.day=lm .day and ld.dep=lm.dep and ld.flt=lm.flt and ld.legcd=lm.legcd
left outer join LEGMEMO m on  m.day=ld .day and m.dep=ld.dep and m.flt=ld.flt and m.legcd=ld.legcd and CATEGORY=1



where 
 lm.CANCELLED=1 and lm.FLTYPE=1
and [dbo].[fn_ToDate]  (lm.DAY ) >=@StartDate and [dbo].[fn_ToDate]  (lm.DAY )  <=@ENDDate

order by dbo. [GetStartTimev2](lm.DAY,lm.STD)



