-- @1 group by

SELECT pId, jmeno
FROM dbo.predmet
GROUP BY pId, jmeno
GO

/*SELECT pId, jmeno
FROM dbo.predmet;*/

-- @2 where
SELECT *
FROM dbo.predmet
WHERE 1=1
GO

/*SELECT *
FROM dbo.predmet;*/

-- @3 where
SELECT *
FROM dbo.student sdt
INNER JOIN dbo.studuje sde ON sdt.sID = sde.sID
INNER JOIN dbo.predmet pdt ON sde.pID = pdt.pID
WHERE sdt.sID = sde.sID
ORDER BY sdt.sID
GO

/*SELECT *
FROM dbo.student sdt
INNER JOIN dbo.studuje sde ON sdt.sID = sde.sID
INNER JOIN dbo.predmet pdt ON sde.pID = pdt.pID
ORDER BY sdt.sID;*/

-- @4 join
SELECT sdt.sId, sdt.jmeno
FROM dbo.student sdt
LEFT JOIN dbo.studuje sde ON sdt.sID = sde.sID
GROUP BY sdt.sID, sdt.jmeno
GO

/*SELECT sdt.sId, sdt.jmeno
FROM dbo.student sdt
GROUP BY sdt.sID, sdt.jmeno;*/

-- @5 like
SELECT *
FROM predmet
WHERE jmeno LIKE '%'
GO

/*SELECT *
FROM predmet;*/
 