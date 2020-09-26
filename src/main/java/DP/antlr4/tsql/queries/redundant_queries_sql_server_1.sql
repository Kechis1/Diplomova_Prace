-----------------
-- GROUP BY -----
-----------------
-- example
SELECT PID, JMENO
FROM DBO.PREDMET
GROUP BY PID, JMENO
GO

/*SELECT PID, JMENO
FROM DBO.PREDMET;*/

    -- 1. MUST BE LEFT ALONE
    -- aggregate function in HAVING
    SELECT pId, jmeno
    FROM dbo.predmet
    GROUP BY pid, jmeno
    HAVING sum(pid) > 3

    -- aggregate function in ORDER
    SELECT pId, jmeno
    FROM dbo.predmet
    GROUP BY pid, jmeno
    ORDER BY sum(pid)

    -- not all primary keys are present in group by
    SELECT pr.pId, stt.sID
    FROM dbo.student stt
             JOIN dbo.studuje ste ON stt.sID = ste.sID
             JOIN dbo.predmet pr ON ste.pID = pr.pID
    GROUP BY pr.pID, stt.sID


    -- 1. CAN BE REWRITTEN
    -- all primary keys are present in group by
    SELECT pr.pId, stt.sID, ste.sID, ste.pID
    FROM dbo.student stt
             JOIN dbo.studuje ste ON stt.sID = ste.sID
             JOIN dbo.predmet pr ON ste.pID = pr.pID
    GROUP BY pr.pID, stt.sID, ste.pID, ste.sID, ste.rok

    -- aggregate function sum in select, with all primary keys
    SELECT pId, jmeno, sum(pid)
    FROM dbo.predmet
    GROUP BY pid, jmeno
    -- can be rewritten to
    SELECT pId, jmeno, pID
    FROM dbo.predmet

    -- aggregate function count in select, with all primary keys
    SELECT pId, jmeno, count(*)
    FROM dbo.predmet
    GROUP BY pid, jmeno
    -- can be rewritten to
    SELECT pId, jmeno, 1
    FROM dbo.predmet
--------------------
-- END GROUP BY --
--------------------

--------------------
-- WHERE -----------
--------------------
SELECT *
FROM DBO.PREDMET
WHERE 1 = 1
GO

/*SELECT *
FROM DBO.PREDMET;*/

-- vnejsi podminky vs vnitrni podminky (maji jiny vyznam)
SELECT *
FROM DBO.STUDENT SDT
         INNER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID
         INNER JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID
WHERE SDT.SID = SDE.SID
ORDER BY SDT.SID
GO

/*SELECT *
FROM DBO.STUDENT SDT
INNER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID
INNER JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID
ORDER BY SDT.SID;*/

--------------------
-- END WHERE -----
--------------------

--------------------
-- JOIN ------------
--------------------
SELECT distinct SDT.SID, SDT.JMENO
FROM DBO.STUDENT SDT
LEFT JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID
GO

/*SELECT SDT.SID, SDT.JMENO
FROM DBO.STUDENT SDT
;*/

--------------------
-- END JOIN ------
--------------------

--------------------
-- LIKE ------------
--------------------
SELECT *
FROM PREDMET
WHERE JMENO LIKE '%'
GO

/*SELECT *
FROM PREDMET;*/

--------------------
-- END LIKE ------
--------------------


