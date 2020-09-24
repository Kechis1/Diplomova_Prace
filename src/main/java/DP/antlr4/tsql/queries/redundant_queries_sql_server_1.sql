-----------------
-- GROUP BY -----
-----------------
-- priklad
SELECT PID, JMENO
FROM DBO.PREDMET
GROUP BY PID, JMENO
GO

/*SELECT PID, JMENO
FROM DBO.PREDMET;*/

    -- 1. MUSI BYT
    -- agregacni funkce v HAVING
    SELECT pId, jmeno
    FROM dbo.predmet
    GROUP BY pid, jmeno
    HAVING sum(pid) > 3

    -- agregacni funkce v ORDER
    SELECT pId, jmeno
    FROM dbo.predmet
    GROUP BY pid, jmeno
    ORDER BY sum(pid)

    -- agregacni funkce v select
    SELECT pId, jmeno, sum(pid)
    FROM dbo.predmet
    GROUP BY pid, jmeno

    -- v group by nejsou vsechny primarni klice ze vsech spojovanych tabulkach
    SELECT pr.pId, stt.sID
    FROM dbo.student stt
             JOIN dbo.studuje ste ON stt.sID = ste.sID
             JOIN dbo.predmet pr ON ste.pID = pr.pID
    GROUP BY pr.pID, stt.sID


    -- 1. NEMUSI BYT
    -- pokud jsou jen primarni klice ze vsech join tabulek
    SELECT pr.pId, stt.sID, ste.sID, ste.pID
    FROM dbo.student stt
             JOIN dbo.studuje ste ON stt.sID = ste.sID
             JOIN dbo.predmet pr ON ste.pID = pr.pID
    GROUP BY pr.pID, stt.sID, ste.pID, ste.sID, ste.rok

    SELECT pId, jmeno, count(*)
    FROM dbo.predmet
    GROUP BY pid, sqrt(jmeno), jmeno
    -- se muze prepsat na
    SELECT pId, jmeno, count(*)
    FROM dbo.predmet

    -- count(*) bude vzdy vracet 1
    SELECT pId, jmeno, count(*)
    FROM dbo.predmet
    GROUP BY pid, jmeno
    -- se muze prepsat na
    SELECT pId, jmeno, 1
    FROM dbo.predmet

    -- ostatni agregacni funkce se mohou prepsat na sloupec
    SELECT pId, jmeno, sum(pId)
    FROM dbo.predmet
    GROUP BY pid, jmeno
    -- se muze prepsat na
    SELECT pId, jmeno, pID
    FROM dbo.predmet

--------------------
-- KONEC GROUP BY --
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
-- KONEC WHERE -----
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
-- KONEC JOIN ------
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
-- KONEC LIKE ------
--------------------


