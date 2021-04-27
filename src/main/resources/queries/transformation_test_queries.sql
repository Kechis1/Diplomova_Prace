SELECT * FROM student stt JOIN studuje sde ON stt.sID = stt.sID WHERE sde.sID LIKE stt.sID;

SELECT * FROM student stt JOIN studuje sde ON stt.sID = stt.sID WHERE sde.sID LIKE stt.sID;

SELECT * FROM student stt JOIN studuje sde ON stt.sID = stt.sID WHERE sde.sID LIKE stt.sID;

SELECT c.ProductCategoryID
     , s.ProductSubcategoryID
     , AVG(ListPrice) AS 'Average'
    , MIN(ListPrice) AS 'Minimum'
    , MAX(ListPrice) AS 'Maximum'
FROM (
         SELECT *, MIN(ListPrice) OVER (PARTITION BY ProductSubcategoryID) AS MinimumListPriceForSubcategory
         FROM Production.Product
     ) p
         JOIN Production.ProductSubcategory s ON s.ProductSubcategoryID = p.ProductSubcategoryID
         JOIN Production.ProductCategory c ON s.ProductCategoryID = c.ProductCategoryID
WHERE ListPrice <> 0
  AND MinimumListPriceForSubcategory > 200
GROUP BY c.ProductCategoryID, s.ProductSubcategoryID;

select top(1) with ties birdType, count(1) AS birdCount
from migratorybirds
group by birdType
order by count (1) desc;

select hgmclent.clnt_name
     , cmpadd.add_1
     , cmpadd.add_2
     , cmpadd.add_3
     , hgmprty1.post_code
     , hgmprty1.prty_id
     , hraptvpd.seq_no
     , vd_prd
     , void_cd
     , void_desr
     , st_date
     , seq_no
     , end_date
     , est_avldt
     , lst_revn
     , Rank() OVER (PARTITION BY clnt_name,cmpadd.add_1 ORDER BY hraptvpd.seq_no DESC) AS RNK
from hgmprty1
         join hratency on prty_ref = prty_id
         join hrartamt on hrartamt.rent_acc_no = hratency.rent_acc_no
         join hracracr on hracracr.rent_acc_no = hrartamt.rent_acc_no
         join hgmclent on hgmclent.client_no = hracracr.clnt_no
         join cmpadd on cmpadd.add_id = hgmprty1.add_cd
         JOIN hraptvpd WITH (nolock)
ON hraptvpd.prty_ref=hgmprty1.prty_id
    JOIN hraptvps on hraptvps.prty_ref=hraptvpd.prty_ref AND seq_no=vd_prd
where
    tency_end_dt is NULL
  AND
    prim_clnt_yn=1;

SELECT DISTINCT v,
                ROW_NUMBER() OVER (ORDER BY v) row_number
FROM t
ORDER BY v, row_number;


SELECT LPSource, SymbolId, Volume, CreatedDate
FROM (
         SELECT LPSource,
                SymbolId,
                Volume,
                CreatedDate,
                ROW_NUMBER() OVER (PARTITION BY LPSource, SymbolId
                          ORDER BY CreatedDate DESC) AS rn
         FROM mytable) AS t
WHERE t.rn = 1;

select prchseordr_id               as "PO ID",
       max(prchseordr_dte_rqstd)   as DateRequested,
       prchseordr_type             as POType,
       max(vndr_nme)               as Vendor,
       sum(imhstry_qntty_ordrd)    as QuantityOrdered,
       sum(imhstry_qntty_invcd_ap) as QuantityVouchered
from imhstry
         join prchseordr on imhstry.imhstry_ordr_id = prchseordr.prchseordr_id
         join brnch on prchseordr.brnch_rn = brnch.brnch_rn
         join vndr on prchseordr.vndr_rn = vndr.vndr_rn
where prchseordr_dte_rqstd between '2016-01-01' and '2016-04-01'
group by prchseordr.prchseordr_id, prchseordr_type
HAVING SUM(CASE WHEN prchseordr_type = 'Credit' THEN imhstry_qntty_invcd_ap END) = 0
    OR SUM(CASE WHEN prchseordr_type = 'Purchase' THEN imhstry_qntty_invcd_ap - imhstry_qntty_ordrd END) < 0
order by prchseordr_id asc;

SELECT ParticipantId
FROM Contact
WHERE EXISTS
          (SELECT 1
           FROM Contact c2
           WHERE c2.ParticipantID = c.ParticipantId
             AND ContactTypeId = 1
           GROUP BY ParticipantID
           HAVING COUNT(*) > 1
              AND COUNT(CASE WHEN IsCurrent = 0 AND IsActive = 1 THEN 1 END) >= 1
          );




SELECT P.PK_PatientId, PV.PK_PatientVisitId, ISNULL(P.FName, '') + ', ' + ISNULL(P.LName, '') AS NAME, MAX(TVP.PK_VisitProcedureId) AS PK_VisitProcedureId, DateSort FROM (SELECT PV.FK_PatientId PatientId, MAX(PK_PatientVisitId) PatientVisitId, MAX(PV.LastUpdated) AS DateSort FROM dbo.M_PatientVisit AS PV AND (PV.IsActive = 1) GROUP BY PV.FK_PatientId) AS LatestVisits, M_Patient AS p, TX_VisitProcedure AS tvp WHERE p.PK_PatientId = LatestVisits.PatientId AND tvp.FK_PatientVisitId = LatestVisits.PatientVisitId AND (P.IsActive = 1) AND (TVP.IsActive = 1) GROUP BY PK_PatientId, PK_PatientVisitId, ISNULL(P.FName, '') + ', ' + ISNULL(P.LName, ''), DateSort ORDER BY 1 DESC, DateSort DESC;


select name, age, count(*) as cnt
from #People
group by name, age
having count(*) > 2;

SELECT JotID,
       MAX(CASE WHEN city = 'London' AND language = 'English' THEN 1 ELSE 0 END) as EnglishFlag,
       MAX(CASE WHEN city = 'Madrid' AND language = 'Spanish' THEN 1 ELSE 0 END) as SpanishFlag
FROM uv_attributes
WHERE HOC = 1
  AND JotTypeID = 5
  AND ((city = 'London' AND language = 'English') OR
       (city = 'Madrid' AND language = 'Spanish')
    )
GROUP BY JotID;



SELECT DISTINCT productid, MAX(id) OVER (PARTITION BY productid) AS LastRowId
from [order_items]
where productid in
    (SELECT productid
    FROM [order_items]
    group by productid
    having COUNT (*)
    >0)
order by productid;


SELECT * FROM student stt JOIN studuje sde ON stt.sID = stt.sID WHERE sde.sID LIKE stt.sID;

SELECT * FROM student stt JOIN studuje sde ON stt.sID = sde.sID WHERE stt.sID LIKE stt.sID;

SELECT stt.sid, stt.jmeno, COUNT(*) FROM student stt JOIN studuje ste on stt.sid = ste.sid WHERE stt.prijmeni LIKE '%ová' GROUP BY stt.sid, stt.jmeno;

select s.[name] 'Schema',t.[name] 'Table',c.[name] 'Column',d.[name] 'Data Type',c.[max_length] 'Length',d.[max_length] 'Max Length',d.[precision] 'Precision', c.[is_identity] 'Is Id', c.[is_nullable] 'Is Nullable', c.[is_computed] 'Is Computed', d.[is_user_defined] 'Is UserDefined', t.[modify_date] 'Date Modified', t.[create_date] 'Date created' from sys.schemas s inner join sys.tables t on s.schema_id = t.schema_id inner join sys.columns c on t.object_id = c.object_id inner join sys.types d on c.user_type_id = d.user_type_id where c.name like '%ColumnName%';

SELECT * FROM Orders o WHERE EXISTS(SELECT * FROM Products p WHERE p.ProductNumber = o.ProductNumber);

SELECT CASE WHEN EXISTS(SELECT 1 FROM theTable WHERE theColumn LIKE 'theValue%') THEN 1 ELSE 0 END;

SELECT * FROM DBO.STUDUJE SDT WHERE EXISTS(SELECT * FROM DBO.PREDMET PDT WHERE SDT.PID = PDT.PID);

SELECT distinct SDT.SID, SDT.JMENO FROM DBO.STUDENT SDT LEFT JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID HAVING SUM(SDT.SID) > 3 ORDER BY SDT.SID;

SELECT PID, JMENO, 2 FROM DBO.PREDMET WHERE ROCNIK = 2;

SELECT * FROM Orders WHERE ProductNumber IN (SELECT ProductNumber FROM Products WHERE ProductInventoryQuantity > 0);

select t.name as TableWithForeignKey,fk.constraint_column_id as FK_PartNo,c.name as ForeignKeyColumn from sys.foreign_key_columns as fk inner join sys.tables as t on fk.parent_object_id = t.object_id inner join sys.columns as c on fk.parent_object_id = c.object_id and fk.parent_column_id = c.column_id where fk.referenced_object_id = (select object_id from sys.tables where name = 'TableOthersForeignKeyInto') order by TableWithForeignKey, FK_PartNo;


SELECT f.name AS 'Name of Foreign Key', OBJECT_NAME(f.parent_object_id) AS 'Table name', COL_NAME(fc.parent_object_id, fc.parent_column_id) AS 'Fieldname', OBJECT_NAME(t.object_id) AS 'References Table name', COL_NAME(t.object_id, fc.referenced_column_id) AS 'References fieldname','ALTER TABLE [' + OBJECT_NAME(f.parent_object_id) + ']  DROP CONSTRAINT [' + f.name + ']' AS 'Delete foreign key','ALTER TABLE [' + OBJECT_NAME(f.parent_object_id) + ']  WITH NOCHECK ADD CONSTRAINT [' + f.name + '] FOREIGN KEY([' + COL_NAME(fc.parent_object_id, fc.parent_column_id) + ']) REFERENCES ' + '[' + OBJECT_NAME(t.object_id) + '] ([' + COL_NAME(t.object_id, fc.referenced_column_id) + '])' AS 'Create foreign key' FROM sys.foreign_keys AS f,sys.foreign_key_columns AS fc,sys.tables t WHERE f.OBJECT_ID = fc.constraint_object_id AND t.OBJECT_ID = fc.referenced_object_id AND OBJECT_NAME(t.object_id) = 'Employees'ORDER BY 2;

SELECT ProductID, ProductName FROM Products p WHERE NOT EXISTS(SELECT * FROM [Order Details] od WHERE p.ProductId = od.ProductId);


SELECT name,email, COUNT(*) FROM users GROUP BY name, email HAVING COUNT(*) > 1;

SELECT id, name, email FROM users u,users u2 WHERE u.name = u2.name AND u.email = u2.email AND u.id > u2.id;

SELECT MIN(x.id), x.customer, x.total FROM PURCHASES xJOIN (SELECT p.customer, MAX(total) AS max_total FROM PURCHASES p GROUP BY p.customer) y ON y.customer = x.customer AND y.max_total = x.total GROUP BY x.customer, x.total;

SELECT DISTINCT (customer) id, customer, total FROM purchases ORDER BY customer, total DESC, id;

SELECT categoryName, AVG(unitPrice)FROM Products p INNER JOIN Categories c ON c.categoryId = p.categoryId GROUP BY categoryName HAVING AVG(unitPrice) > 10;

SELECT categoryId,productId,productName,unitPrice FROM Products p1 WHERE unitPrice = ( SELECT MIN(unitPrice) FROM Products p2 WHERE p2.categoryId = p1.categoryId);

SELECT categoryId, categoryName, productId, SUM(unitPrice)FROM Products p INNER JOIN Categories c ON c.categoryId = p.categoryId GROUP BY categoryId, productId;

SELECT a.id, a.rev, a.contents FROM products aINNER JOIN ( SELECT id, MAX(rev) rev FROM products GROUP BY id ) b ON a.id = b.id AND a.rev = b.rev;


WITH t2o AS (SELECT t2.*, ROW_NUMBER() OVER (PARTITION BY t1_id ORDER BY rank) AS rn FROM t2)SELECT t1.*, t2o.*FROM t1 INNER JOIN t2o ON t2o.t1_id = t1.id AND t2o.rn <= 3;


SELECT S.SID, ST.BODY, ST.ROK FROM STUDENT AS S CROSS APPLY (SELECT TOP 1 BODY, ROK FROM STUDUJE WHERE S.SID = S.SID ORDER BY ROK DESC) AS ST;


SELECT * FROM DBO.STUDENT WHERE 'abc' BETWEEN 'aaa' AND 'abc';

SELECT * FROM DBO.PREDMET WHERE EXISTS (SELECT 1);

SELECT * FROM DBO.PREDMET WHERE EXISTS(SELECT 1 FROM STUDUJE WHERE PREDMET.PID = STUDUJE.PID);

SELECT * FROM DBO.STUDENT SDT INNER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID INNER JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID WHERE SDT.SID = SDT.JMENO ORDER BY SDT.SID;


SELECT COUNT(st.sid) AS sid_count, st.rok FROM (SELECT DISTINCT stu.sid, stu.rok FROM studuje stu WHERE stu.sid = 3) st GROUP BY st.rok;

SELECT COUNT(st.pid), st.rok FROM studuje st GROUP BY st.sid HAVING COUNT(st.pid) > 5;


select #temp.num, count(*)
from #temp
         left join
     (
         SELECT count(membership.memberid) as MembershipCount
         from Membership,
              Package
         WHERE membership.PackageId = Package.Id
           AND membership.DiscountPercentage != 100
    AND Package.PackageTypeId != 1
    AND membership.MembershipStateId != 5
    AND Membership.LocationId = 1
         group by memberid
         having count (membership.memberid) > 1
     ) ntm
     on ntm.MembershipCount > #temp.num
group by #temp.num;



SELECT sd.OrderId,
       sd.TotalAmount
FROM Sales_Order sd
         INNER JOIN
     Order_Line ol
     ON
         (sd.OrderId = ol.OrderId);


SELECT c.hacker_id,
       h.name,
       COUNT(c.hacker_id) AS ctn
FROM Sample0.Hackers as h
         left JOIN Sample0.Challenges as c
                   ON h.hacker_id = c.hacker_id
GROUP BY c.hacker_id, h.name
HAVING COUNT(c.hacker_id) = (SELECT TOP 1 COUNT (c1.challenge_id)
FROM Sample0.Challenges AS c1
GROUP BY c1.hacker_id
ORDER BY COUNT (*)) OR
    COUNT (c.hacker_id) NOT IN (
SELECT COUNT (c2.challenge_id)
FROM Sample0.Challenges AS c2
where c2.hacker_id <> c.hacker_id
GROUP BY c2.hacker_id );

SELECT s.*
FROM STUDENT s
WHERE NOT EXISTS(
        SELECT 1 FROM STUDENT_COURSE c WHERE c.STUDENT_ID = s.ID
    );

SELECT s.*
FROM STUDENT s
         LEFT JOIN STUDENT_COURSE c
                   ON c.STUDENT_ID = s.ID
WHERE c.ID IS NULL;

SELECT *
FROM STUDENT
WHERE ID NOT IN (
    SELECT STUDENT_ID
    FROM STUDENT_COURSE
);

SELECT p.FirstName,
       p.LastName,
       p.Email
FROM People p
WHERE p.PeopleID IN
      (
          SELECT r.PeopleIdNum
          FROM Registration r
                   INNER JOIN Section s
                              on r.SectionIDNum = s.SectionID
                   INNER JOIN School sc
                              on p.SchoolIDNum = sc.SchoolID
          WHERE s.CourseIDNum IN (11, 12, 68, 177, 128)
            AND sc.DistrictIDNum = 5
            AND r.Completed = 'Y'
      );

SELECT *
FROM RF_CustomerCard
WHERE DateOfBirth IN (
    SELECT DateOfBirth
    FROM RF_CustomerCard
    GROUP BY DateOfBirth
    HAVING COUNT(DateOfBirth) > 1
)
ORDER BY DateOfBirth;

select *
from table1 t
where exists(
              select id
              from table2 t2
              where t2.fkid = t.id
              group by t2.id
              having count(*) > 1
          );

select ci.CaseNumber,
       ci.CaseName,
       p.firstname + ' ' + p.lastname AS child,
       ci.programClosureDate
from (select CaseId
      from CaseChild cc
      group by CaseId
      having count(*) = count(cc.ProgramClosureDate)
     ) closed
         join
     CaseInfo ci
     on closed.CaseId = ci.CaseId
         join
     Party p
     ON cc.ChildPartyID = p.PartyID
order by ci.CaseName;