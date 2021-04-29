-- HEX = XXX
-- predikat je TRUE
SELECT * FROM dbo.student WHERE 0xA = 0xA;
SELECT * FROM dbo.student WHERE 0xA = 10;
-- error converting data type
SELECT * FROM dbo.student WHERE 0xA = 10.0;
SELECT * FROM dbo.student WHERE 0xA = 10e0;
-- predikat je FALSE
SELECT * FROM dbo.student WHERE 0xA = '0xA';
SELECT * FROM dbo.student WHERE 0xA = '10';
SELECT * FROM dbo.student WHERE 0xA = '10.0';
SELECT * FROM dbo.student WHERE 0xA = '10e0';




-- INT = XXX
-- predikat je TRUE
SELECT * FROM dbo.student WHERE 10 = 0xA;
SELECT * FROM dbo.student WHERE 10 = 10;
SELECT * FROM dbo.student WHERE 10 = 10.0;
SELECT * FROM dbo.student WHERE 10 = 10e0;
-- error converting data type
SELECT * FROM dbo.student WHERE 10 = '0xA';
-- predikat je TRUE
SELECT * FROM dbo.student WHERE 10 = '10';
-- error converting data type
SELECT * FROM dbo.student WHERE 10 = '10.0';
SELECT * FROM dbo.student WHERE 10 = '10e0';




-- FLOAT = XXX
-- error converting data type
SELECT * FROM dbo.student WHERE 10.0 = 0xA;
-- predikat je TRUE
SELECT * FROM dbo.student WHERE 10.0 = 10;
SELECT * FROM dbo.student WHERE 10.0 = 10.0;
SELECT * FROM dbo.student WHERE 10.0 = 10e0;
-- error converting data type
SELECT * FROM dbo.student WHERE 10.0 = '0xA';
-- predikat je TRUE
SELECT * FROM dbo.student WHERE 10.0 = '10';
SELECT * FROM dbo.student WHERE 10.0 = '10.0';
-- error converting data type
SELECT * FROM dbo.student WHERE 10.0 = '10e0';




-- REAL = XXX
-- error converting data type
SELECT * FROM dbo.student WHERE 10e0 = 0xA;
-- predikat je TRUE
SELECT * FROM dbo.student WHERE 10e0 = 10;
SELECT * FROM dbo.student WHERE 10e0 = 10.0;
SELECT * FROM dbo.student WHERE 10e0 = 10e0;
-- error converting data type
SELECT * FROM dbo.student WHERE 10e0 = '0xA';
-- predikat je TRUE
SELECT * FROM dbo.student WHERE 10e0 = '10';
SELECT * FROM dbo.student WHERE 10e0 = '10.0';
SELECT * FROM dbo.student WHERE 10e0 = '10e0';




-- STRING(HEX) = XXX
-- predikat je FALSE
SELECT * FROM dbo.student WHERE '0xA' = 0xA;
-- error converting data type
SELECT * FROM dbo.student WHERE '0xA' = 10;
SELECT * FROM dbo.student WHERE '0xA' = 10.0;
SELECT * FROM dbo.student WHERE '0xA' = 10e0;
-- predikat je TRUE
SELECT * FROM dbo.student WHERE '0xA' = '0xA';
-- predikat je FALSE
SELECT * FROM dbo.student WHERE '0xA' = '10';
SELECT * FROM dbo.student WHERE '0xA' = '10.0';
SELECT * FROM dbo.student WHERE '0xA' = '10e0';




-- STRING(INT) = XXX
-- predikat je FALSE
SELECT * FROM dbo.student WHERE '10' = 0xA;
-- predikat je TRUE
SELECT * FROM dbo.student WHERE '10' = 10;
SELECT * FROM dbo.student WHERE '10' = 10.0;
SELECT * FROM dbo.student WHERE '10' = 10e0;
-- predikat je FALSE
SELECT * FROM dbo.student WHERE '10' = '0xA';
-- predikat je TRUE
SELECT * FROM dbo.student WHERE '10' = '10';
-- predikat je FALSE
SELECT * FROM dbo.student WHERE '10' = '10.0';
SELECT * FROM dbo.student WHERE '10' = '10e0';




-- STRING(FLOAT) = XXX
-- predikat je FALSE
SELECT * FROM dbo.student WHERE '10.0' = 0xA;
-- error converting data type
SELECT * FROM dbo.student WHERE '10.0' = 10;
-- predikat je TRUE
SELECT * FROM dbo.student WHERE '10.0' = 10.0;
SELECT * FROM dbo.student WHERE '10.0' = 10e0;
-- predikat je FALSE
SELECT * FROM dbo.student WHERE '10.0' = '0xA';
SELECT * FROM dbo.student WHERE '10.0' = '10';
-- predikat je TRUE
SELECT * FROM dbo.student WHERE '10.0' = '10.0';
-- predikat je FALSE
SELECT * FROM dbo.student WHERE '10.0' = '10e0';




-- STRING(REAL) = XXX
-- predikat je FALSE
SELECT * FROM dbo.student WHERE '10e0' = 0xA;
-- error converting data type
SELECT * FROM dbo.student WHERE '10e0' = 10;
SELECT * FROM dbo.student WHERE '10e0' = 10.0;
-- predikat je TRUE
SELECT * FROM dbo.student WHERE '10e0' = 10e0;
-- predikat je FALSE
SELECT * FROM dbo.student WHERE '10e0' = '0xA';
SELECT * FROM dbo.student WHERE '10e0' = '10';
SELECT * FROM dbo.student WHERE '10e0' = '10.0';
-- predikat je TRUE
SELECT * FROM dbo.student WHERE '10e0' = '10e0';