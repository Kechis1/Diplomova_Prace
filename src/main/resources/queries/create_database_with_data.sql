IF OBJECT_ID('dbo.studuje', 'U') IS NOT NULL
  DROP TABLE dbo.studuje
IF OBJECT_ID('dbo.predmet', 'U') IS NOT NULL
  DROP TABLE dbo.predmet
IF OBJECT_ID('dbo.student', 'U') IS NOT NULL
  DROP TABLE dbo.student

create table student (
	sID int primary key,
	jmeno varchar(30) not null,
	rok_narozeni int
)

create table predmet (
	pID int primary key,
	jmeno varchar(30) not null,
	rocnik int
)
	
create table studuje (
	pID int references predmet,
	sID int references student,
	rok int,
	body int,
	primary key (pID, sID, rok)
)


insert into student values (1, 'Petr', 1986);
insert into student values (2, 'Karel', 1984);
insert into student values (3, 'Jan', 1980);
insert into student values (4, 'Vojta', 1983);
insert into student values (5, 'Lucie', 1985);
insert into student values (6, 'Iva', 1986);
insert into student values (7, 'Petra', 1985);
insert into student values (8, 'Olaf', null);

insert into predmet values (1, 'UDBS', 2);
insert into predmet values (2, 'DAIS', 2);
insert into predmet values (3, 'DBS', 3);
insert into predmet values (4, 'MAIT', 1);
insert into predmet values (5, 'ZP', 1);
insert into predmet values (6, 'DIM', 2);

insert into studuje values (1, 1, 2010, 45);
insert into studuje values (1, 1, 2011, 51);
insert into studuje values (1, 3, 2011, 23);
insert into studuje values (1, 4, 2010, 62);
insert into studuje values (1, 5, 2010, 56);
insert into studuje values (1, 6, 2010, 2);
insert into studuje values (1, 6, 2011, 51);
insert into studuje values (1, 7, 2010, 12);
insert into studuje values (1, 7, 2011, 15);
insert into studuje values (2, 1, 2010, 12);
insert into studuje values (2, 6, 2010, 15);
insert into studuje values (3, 4, 2010, 68);
insert into studuje values (3, 5, 2010, 69);
insert into studuje values (3, 6, 2010, 52);
insert into studuje values (5, 4, 2010, 78);
insert into studuje values (5, 5, 2010, 59);
insert into studuje values (6, 4, 2011, 86);
insert into studuje values (6, 5, 2011, 55);
insert into studuje values (6, 3, 2012, null);