
CREATE DATABASE Mineria

USE Mineria

if not exists (select * from sysobjects where name='dim_edu_achieved' and xtype='U')
create table dim_edu_achieved (
id_e int identity(1,1) NOT NULL, -- Auto_increment: empieza en el valor uno y aumenta de uno en uno.
edu_ach varchar(120) NOT NULL,
perc real NOT NULL
)

GO

alter table dim_edu_achieved
	add primary key (id_e)

GO

if not exists (select * from sysobjects where name='dim_companies' and xtype='U')
create table dim_companies (
id_c int identity(1,1) NOT NULL, -- Auto_increment: empieza en el valor uno y aumenta de uno en uno.
quantity int NOT NULL,
class varchar(120) NOT NULL
)

GO

alter table dim_companies
	add primary key (id_c)

GO


if not exists (select * from sysobjects where name='dim_poverty' and xtype='U')
create table dim_poverty (
id_p int identity(1,1) NOT NULL, -- Auto_increment: empieza en el valor uno y aumenta de uno en uno.
class varchar(120) NOT NULL,
perc real NOT NULL
)

GO

alter table dim_poverty
	add primary key (id_p)

GO

if not exists (select * from sysobjects where name='facts_migration' and xtype='U')
create table facts_migration (
id int identity(1,1) NOT NULL, -- Auto_increment: empieza en el valor uno y aumenta de uno en uno.
comunidad varchar(100) NOT NULL,
n_year int NOT NULL,
flow int NOT NULL,
age int NOT NULL,
id_pov int,
id_com int,
id_edu int,
)

GO

alter table facts_migration
	add primary key (id)

GO




alter table facts_migration
	add constraint fk_m_poverty foreign key (id_pov) references dim_poverty (id_p)
GO

alter table facts_migration
	add constraint fk_m_company foreign key (id_com) references dim_companies (id_c)
GO

alter table facts_migration
	add constraint fk_m_education foreign key (id_edu) references dim_edu_achieved (id_e)
GO



if not exists (select * from sysobjects where name='edu_achieved' and xtype='U')
create table edu_achieved (
id_e int identity(1,1) NOT NULL, -- Auto_increment: empieza en el valor uno y aumenta de uno en uno.
_period varchar(10) NOT NULL,
_year int NOT NULL,
_state varchar(100) NOT NULL,
province varchar(100) NOT NULL,
edu_ach varchar(120) NOT NULL,
perc real NOT NULL
)

GO

alter table edu_achieved
	add primary key (id_e)

GO



if not exists (select * from sysobjects where name='companies' and xtype='U')
create table companies (
id_c int identity(1,1) NOT NULL, -- Auto_increment: empieza en el valor uno y aumenta de uno en uno.
_period varchar(10) NOT NULL,
_year int nOT NULL,
_state varchar(100) NOT NULL,
province varchar(100) NOT NULL,
quantity int NOT NULL,
class varchar(120) NOT NULL
)

GO

alter table companies
	add primary key (id_c)

GO


if not exists (select * from sysobjects where name='poverty' and xtype='U')
create table poverty (
id_p int identity(1,1) NOT NULL, -- Auto_increment: empieza en el valor uno y aumenta de uno en uno.
_period varchar(10) NOT NULL,
_year int NOT NULL,
_state varchar(100) NOT NULL,
province varchar(100) NOT NULL,
class varchar(120) NOT NULL,
perc real NOT NULL
)

GO

alter table poverty
	add primary key (id_p)

GO


if not exists (select * from sysobjects where name='migration' and xtype='U')
create table migration (
id int identity(1,1) NOT NULL, -- Auto_increment: empieza en el valor uno y aumenta de uno en uno.
_period varchar(10) NOT NULL,
_year int NOT NULL,
_state varchar(100) NOT NULL,
province varchar(100) NOT NULL,
flow int NOT NULL,
age int NOT NULL
)

GO

alter table migration
	add primary key (id)

GO