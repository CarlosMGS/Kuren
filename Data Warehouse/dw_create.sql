
CREATE DATABASE Mineria

USE Mineria

if not exists (select * from sysobjects where name='dim_edu_achieved' and xtype='U')
create table dim_edu_achieved (
id_e int identity(1,1) NOT NULL, -- Auto_increment: empieza en el valor uno y aumenta de uno en uno.
edu_ach varchar(100) NOT NULL,
perc int NOT NULL
)

GO

alter table dim_edu_achieved
	add primary key (id_e)

GO

if not exists (select * from sysobjects where name='dim_companies' and xtype='U')
create table dim_companies (
id_c int identity(1,1) NOT NULL, -- Auto_increment: empieza en el valor uno y aumenta de uno en uno.
quantity int NOT NULL,
class varchar(100) NOT NULL
)

GO

alter table dim_companies
	add primary key (id_c)

GO


if not exists (select * from sysobjects where name='dim_poverty' and xtype='U')
create table dim_poverty (
id_p int identity(1,1) NOT NULL, -- Auto_increment: empieza en el valor uno y aumenta de uno en uno.
class varchar(100) NOT NULL,
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

