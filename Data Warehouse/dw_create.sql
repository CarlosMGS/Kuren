
CREATE DATABASE Mineria

USE Mineria

if not exists (select * from sysobjects where name='dim_edu_achieved' and xtype='U')
create table dim_edu_achieved (
id_e int identity(1,1) NOT NULL, -- Auto_increment: empieza en el valor uno y aumenta de uno en uno.
analfabets real NOT NULL, -- Analfabetos
primaria_inco real NOT NULL, -- Estudios primarios incompletos
primaria real NOT NULL, -- Educacion primaria
first_secun real NOT NULL, -- Primera etapa de Educación Secundaria y similar
second_secun real NOT NULL, -- Segunda etapa de educación secundaria, con orientacion general
second_secun_pro real NOT NULL, -- Segunda etapa de educación secundaria, con orientacion profesional
superior real NOT NULL -- Educacion superior
)

GO

alter table dim_edu_achieved
	add primary key (id_e)

GO

if not exists (select * from sysobjects where name='dim_companies' and xtype='U')
create table dim_companies (
id_c int identity(1,1) NOT NULL, -- Auto_increment: empieza en el valor uno y aumenta de uno en uno.
anonima int NOT NULL, -- Sociedades anonimas
resp_limit int NOT NULL, -- Sociedades de responsabilidad limitada
colectiva int NOT NULL, -- Sociedades colectivas
comanditaria int NOT NULL, -- Sociedades comanditarias
bienes int NOT NULL, -- Comunidades de bienes
coop int NOT NULL, -- Sociedades cooperativas
asociaciones int NOT NULL, -- Asociaciones y otros tipos
autonomos int NOT NULL, -- Organizaciones autonomas y otros
personas int NOT NULL -- Personas fisicas
)

GO

alter table dim_companies
	add primary key (id_c)

GO


if not exists (select * from sysobjects where name='dim_poverty' and xtype='U')
create table dim_poverty (
id_p int identity(1,1) NOT NULL, -- Auto_increment: empieza en el valor uno y aumenta de uno en uno.
tasa_riesgo real NOT NULL, --Tasa de riesgo de pobreza o exclusión social
riesgo_pobreza real NOT NULL, --Riesgo de pobreza
car_material real NOT NULL, --Carencia material severa
hog_baja_int real NOT NULL --Hogar con baja intensidad en el trabajo
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