library(RODBC)
library(dplyr)
library(stats)


#liberamos memoria
rm(list = ls())
gc()

#conexión con la base de datos
con <- odbcDriverConnect("driver={SQL Server Native Client 11.0};Server=min-serv.database.windows.net ; Database=Mineria;Uid=usuariomin; Pwd=dctaMineria5")

#query para traer los datos de trabajo
data <- sqlQuery(con, "select comunidad, n_year, flow, age, id_com, dim_companies.class as com_type, quantity, id_edu, edu_ach, dim_edu_achieved.perc as edu_perc,
                             id_pov, dim_poverty.class as pov_type, dim_poverty.perc as pov_perc
                      from facts_migration left join dim_companies on (facts_migration.id_com = dim_companies.id_c)
                                           left join dim_edu_achieved on (facts_migration.id_edu = dim_edu_achieved.id_e)
					                                 left join dim_poverty on (facts_migration.id_pov = dim_poverty.id_p)")
#hacemos una copia para trabajar
mig <- data

#eliminamos ids
mig <- mig[,-5]
mig <- mig[,-7]
mig <- mig[,-9]

