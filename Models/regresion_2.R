library(RODBC)
library(dplyr)
library(stats)


#liberamos memoria
rm(list = ls())
gc()

#conexi√≥n con la base de datos
con <- odbcDriverConnect("driver={SQL Server Native Client 11.0};Server=min-serv.database.windows.net ; Database=Mineria;Uid=usuariomin; Pwd=dctaMineria5")

#query para traer los datos de trabajo
data <- sqlQuery(con, "select *
                      from facts_migration left join dim_companies on (facts_migration.id_com = dim_companies.id_c)
                                           left join dim_edu_achieved on (facts_migration.id_edu = dim_edu_achieved.id_e)
					                                 left join dim_poverty on (facts_migration.id_pov = dim_poverty.id_p)")
#hacemos una copia para trabajar
mig <- data

#eliminamos ids
mig <- mig[,-1]
mig <- mig[,-5]
mig <- mig[,-5]
mig <- mig[,-5]
mig <- mig[,-5]
mig <- mig[,-22]
mig <- mig[,-14]

#cambiamos los NA por -1 para que funcione 
for(i in 1:length(mig)){
  mig[is.na(mig[,i]), i] <- -1
}

cor(mig$flow, mig$car_material)

andalusia30 <- mig[mig$comunidad=="AndalucÌa" && mig$age == 30, 3]

plot(cpi, xaxt="n", ylab="CPI", xlab="")

