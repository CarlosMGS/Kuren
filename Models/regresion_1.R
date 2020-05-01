library(RODBC)
library(dplyr)
library(stats)


#liberamos memoria
rm(list = ls())
gc()

#conexion a la bd
con <- odbcDriverConnect("driver={SQL Server Native Client 11.0};Server=DESKTOP-BM96OLK ; Database=Mineria;Uid=; Pwd=; trusted_connection=yes")

#establecemos semilla para la aleatoriedad
set.seed(89767)

#realizamos consulta para cargar todos los hechos con sus dimensiones
mig <- sqlQuery(con, "select *
                      from facts_migration left join dim_companies on (facts_migration.id_com = dim_companies.id_c)
                                           left join dim_edu_achieved on (facts_migration.id_edu = dim_edu_achieved.id_e)
					                                 left join dim_poverty on (facts_migration.id_pov = dim_poverty.id_p)")

#comporbamos los nombres de las columnas de nuestro df
colnames(mig)

#hacemos la copia para trabajar
mig2 <- mig

#quitamos las columnas de los ids
mig2$id <- NULL
mig2$id_c <- NULL
mig2$id_com <- NULL
mig2$id_e <- NULL
mig2$id_edu <- NULL
mig2$id_p <- NULL
mig2$id_pov <- NULL

#quitamos el campo comunidad 
#mig2$comunidad <- NULL

#cambiamos los NA por -1 para que funcione el kmeans 
for(i in 1:length(mig2)){
  mig2[is.na(mig2[,i]), i] <- -1
}



