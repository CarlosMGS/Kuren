library(RODBC)
library(dplyr)
library(rjson)
library(stats)
library(fpc)
library(dbscan)
library(rgl)

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
mig2$comunidad <- NULL



#cambiamos los NA por -1 para que funcione el kmeans 
for(i in 1:length(mig2)){
  mig2[is.na(mig2[,i]), i] <- -1
}





############## 1. kmeans sin agrupar por edad

#uso de kameans
(kmeans.result <- kmeans(mig2, 19))

#cruzamos la tabla para cruzar comunidades con el cluster, para ver si se puede agrupar por comunidad
table(mig$comunidad, kmeans.result$cluster)

#visualizamos los datos del kmeans
plot(mig2[c("n_year", "flow")], col = kmeans.result$cluster)
points(kmeans.result$centers[,c("n_year", "flow")], col = 1:19, pch = 8, cex=2)









################  2. uso de dbscan, que tiene en cuenta la densidad de observaciones en las distribuciones.Calcula la k
#la propia función

#utilizamos esta gráfica de k-nearest para saber qué valor de eps nos interesa para la funcion dbscan
dbscan::kNNdistplot(mig2, k =  19)
abline(h = 20, lty = 2)

ds <- dbscan(mig2, eps=5500, MinPts=5)

table(mig$comunidad, ds$cluster)

plot(ds, mig2[c("n_year","flow")])
points(kmeans.result$centers[,c("n_year", "flow")], col = 1:19, pch = 8, cex=2)

#en la tabla se ve claramente que los primeros grupos aglutinan la mayoría de las observaciones, 
#estas son las que tienen los flujos cercanos a 0








########### 3. vamos a visualizar los datos en funcion tambien de la edad en un grafico 3D

# Abre el sistema gráfico donde mostrar las gráficas
open3d() 

#pasamos el kmeans del apartado 1
plot3d(mig2[c("n_year", "flow", "age")], type = "s", col = kmeans.result$cluster)
#agrupamos por comunidades para comparar
plot3d(mig2[c("n_year", "flow", "age")], type = "s", col = as.integer(mig$comunidad))




########### 4.categorizando por grupos de edad

mig3 <- mig2

columnas <- colnames(mig2)[-4]
columnas <- columnas[-3]
mig3<-plyr::ddply(mig3, columnas, plyr::summarize, flow=sum(flow))

sum( is.na( mig3 ) ) > 0

#uso de kameans
(kmeans.result <- kmeans(mig3, 19))
