library(RODBC)
library(odbc)
library(dplyr)
library(pracma)
library(rjson)
library(stats)
library(fpc)

con <- odbcDriverConnect("driver={SQL Server Native Client 11.0};Server=DESKTOP-BM96OLK ; Database=Mineria;Uid=; Pwd=; trusted_connection=yes")

set.seed(897676676)

mig <- sqlQuery(con, "select * from dbo.facts_migration")

mig <- sqlQuery(con, "select comunidad, n_year, flow, age, id_com, dim_companies.class, quantity, id_edu, edu_ach, dim_edu_achieved.perc,
                             id_pov, dim_poverty.class, dim_poverty.perc
                      from facts_migration left join dim_companies on (facts_migration.id_com = dim_companies.id_c)
                                           left join dim_edu_achieved on (facts_migration.id_edu = dim_edu_achieved.id_e)
					                                 left join dim_poverty on (facts_migration.id_pov = dim_poverty.id_p)")

colnames(mig)

mig2 <- mig

mig2$flow <- NULL
mig2$comunidad <- NULL

mig2 <- group_by(mig2,n_year , id_pov,id_com, id_edu) %>% summarise(sum <- sum(flow))
mig <- group_by(mig,comunidad,n_year , id_pov,id_com, id_edu) %>% summarise(sum <- sum(flow))

colnames(mig2)[5] <- "flow"

#cambiamos los NA por -1 para que funcione el kmeans 
mig2[is.na(mig2$id_pov), 2] <- -1
mig2[is.na(mig2$id_edu), 4] <- -1

#uso de kameans
(kmeans.result <- kmeans(mig2, 19))

table(mig$comunidad, kmeans.result$cluster)

sum( is.na( mig2 ) ) > 0

any(is.na(mig2))

plot(mig2[c("n_year", "flow")], col = kmeans.result$cluster)

points(kmeans.result$centers[,c("n_year", "flow")], col = 1:19,
       pch = 8, cex=2)


#uso de dbscan, que tiene en cuenta la densidad de observaciones en las distribuciones.Calcula la k
#la propia función


library(dbscan)
dbscan::kNNdistplot(mig2, k =  19)
abline(h = 1000, lty = 2)

ds <- dbscan(mig2, eps=1350, MinPts=5)

table(mig$comunidad, ds$cluster)

plot(ds, mig2[c("n_year","flow")])

#en la tabla se ve claramente que hay un grupo que aglutina la mayoría de las observaciones, estas son las que tienen los flujos cercanos a 0