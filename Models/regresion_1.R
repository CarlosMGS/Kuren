library(RODBC)
library(dplyr)
library(stats)


#cargamos los datos del DW
con <- odbcDriverConnect("driver={SQL Server Native Client 11.0};Server=DESKTOP-BM96OLK ; Database=Mineria;Uid=; Pwd=; trusted_connection=yes")

set.seed(8976)

mig <- sqlQuery(con, "select comunidad, n_year, flow, age, id_com, dim_companies.class, quantity, id_edu, edu_ach, dim_edu_achieved.perc,
                             id_pov, dim_poverty.class, dim_poverty.perc
                      from facts_migration left join dim_companies on (facts_migration.id_com = dim_companies.id_c)
                                           left join dim_edu_achieved on (facts_migration.id_edu = dim_edu_achieved.id_e)
					                                 left join dim_poverty on (facts_migration.id_pov = dim_poverty.id_p)")

mig2 <- sqlQuery(con, "select * from dbo.facts_migration")
gc()

unique(mig$class)

#mig[mig$id_com == 2054,]

#copiamos los datos en otro df para manipular columnas
mig2 <- mig

mig2 <- group_by(mig2,comunidad,n_year , id_pov,class.1,perc.1,id_com,class,quantity, id_edu, edu_ach, perc) %>% summarise(sum <- sum(flow))
mig2 <- group_by(mig2,comunidad,n_year , id_pov,id_com, id_edu) %>% summarise(sum <- sum(flow))



#renombramos las columnas para evitar confusiones
colnames(mig2)[4] <- "nivel_pobreza"
colnames(mig2)[5] <- "pov_perc"
colnames(mig2)[7] <- "tipo_empresa"
colnames(mig2)[8] <- "com_quantity"
colnames(mig2)[10] <- "nivel_educativo"
colnames(mig2)[11] <- "edu_perc"
colnames(mig2)[12] <- "flow"

#TODO gestionar NA para hallar la correlacion
cor(mig2$com_quantity, mig2$flow)
cor(mig2$pov_perc, mig2$flow)
cor(mig2$edu_perc, mig2$flow)


unique(mig2$n_year)

and_flow <- unique(mig2[mig2$comunidad == "Andalucía" , 6])

plot(x = unique(mig2$n_year),y=unique(mig2[mig2$comunidad == "Andalucía", 12]), xaxt="n", ylab="saldo migratorio", xlab="")
# Dibuja el eje de abscisas
axis(1, labels=unique(mig2$n_year), at=1:12, las=3)

