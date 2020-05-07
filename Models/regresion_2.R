library(RODBC)
library(dplyr)
library(stats)
library(plyr)


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

#Funcion lm() con todos los datos
fit <- lm(mig$flow ~ ., mig)

#attributes(fit)
#summary

#4 graficos a la vez
layout(matrix(c(1,2,3,4),2,2)) # optional 4 graphs/page
plot(fit)

#Cajas y bigotes
boxplot(mig$flow,horizontal =T ) 

#Histograma
hist(mig$flow, xlab="",ylab="",main="") 

#Grafica de la regresion lineal
plot(mig[c("n_year", "flow")])

abline(lm(mig$flow ~ mig$n_year))

#Grafica de densidad
d <- density(mig$flow)
plot(d, main="Densidad")
polygon(d, col="red", border="blue")


############################################
#####Funcion para mostrar por comunidad#####
############################################

grafPorComunidad <- function(n){
  div <- mig[mig$comunidad == n, ]
  
  #Funcion lm() con todos los datos
  div <- div[,-1]
  fit <- lm(div$flow ~ ., div)
  
  #attributes(fit)
  summary(fit)
  
  #4 graficos a la vez
  layout(matrix(c(1,2,3,4),2,2)) # optional 4 graphs/page
  plot(fit, main = n)
  
  #Cajas y bigotes
  boxplot(div$flow,horizontal =T, main = n ) 
  
  #Histograma
  hist(div$flow, xlab="",ylab="",main="") 
  
  #Grafica de la regresion lineal
  plot(div[c("n_year", "flow")])
  
  abline(lm(div$flow ~ div$n_year))
  
  #Grafica de densidad
  d <- density(div$flow)
  plot(d, main="Densidad")
  polygon(d, col="red", border="blue")
}

#Prueba con CataluÒa
grafPorComunidad("CataluÒa")

#Eliminamos duplicados
com <- mig$comunidad[!duplicated(mig$comunidad)]

#Se hace por cada COmunidad
for(n in 1:19){
  grafPorComunidad(com[n]);
}


################################################
#####Funcion para mostrar por grupo de edad#####
################################################

# 0-17 menores
# 18-38 jÛvenes
# 39-65 adultos
# +65 3a edad

#aÒadimos columna para la categorÌa en una copia
mig2 <- mig
mig2["grupo_edad"] <- ""

#ifs para colocar cada edad en su categoria

for(i in 1:nrow(mig2)){
  if(mig$age[i] < 18){
    mig2$grupo_edad[i] <- "menores"
  }else if(mig$age[i] >= 18 && mig$age[i] < 39){
    mig2$grupo_edad[i] <- "jÛvenes"
  }else if(mig$age[i] >= 39 && mig$age[i] < 66){
    mig2$grupo_edad[i] <- "adultos"
  }else{
    mig2$grupo_edad[i] <- "tercera_edad"
  }
}

unique(mig2$grupo_edad)


# agrupamos por las categorÌas de edad

mig3 <- mig2

columnas <- colnames(mig2)[-4]
columnas <- columnas[-3]
mig3<-plyr::ddply(mig3, columnas, plyr::summarize, flow=sum(flow))

#cuando todo funciona bien copiamos en mig para sacar las graficas de cada comunidad en funcion del grupo de edad
mig <- mig3

com <- unique(mig$comunidad)

for(n in 1:19){
  grafPorComunidad(com[n]);
}

