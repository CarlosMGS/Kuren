library(RODBC)
library(dplyr)
library(stats)
library(plyr)
library(tidyr)

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

apost <- function(fit, test){
  
  print("Error a posteriori")
  
  pre <- predict(fit, test, interval = "confidence")
  
  residuosReales <- test$flow - as.integer(pre)
  
  ECMaPosteriori <- sum(residuosReales^2)/length(residuosReales)
  print(ECMaPosteriori)
  
  EMAaPosteriori <- sqrt(ECMaPosteriori)
  
  EMRaPosteriori <- EMAaPosteriori / mean(test$flow)
  print(EMRaPosteriori)
  
}

regresion <- function(train){
  #REGRESION 80 - 20 #Se borro y no se como se hace ahora
  
  fit <- lm(flow ~ ., data = train)
  #head(predict(fit, test, interval = "confidence"), 10)
  

  #Calculo del error medio absoluto "teorico" o "a priori" de la regresion
  ECMaPriori <- sum((residuals(fit)^2))/length(residuals(fit))
  print(ECMaPriori)
  
  EMAaPriori <- sqrt(ECMaPriori)
  
  #Calculo del error medio relativo (o porcentual) "teorico" o "a priori" de la regresion
  #CPI es la suma de los ecoeficinetes Intercept year quarter del ejemplo, transparencia 19
  EMRaPriori <- EMAaPriori / mean(train$flow)
  
  print(EMRaPriori)
  
  return (fit)
}


grafPorComunidad <- function(n, df){
  div <- df[df$comunidad == n, ]
  
  #Funcion lm() con todos los datos
  div <- div[,-1]
  
  div <- div[,-6]
  
  sample <- floor(0.9 * nrow(div))
  train.index <- sample(seq_len(nrow(div)), size = sample)
  train <- div[train.index, ]
  test <- div[- train.index, ]
  
  nc <- paste("Error cuadratico medio de la comunidad ", n)
  print(nc)
  
  r <- regresion(train)
  apost(r, test)
  
  #4 graficos a la vez
  layout(matrix(c(1,2,3,4),2,2)) # optional 4 graphs/page
  plot(r, main = n)
  
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
grafPorComunidad("CataluÒa", mig)




bucleCom <- function(df){
  #Eliminamos duplicados
  com <- df$comunidad[!duplicated(df$comunidad)]

  for(n in 1:19){
   grafPorComunidad(com[n], df);
  }
}




################################################
#####Funcion para mostrar por grupo de edad#####
################################################

# 0-17 menores
# 18-38 jÛvenes
# 39-65 adultos
# +65 3a edad

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


#Separado por edades
#Menores

mig3 <- mig3[,-12]
mig3 <- mig3[,-12]
mig3 <- mig3[,-12]
mig3 <- mig3[,-12]
mig3 <- mig3[,-12]
mig3 <- mig3[,-12]
mig3 <- mig3[,-12]

mig3["Empresas"] <- 1:nrow(mig3)
mig3["tipoE"] <- " "

mig3 <- gather(data = mig3, key = "tipoE", value = "Empresas", 3:11)

columnas <- colnames(mig3)[-9]
columnas <- columnas[-9]
mig3<-plyr::ddply(mig3, columnas, plyr::summarize, Empresas=sum(Empresas))

bucleCom(mig3)
migMenores <- mig3
migMenores <- migMenores[mig3$grupo_edad == "menores", ]

#Jovenes
migJovenes <- mig3
migJovenes <- migJovenes[mig3$grupo_edad == "jÛvenes", ]

#Adultos
migAdultos <- mig3
migAdultos <- migAdultos[mig3$grupo_edad == "adultos", ]

#3a Edad
mig3aEdad <- mig3
mig3aEdad <- mig3aEdad[mig3$grupo_edad == "tercera_edad", ]

bucleCom(migMenores)

bucleCom(migJovenes)
bucleCom(migAdultos)
bucleCom(mig3aEdad)

