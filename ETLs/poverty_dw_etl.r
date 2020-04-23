library("RODBC")
library(rjson)
library(plyr)

library(jsonlite)
detach("package:jsonlite", unload=TRUE)

#establecemos el directorio de trabajo
#setwd("C:/Users/CGil/Documents/MIN")


#lectura del json
data <- fromJSON(file= "10011.json")
data
str(data)
data.class(data)



#creamos tres listas para las dimensiones
provinces <- list()
ages <- list()
risks <- list()

provinces <-NULL
ages <-NULL
risks <- NULL

provinces[80] <- "aaa"
ages[80] <- "aaa"
risks[80] <- "aaa"

typeof(ages)


#rellenamos las dimensiones
for(i in 1:80){
  provinces[i] <- data[[i]][[5]][[1]][3]
  ages[i] <- data[[i]][[5]][[2]][3]
  risks[i] <- data[[i]][[5]][[3]][3]
  
}

#convertimos las listas a arrays para rellenar el df
provinces <- unlist(provinces, use.names=FALSE)
ages <- unlist(ages, use.names=FALSE)
risks <- unlist(risks, use.names=FALSE)

#creamos el dataframe
values <- data.frame(
  "province" = provinces,
  "ages"= ages,
  "risk"=risks,
  "2018"=1:80,
  "2017"=1:80,
  "2016"=1:80,
  "2015"=1:80,
  "2014"=1:80,
  "2013"=1:80,
  "2012"=1:80,
  "2011"=1:80,
  "2010"=1:80,
  "2009"=1:80,
  "2008"=1:80
)

values

#rellenamos los valores para los porcentajes
for(i in 1:80){
  
  for(j in 1:11){
    
    values[i, j+3]<-data[[i]][[6]][[j]][5]
    
  }
  
}

values

colnames(values)

values <- values[values$province != "Total Nacional",]
values <- values[,-2]


#transformamos las columnas de los años en una unica columna añadiendo mas filas

values <- values[rep(seq_len(nrow(values)), each=11),]
values["year"] <- rep(2008:2018, 76)
values["prct"] <- 1:836

for(i in 0:75){
  for(j in 1:11){
    values[i*11 + j,15] <- values[i*11+j,2+j]
  }
}

#eliminamos las columnas sobrantes
for(i in 1:11){
  values <- values[,-3]
}

#conexion con la base de datos
con <- odbcDriverConnect('driver={SQL Server};server=localhost;database=Mineria;trusted_connection=true')

pov_loaded <- sqlQuery(con, "select * from dbo.poverty")

if(nrow(values) > nrow(pov_loaded)){
  for (i in 1:nrow(values)){
    insert_query <- paste("INSERT INTO dbo.poverty (_period, _year, _state, province, class, perc )
             VALUES ('Full','", values$year[i], "','",values$province[i],"','Full','",values$risk[i],"','",values$prct[i],"')", sep="")
  
    sqlQuery(con, insert_query)
  }
}

