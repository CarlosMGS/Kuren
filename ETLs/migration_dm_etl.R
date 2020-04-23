library(RODBC)
library(odbc)
library(dplyr)
library(pracma)
library(rjson)

#establecemos conexi?n
con <- odbcDriverConnect("driver={SQL Server Native Client 11.0};Server=DESKTOP-BM96OLK ; Database=Mineria;Uid=; Pwd=; trusted_connection=yes")

#setwd("C:/Users/dipdn/Desktop/MIN")

data <- sqlQuery(con, "select * from dbo.migration")

data <- data[,-1]

colnames(data)[1] <- "Trimestre"
colnames(data)[2] <- "Year"
colnames(data)[3] <- "Comunidad"
colnames(data)[4] <- "Provincia"
colnames(data)[5] <- "Total"
colnames(data)[6] <- "Edad"



#agrupamos por años y comunidades paraa adaptar los datos
data <- group_by(data, Comunidad, Edad, Year) %>% summarise(sum <- sum(Total))

colnames(data)[4] <- "Total"

data$Total[is.na(data$Total)] <- 0
data <- data[data$Edad < 1000,]

unique(data$Edad)

unique(data$Year)

#tabla de migraciones lista, sufrirá modificaciones porque debemos guardar varios datos para cada fila




#cargamos fuentes de dimensiones
#empresas
company <- sqlQuery(con, "select * from dbo.companies")

company <- company[,-1]
company <- company[,-1]

#eliminamos el total y nos quedamos con aquellas filas con un tipo de empresa específico
colnames(company)[1]<-"Year"
colnames(company)[2]<-"Comunidad"
colnames(company)[3]<-"Provincia"
colnames(company)[4]<-"Total"
colnames(company)[5]<-"Condicion"


#eliminamos los años anteriores a 2008
company <- company[company$Year > 2007,]

company <- group_by(company, Comunidad, Condicion, Year) %>% summarise(sum <- sum(Total))
colnames(company)[4]<- "Total"

#Añadimos a los hechos el id por cada tipo de empresa
unique(company$Condicion)
data <- data[rep(seq_len(nrow(data)), each=9),]
data["id_c"] <- rep(1:9, nrow(data)/9)


data$Comunidad <- as.character(data$Comunidad)
company$Comunidad <- as.character(company$Comunidad)
company$Condicion <- as.character(company$Condicion)

data <- data [order(data[,1],data[,3],data[,5]), ]
company <- company[order(company[,1],company[,3]),]

for(i in 0:nrow(company)-1){
  

  for(j in 0:90){
  
    data[i*91+j+1,5] <- i+1
  
  }
}

company$Total <- as.integer(company$Total)
#cargamos los datos de empresas en su correspondiente dimension en el DW
dim_com <- sqlQuery(con, "SELECT * FROM dbo.dim_companies")

for(i in 1:nrow(company)){
    insert_query <- paste("INSERT INTO dbo.dim_companies (quantity, class)
             VALUES ('", company[i,4], "','",company$Condicion[i],"')", sep="")
    
    sqlQuery(con, insert_query)
}


#----------------------------------------------------------------------------------------------------#
#educacion
education <- sqlQuery(con, "select * from dbo.edu_achieved")



unique(edudf$achieved)
data <- data[rep(seq_len(nrow(data)), each=7),]
data["id_e"] <- rep(1:7, nrow(data)/7)

data <- data [order(data[,3],data[,1],data[,6]), ]
edudf <- edudf[order(edudf[,3],edudf[,1]),]

data$Year<-as.numeric(data$Year)

for(i in 0:((nrow(edudf)/2)-1)){
  for(j in 0:1637){
      data[i*1638+j+1,6] <- NA
  }
}


for(i in 0:(nrow(edudf)-1)){
  for(j in 0:(1637/2)){
    data[i*819+(nrow(data)/2)+j+1,6] <- i+1
  }
}

#cargamos los datos de la educacion conseguida en su correspondiente dimension en el DW
dim_edu <- sqlQuery(con, "SELECT * FROM dbo.dim_edu_achieved")

if(nrow(dim_edu) < nrow(edudf)){
  for(i in 1:nrow(edudf)){
    
      insert_query <- paste("INSERT INTO dbo.dim_edu_achieved (edu_ach, perc)
             VALUES ('", edudf$achieved[i], "','",edudf[i,4],"')", sep="")
    
    sqlQuery(con, insert_query)
  }
}

#----------------------------------------------------------------------------------------#
#pobreza
poverty <- fromJSON(file="10011.json")

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
  provinces[i] <- poverty[[i]][[5]][[1]][3]
  ages[i] <- poverty[[i]][[5]][[2]][3]
  risks[i] <- poverty[[i]][[5]][[3]][3]
  
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
    
    values[i, j+3]<-poverty[[i]][[6]][[j]][5]
    
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

values <- values[order(values[,1], values[,3]), ]

dim_pov <- sqlQuery(con, "SELECT * FROM dbo.dim_poverty")

data <- data[rep(seq_len(nrow(data)), each=4),]
data["id_p"] <- rep(1:4, nrow(data)/4)

data <- data [order(data[,1],data[,3],data[,5]), ]

values <- values[order(values[,3], values[,1]), ]
data <- data [order(data[,3], data[,1]), ]


for(i in 0:(nrow(values)-1)){
  for(j in 0:90*7*9){
    data[i*91*7*9+j+1,7] <- i+1
  }
}

r <- 76077*7*9
h <- 82992*7*9

for(i in r:h){
  data[i, 7] <- NA
}

for(i in 1:nrow(values)){
  
  insert_query <- paste("INSERT INTO dbo.dim_poverty (class, perc)
           VALUES ('", values$risk[i], "','",values$prct[i],"')", sep="")
  
  sqlQuery(con, insert_query)
}
 