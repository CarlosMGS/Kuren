library(RODBC)
library(odbc)
library(dplyr)
library(pracma)
library(rjson)

#establecemos conexi?n
con <- odbcDriverConnect("driver={SQL Server Native Client 11.0};Server=localhost ; Database=Mineria;Uid=; Pwd=; trusted_connection=yes")
#con <- odbcDriverConnect("driver={SQL Server Native Client 11.0};Server=min-serv.database.windows.net ; Database=Mineria;Uid=usuariomin; Pwd=dctaMineria5")

setwd("C:/Users/dipdn/Desktop/MIN")

data <- sqlQuery(con, "select * from dbo.migration")

data <- data[,-1]

colnames(data)[1] <- "Trimestre"
colnames(data)[2] <- "Year"
colnames(data)[3] <- "Comunidad"
colnames(data)[4] <- "Provincia"
colnames(data)[5] <- "Total"
colnames(data)[6] <- "Edad"



#agrupamos por años y comunidades paraa adaptar los datos
data <- group_by(data, Comunidad, Edad,Year) %>% summarise(sum <- sum(Total))

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


install.packages("tidyr")
library(tidyr)

company <- spread(company, Condicion, Total)


data$Comunidad <- as.character(data$Comunidad)
company$Comunidad <- as.character(company$Comunidad)


##########################################

data <- data [order(data[,1],data[,3]), ]
company <- company[order(company$Year,company$Comunidad),]

company["id_c"] <- 1:nrow(company)

data <- left_join(data, company, by=c("Year", "Comunidad"))


################################################


company$Total <- as.integer(company$Total)
#cargamos los datos de empresas en su correspondiente dimension en el DW
dim_com <- sqlQuery(con, "SELECT * FROM dbo.dim_companies")

for(i in 1:nrow(company)){
    insert_query <- paste("INSERT INTO dbo.dim_companies (anonima , resp_limit, colectiva, comanditaria, bienes, coop, asociaciones, autonomos, personas)
             VALUES ('",company[i,7], "','",company[i,11], "','",company[i,8], "','",company[i,9],"','",company[i,4], "','",company[i,10], "','",company[i,3], "','",company[i,5], "','",company[i,6], "')", sep="")
    
    sqlQuery(con, insert_query)
}


#----------------------------------------------------------------------------------------------------#
#educacion
edudf <- sqlQuery(con, "select * from dbo.edu_achieved where _period = 'T4'")

edudf <- edudf[,-1]
edudf <- edudf[,-1]



colnames(edudf)[1]<-"Year"
colnames(edudf)[2]<-"Comunidad"
colnames(edudf)[3]<-"Provincia"
colnames(edudf)[4]<-"Achieved"
colnames(edudf)[5]<-"Porcentaje"

edudf$Comunidad <- as.character(edudf$Comunidad)

edudf$Comunidad <- replace(edudf$Comunidad, edudf$Comunidad == "Castilla - La Mancha", "Castilla-La Mancha")

edudf$Comunidad <- as.factor(edudf$Comunidad)

unique(edudf$Achieved)

data$Year<-as.numeric(data$Year)



edudf <- spread(edudf, Achieved, Porcentaje)



data <- data [order(data[,1],data[,3]), ]
edudf <- edudf[order(edudf$Year,edudf$Comunidad),]

edudf["id_e"] <- 1:nrow(edudf)

data <- left_join(data, edudf, by=c("Year", "Comunidad"))


unique(data$Comunidad)




#cargamos los datos de la educacion conseguida en su correspondiente dimension en el DW
dim_edu <- sqlQuery(con, "SELECT * FROM dbo.dim_edu_achieved")

if(nrow(dim_edu) < nrow(edudf)){
  for(i in 1:nrow(edudf)){
    
      insert_query <- paste("INSERT INTO dbo.dim_edu_achieved (analfabets, primaria_inco, primaria, first_secun, second_secun, second_secun_pro, superior)
             VALUES ('", edudf[i,4], "','", edudf[i,7], "','", edudf[i,5],  "','", edudf[i,8],"','", edudf[i,10], "','", edudf[i,9], "','",edudf[i,6],"')", sep="")
    
    sqlQuery(con, insert_query)
  }
}

#----------------------------------------------------------------------------------------#
#pobreza
poverty <- sqlQuery(con, "select * from dbo.poverty")

poverty <- poverty[,-1]
poverty <- poverty[,-1]

colnames(poverty)[1]<-"Year"
colnames(poverty)[2]<-"Comunidad"
colnames(poverty)[3]<-"Provincia"
colnames(poverty)[4]<-"Tipo"
colnames(poverty)[5]<-"Porcentaje"

poverty$Comunidad <- as.character(poverty$Comunidad)

poverty$Comunidad <- replace(poverty$Comunidad, poverty$Comunidad == "Castilla - La Mancha", "Castilla-La Mancha")

poverty$Comunidad <- as.factor(poverty$Comunidad)


values <- poverty
values <- values[order(values[,1], values[,2]), ]

dim_pov <- sqlQuery(con, "SELECT * FROM dbo.dim_poverty")


#values["id_p"] <- 1: nrow(values)

#data <- data [order(data[,1],data[,3],data[,7]), ]

#data <- data [order(data[,3], data[,1]), ]
#values <- values[order(values[,1], values[,2]), ]

values <- spread(values, Tipo, Porcentaje)


data <- data [order(data$Year,data$Comunidad), ]
values <- values[order(values$Year,values$Comunidad),]

values["id_p"] <- 1:nrow(values)

data <- left_join(data, values, by=c("Year", "Comunidad"))




for(i in 1:nrow(values)){
  
  insert_query <- paste("INSERT INTO dbo.dim_poverty (tasa_riesgo, riesgo_pobreza, car_material, hog_baja_int )
           VALUES ('", values[i,6], "','", values[i,5], "','", values[i,4], "','",values[i,7],"')", sep="")
  
  sqlQuery(con, insert_query)
}

colnames(data)[4] <- "Flow"

insert_query
########################################################################
for(i in 1:nrow(data)){
  if(is.na(data$id_e[i])){
    
    insert_query <- paste("INSERT INTO dbo.facts_migration (comunidad, n_year, flow, age, id_pov, id_com, id_edu)
           VALUES ('", data$Comunidad[i], "','",data$Year[i], "','",data$Flow[i], "','",data$Edad[i], "','",data$id_p[i], "','",data$id_c[i], "', NULL)", sep="")
  
  }else if(is.na(data$id_p[i])){
      
    insert_query <- paste("INSERT INTO dbo.facts_migration (comunidad, n_year, flow, age, id_pov, id_com, id_edu)
           VALUES ('", data$Comunidad[i], "','",data$Year[i], "','",data$Flow[i], "','",data$Edad[i], "', NULL ,'",data$id_c[i], "','",data$id_e[i],"')", sep="")
    
  }else{
    
    insert_query <- paste("INSERT INTO dbo.facts_migration (comunidad, n_year, flow, age, id_pov, id_com, id_edu)
           VALUES ('", data$Comunidad[i], "','",data$Year[i], "','",data$Flow[i], "','",data$Edad[i], "','",data$id_p[i], "','",data$id_c[i], "','",data$id_e[i],"')", sep="")
    
  }
  
  sqlQuery(con, insert_query)
  
}
 