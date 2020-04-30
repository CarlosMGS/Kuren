library(RODBC)
library(odbc)
library(dplyr)
library(pracma)
library(rjson)

#establecemos conexi?n
con <- odbcDriverConnect("driver={SQL Server Native Client 11.0};Server=localhost ; Database=Mineria;Uid=; Pwd=; trusted_connection=yes")

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



data$Comunidad <- as.character(data$Comunidad)
company$Comunidad <- as.character(company$Comunidad)
company$Condicion <- as.character(company$Condicion)

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
    insert_query <- paste("INSERT INTO dbo.dim_companies (quantity, class)
             VALUES ('", company[i,4], "','",company$Condicion[i],"')", sep="")
    
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


unique(edudf$Achieved)


data <- data [order(data[,3],data[,1],data[,6]), ]
edudf <- edudf[order(edudf[,1],edudf[,2]),]

data$Year<-as.numeric(data$Year)



#cargamos los datos de la educacion conseguida en su correspondiente dimension en el DW
dim_edu <- sqlQuery(con, "SELECT * FROM dbo.dim_edu_achieved")

if(nrow(dim_edu) < nrow(edudf)){
  for(i in 1:nrow(edudf)){
    
      insert_query <- paste("INSERT INTO dbo.dim_edu_achieved (edu_ach, perc)
             VALUES ('", edudf$Achieved[i], "','",edudf[i,5],"')", sep="")
    
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

values <- poverty
values <- values[order(values[,1], values[,2]), ]

dim_pov <- sqlQuery(con, "SELECT * FROM dbo.dim_poverty")


values["id_p"] <- 1: nrow(values)

data <- data [order(data[,1],data[,3],data[,7]), ]

data <- data [order(data[,3], data[,1]), ]
values <- values[order(values[,1], values[,2]), ]



for(i in 1:nrow(values)){
  
  insert_query <- paste("INSERT INTO dbo.dim_poverty (class, perc)
           VALUES ('", values$Tipo[i], "','",values$Porcentaje[i],"')", sep="")
  
  sqlQuery(con, insert_query)
}


for(i in 1:nrow(data)){
  if(is.na(data$id_e[i])){
    
    insert_query <- paste("INSERT INTO dbo.facts_migration (comunidad, n_year, flow, age, id_pov, id_com, id_edu)
           VALUES ('", data$Comunidad[i], "','",data$Year[i], "','",data$Total[i], "','",data$Edad[i], "','",data$id_p[i], "','",data$id_com[i], "', NULL)", sep="")
  
  }else if(is.na(data$id_p[i])){
      
    insert_query <- paste("INSERT INTO dbo.facts_migration (comunidad, n_year, flow, age, id_pov, id_com, id_edu)
           VALUES ('", data$Comunidad[i], "','",data$Year[i], "','",data$Total[i], "','",data$Edad[i], "', NULL ,'",data$id_com[i], "','",data$id_edu[i],"')", sep="")
    
  }else{
    
    insert_query <- paste("INSERT INTO dbo.facts_migration (comunidad, n_year, flow, age, id_pov, id_com, id_edu)
           VALUES ('", data$Comunidad[i], "','",data$Year[i], "','",data$Total[i], "','",data$Edad[i], "','",data$id_p[i], "','",data$id_com[i], "','",data$id_edu[i],"')", sep="")
    
  }
  
  sqlQuery(con, insert_query)
  
}
 