library(RODBC)
library(odbc)
library(dplyr)
library(pracma)
library(rjson)

#establecemos conexi?n
con <- odbcDriverConnect("driver={SQL Server Native Client 11.0};Server=localhost ; Database=Mineria;Uid=; Pwd=; trusted_connection=yes")

setwd("C:/Users/CGIL/Documents/MIN")

data <- read.csv2("24448.csv", encoding = "UTF-8")

colnames(data)[1] <- "Provincias"

data$Provincias <- gsub("[1-90]", "", data$Provincias)
data$Provincias <- gsub(" ", "", data$Provincias)

data$Edad <- gsub("[^1-90]", "", data$Edad)
data$Edad = as.integer(data$Edad)
data$Edad[is.na(data$Edad)] = 1000

data$Year <- substr(data$Periodo, 1,4) 
data$Periodo <- substr(data$Periodo, 5,6) 

data<- transform(data,Total <- as.integer(Total))

#cargamos csv con la relaci?n entre Comunidades y Provincias
prov_ca <- read.csv2("list-pro.csv", sep=";" , header = TRUE, encoding="UTF-8")
prov_ca <- prov_ca[-1]
prov_ca <- prov_ca[-3]
prov_ca <- prov_ca[-2]
prov_ca <- prov_ca[-2]
prov_ca <- prov_ca[-2]


colnames(prov_ca)[1] <- "Provincias"
colnames(prov_ca)[2] <- "Comunidades"

data <- data[data$Sexo == "Ambos sexos",]
unique(data$Sexo)

data <- data[-2]

data <- data[data$Edad < 1000,]


mig_loaded <- sqlQuery(con, "select * from dbo.migration")

if(nrow(data) > nrow(mig_loaded)){
  for (i in 1:nrow(data)){
    c_index <- match(data$Provincias[i], prov_ca$Provincias)
    insert_query <- paste("INSERT INTO dbo.migration (_period, _year, _state, province, flow, age )
             VALUES ('",data$Periodo[i],"','", data$Year[i], "','",prov_ca$Comunidades[c_index],"','",data$Provincias[i],"','",data$Total[i],"','",data$Edad[i],"')", sep="")
    
    sqlQuery(con, insert_query)
  }
}
