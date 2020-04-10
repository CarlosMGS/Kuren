library(RODBC)
library(odbc)

#establecemos conexión
dbconnection <- odbcDriverConnect("driver={SQL Server Native Client 11.0};Server=localhost; Database=Mineria;Uid=; Pwd=; trusted_connection=yes")

setwd("C:/Users/CGil/Documents/MIN")

data <- read.csv2("24448.csv", encoding = "UTF-8")

colnames(data)[1] <- "Provincias"

data

data$Provincias <- gsub("[1-90]", "", data$Provincias)
data$Provincias <- gsub(" ", "", data$Provincias)

data$Edad <- gsub("[^1-90]", "", data$Edad)
data$Edad = as.integer(data$Edad)
data$Edad[is.na(data$Edad)] = 1000

data$Year <- substr(data$Periodo, 1,4) 
data$Periodo <- substr(data$Periodo, 5,6) 

data


#cargamos csv con la relación entre Comunidades y Provincias
prov_ca <- read.csv2("list-pro.csv", sep=";" , header = TRUE, encoding="UTF-8")
prov_ca <- prov_ca[-1]
prov_ca <- prov_ca[-3]
prov_ca <- prov_ca[-2]


colnames(prov_ca)[1] <- "Provincias"
colnames(prov_ca)[2] <- "Comunidades"

prov_ca





