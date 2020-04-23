library(RODBC)
library(odbc)
library(dplyr)
library(pracma)
library(rjson)

#establecemos conexi?n
con <- odbcDriverConnect("driver={SQL Server Native Client 11.0};Server=DESKTOP-BM96OLK ; Database=Mineria;Uid=; Pwd=; trusted_connection=yes")

setwd("C:/Users/CGIL/Documents/MIN")


company <- read.csv2("302.csv", encoding = "UTF-8")



#eliminamos el total y nos quedamos con aquellas filas con un tipo de empresa específico
colnames(company)[2]<-"Condicion"
colnames(company)[1]<-"Provincia"
colnames(company)[3]<-"Year"

company$Provincia <- gsub("[1-90]", "", company$Provincia)
company$Provincia <- gsub(" ", "", company$Provincia)

#eliminamos los totales
company <- company[company$Condicion != "Total",]
company <- company[company$Provincia != "TotalNacional",]

#cargamos csv con la relaci?n entre Comunidades y Provincias
prov_ca <- read.csv2("list-pro.csv", sep=";" , header = TRUE, encoding="UTF-8")
prov_ca <- prov_ca[-1]
prov_ca <- prov_ca[-3]
prov_ca <- prov_ca[-2]
prov_ca <- prov_ca[-2]
prov_ca <- prov_ca[-2]


colnames(prov_ca)[1] <- "Provincias"
colnames(prov_ca)[2] <- "Comunidades"


com_loaded <- sqlQuery(con, "select * from dbo.companies")

if(nrow(company) > nrow(com_loaded)){
  for (i in 1:nrow(company)){
    c_index <- match(company$Provincia[i], prov_ca$Provincias)
    insert_query <- paste("INSERT INTO dbo.companies (_period, _year, _state, province, quantity, class)
             VALUES ('Full','", company$Year[i], "','",prov_ca$Comunidades[c_index],"','",company$Provincia[i],"','",company$Total[i],"','",company$Condicion[i],"')", sep="")
    
    sqlQuery(con, insert_query)
  }
}
