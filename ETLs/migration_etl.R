library(RODBC)
library(odbc)

#establecemos conexi�n
dbconnection <- odbcDriverConnect("driver={SQL Server Native Client 11.0};Server=localhost; Database=MIN;Uid=; Pwd=; trusted_connection=yes")

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


#cargamos csv con la relaci�n entre Comunidades y Provincias
prov_ca <- read.csv2("list-pro.csv", sep=";" , header = TRUE, encoding="UTF-8")
prov_ca <- prov_ca[-1]
prov_ca <- prov_ca[-3]
prov_ca <- prov_ca[-2]
prov_ca <- prov_ca[-2]
prov_ca <- prov_ca[-2]


colnames(prov_ca)[1] <- "Provincias"
colnames(prov_ca)[2] <- "Comunidades"

prov_ca

prov <- unique(data$Provincias)
prov

#conexion con la base de datos
conn <- odbcDriverConnect('driver={SQL Server};server=DESKTOP-BM96OLK;database=MIN;trusted_connection=true')

dim_prov <- sqlQuery(conn, "SELECT n_province FROM dbo.dim_provinces WHERE n_province <> 'Whole'")



for( row in prov){
  
  if(!row %in% dim_prov$n_province){
    
    c_index <- match(row, prov_ca$Provincias)
    
    
    insert_query <- paste("INSERT INTO dbo.dim_provinces (n_province, s_name)
             VALUES ('",row,"','", prov_ca$Comunidades[c_index], "')", sep="")
    
    sqlQuery(conn, insert_query)
  }
  
  
}


unique(prov_ca$Comunidades)

data["2019T1"] <- new_vector

