library(RODBC)
library(odbc)
library(dplyr)
library(pracma)

#establecemos conexión
con <- odbcDriverConnect("driver={SQL Server Native Client 11.0};Server=localhost; Database=Mineria;Uid=; Pwd=; trusted_connection=yes")

setwd("C:/Users/amali/Downloads")

data <- read.csv2("24448.csv", encoding = "UTF-8")

colnames(data)[1] <- "Provincias"

data$Provincias <- gsub("[1-90]", "", data$Provincias)
data$Provincias <- gsub(" ", "", data$Provincias)

data$Edad <- gsub("[^1-90]", "", data$Edad)
data$Edad = as.integer(data$Edad)
data$Edad[is.na(data$Edad)] = 1000

data$Year <- substr(data$Periodo, 1,4) 
data$Periodo <- substr(data$Periodo, 5,6) 

#cargamos csv con la relación entre Comunidades y Provincias
prov_ca <- read.csv2("list-pro.csv", sep=";" , header = TRUE, encoding="UTF-8")
prov_ca <- prov_ca[-1]
prov_ca <- prov_ca[-3]
prov_ca <- prov_ca[-2]
prov_ca <- prov_ca[-2]
prov_ca <- prov_ca[-2]


colnames(prov_ca)[1] <- "Provincias"
colnames(prov_ca)[2] <- "Comunidades"

prov <- unique(data$Provincias)
prov

dim_prov <- sqlQuery(con, "SELECT n_province FROM dbo.dim_provinces WHERE n_province <> 'Whole'")



for( row in prov){
  
  if(!row %in% dim_prov$n_province){
    
    c_index <- match(row, prov_ca$Provincias)
    
    
    insert_query <- paste("INSERT INTO dbo.dim_provinces (n_province, s_name)
             VALUES ('",row,"','", prov_ca$Comunidades[c_index], "')", sep="")
    
    sqlQuery(con, insert_query)
  }
  
  
}

# GENERO
sexo <- unique(data$Sexo)
dim_sex <- sqlQuery(con, "SELECT n_gender FROM dbo.dim_gender")

for(i in sexo){ 
  
  if((i %in% dim_sex$n_gender == FALSE) || nrow(dim_sex) < 3){
    insert_query <- paste("INSERT INTO dbo.dim_gender (n_gender) VALUES ('", i, "')", sep="")
    sqlQuery(con, insert_query)
  }
  
}

#Anyos
dim_years <- sqlQuery(con, "SELECT n_year FROM dbo.dim_year WHERE y_period = 'F'")

years_levels <- 2008:2019
years_levels

level %in% dim_years$n_year

for(level in years_levels){
  
  if((level %in% dim_years$n_year) == FALSE || length(dim_years)==0){
    
    #Completo(Full)
    insert_query <- paste("INSERT INTO dbo.dim_year (n_year, y_period)
             VALUES ('", level, "','F')", sep="")
    
    sqlQuery(con, insert_query)
    
    #Semestre1
    insert_query <- paste("INSERT INTO dbo.dim_year (n_year, y_period)
             VALUES ('", level, "','S1')", sep="")
    
    sqlQuery(con, insert_query)
    
    #Semestre2
    insert_query <- paste("INSERT INTO dbo.dim_year (n_year, y_period)
             VALUES ('", level, "','S2')", sep="")
    
    sqlQuery(con, insert_query)
    
    #Trimestre1
    insert_query <- paste("INSERT INTO dbo.dim_year (n_year, y_period)
             VALUES ('", level, "','T1')", sep="")
    
    sqlQuery(con, insert_query)
    
    #Trimestre2
    insert_query <- paste("INSERT INTO dbo.dim_year (n_year, y_period)
             VALUES ('", level, "','T2')", sep="")
    
    sqlQuery(con, insert_query)
    
    #Trimestre3
    insert_query <- paste("INSERT INTO dbo.dim_year (n_year, y_period)
             VALUES ('", level, "','T3')", sep="")
    
    sqlQuery(con, insert_query)
  }
  
}

dim_yearsS1 <- sqlQuery(con, "SELECT id_year, n_year, y_period FROM dbo.dim_year WHERE y_period = 'S1'" )
dim_yearsS2 <- sqlQuery(con, "SELECT id_year, n_year, y_period FROM dbo.dim_year WHERE y_period = 'S2'" )
dim_gender <- sqlQuery(con, "SELECT id_gender, n_gender FROM dbo.dim_gender")
dim_prov <- sqlQuery(con, "SELECT id_province, s_name, n_province FROM dbo.dim_provinces")

for (row in prov){
  c_index <- match(row, dim_prov$n_province)
  
  data$Provincias <- gsub(row,dim_prov$id_province[c_index], data$Provincias)
  
}

colnames(data)[1] <- "id_province"

for (row in dim_gender$n_gender){
  c_index <- match(row, dim_gender$n_gender)
  
  data$Sexo <- gsub(row,dim_gender$id_gender[c_index], data$Sexo)
  
}

colnames(data)[2] <- "id_gender"
colnames(data)[3] <- "nm_age"

for (row in data$Year){
  d_index <- match(row, data$Year)
  if( strcmp(data$Periodo[d_index], "S1") ){
    c_indexD <-  match(row, dim_yearsS1$n_year)
    data$Year[d_index] <- dim_yearsS1$id_year[c_indexD]
  }
  else{
    c_indexD <-  match(row, dim_yearsS2$n_year)
    data$Year[d_index] <- dim_yearsS2$id_year[c_indexD]
  }
}


