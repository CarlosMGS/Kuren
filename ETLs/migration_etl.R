library(RODBC)
library(odbc)
library(dplyr)
library(pracma)
library(rjson)

#establecemos conexi?n
con <- odbcDriverConnect("driver={SQL Server Native Client 11.0};Server=localhost ; Database=Mineria;Uid=; Pwd=; trusted_connection=yes")

setwd("C:/Users/dipdn/Desktop/MIN")

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

prov <- unique(data$Provincias)
prov

#eliminamos filas donde sexo se diferencie
data <- data[data$Sexo == "Ambos sexos",]
unique(data$Sexo)

data <- data[-2]


data <- data[-3]

data[, 3] <- as.numeric(as.character( data[, 3] ))




#para cada provincia guardamos la comunidad autónoma a la que pertenece
for(i in 1:nrow(data)){
  c_index <- match(data$Provincias[i], prov_ca$Provincias)
  
  data$Provincias[i] <- as.character(prov_ca$Comunidades[c_index])
}


#agrupamos por años y comunidades paraa adaptar los datos
data <- group_by(data, Provincias, Edad, Year) %>% summarise(sum <- sum(Total))

colnames(data)[4] <- "Total"

data$Total[is.na(data$Total)] <- 0
data <- data[data$Edad < 1000,]

unique(data$Edad)

unique(data$Year)

#tabla de migraciones lista, sufrirá modificaciones porque debemos guardar varios datos para cada fila




#cargamos fuentes de dimensiones
#empresas
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

#eliminamos los años anteriores a 2008

company[, 3] <- as.numeric(as.character( company[, 3] ))
company <- company[company$Year > 2007,]

#agrupamos por comunidad
for(i in 1:nrow(company)){
  c_index <- match(company$Provincia[i], prov_ca$Provincias)
  
  company$Provincia[i] <- as.character(prov_ca$Comunidades[c_index])
}

company[, 4] <- as.numeric(as.character( company[, 4] ))
company <- group_by(company, Provincia, Condicion, Year) %>% summarise(sum <- sum(Total))
colnames(company)[4]<- "Total"

#Añadimos a los hechos el id por cada tipo de empresa
unique(company$Condicion)
data <- data[rep(seq_len(nrow(data)), each=9),]
data["id_c"] <- rep(1:9, nrow(data)/9)

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
education <- fromJSON(file="education.json")

#str(education)
data.class(education)

#Dimensiones
gender <- list()
states <- list()
achieved <- list() #Nivel de formacion conseguido


gender <- NULL
states <- NULL
achieved <- NULL


for(i in 1:480){
  gender[i] <- education[[i]][[6]][[2]][[3]]
  states[i] <- education[[i]][[6]][[3]][[3]]
  achieved[i] <- education[[i]][[6]][[4]][[3]]
}


states <- unlist(states, use.names = FALSE)
gender <- unlist(gender, use.names = FALSE)
achieved <- unlist(achieved, use.names = FALSE)

edudf <- data.frame(
  "gender" = gender,
  "states" = states,
  "achieved" = achieved
)

periodos <- education[[1]][[7]]

for(i in 1:480){
  for(j in 1:24){
    year <- paste(periodos[[j]][[3]], periodos[[j]][[4]], sep = "")
    if(i == 1){
      if(!year %in% colnames(edudf)){
        edudf[year] <- 1:480
      }
    }
    edudf[i, year] <- education[[i]][[7]][[j]][[5]]
  }
}

for(i in 1:6){
  for(j in 1:3){
    edudf <- edudf[,-(4+i)]
  }  
}

unique(data$Year)

#Eliminamos filas sobrantes o poco relevantes
#Eliminamos de la columna "achieved" las filas que contengan "Total" ya que no es relevante
edudf <- edudf[edudf$achieved != "Total", ]
#Eliminamos las filas en las que se diferenciaba por sexo
edudf <- edudf[edudf$gender == "Ambos sexos", ]
#Eliminamos las filas que daban el Total Nacional ya que no son relevantes
edudf <- edudf[edudf$states != "Total Nacional", ]
#Eliminamos la primera columna "gender"
edudf<-edudf[, -1]

edudf <- edudf[rep(seq_len(nrow(edudf)), each=6),]
edudf["year"] <- rep(2014:2019, 133)
edudf["prct"] <- 1:798

for(i in 0:132){
  for(j in 1:6){
    edudf[i*6 + j,10] <- edudf[i*6+j,2+j]
  }
}

#eliminamos las columnas sobrantes
for(i in 1:6){
  edudf <- edudf[,-3]
}

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
 