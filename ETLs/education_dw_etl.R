library(RODBC)
library(odbc)
library(dplyr)
library(pracma)
library(rjson)

#establecemos conexi?n
con <- odbcDriverConnect("driver={SQL Server Native Client 11.0};Server=DESKTOP-BM96OLK ; Database=Mineria;Uid=; Pwd=; trusted_connection=yes")

setwd("C:/Users/CGIL/Documents/MIN")


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
edudf["year"] <- rep(2019:2014, nrow(edudf)/6)
edudf <- edudf[rep(seq_len(nrow(edudf)), each=4),]
edudf["period"] <- rep(c('T4','T3','T2','T1'), nrow(edudf)/4)


edudf["prct"] <- 1:nrow(edudf)

for(i in 0:132){
  for(j in 1:24){
    edudf[i*24 + j,29] <- edudf[i*24+j,2+j]
  }
}

for(i in 1:24){
  edudf <- edudf[,-3]
}


edu_loaded <- sqlQuery(conn, "select * from dbo.edu_achieved")

if(nrow(edudf) > nrow(edu_loaded)){
  for (i in 1:nrow(edudf)){
    
    insert_query <- paste("INSERT INTO dbo.edu_achieved (_period, _year, _state, province, edu_ach, perc)
             VALUES ('", edudf$period[i], "','",edudf$year[i],"','",edudf$states[i],"','Full','",edudf$achieved[i],"','",edudf$prct[i],"')", sep="")
    
    sqlQuery(conn, insert_query)
  }
}
