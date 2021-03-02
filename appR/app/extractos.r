############################################   EXTRACTOS      ##############################################################################################
############################################################################################################################################################

#Fijar el la ruta de trabajo
	setwd("D:/github/appi003/appR/")

#Blibliotecas requeridas
	library(httr)    
	library(rjson)  
	library(jsonlite)
	library(dplyr)
	library(openxlsx)
	library(data.table)
	library(bit64)
	library(stringr)

#Conexion a D365FO TRUJILLO INVESTMENT
	body <- list(grant_type = "client_credentials", client_id = "7cb678f1-2bc4-4456-a590-f7216b23dd88",
	client_secret = "~9X1PR2etZ1sHw1tsv-15Y.pD3F_RaTCxG", resource = "https://mistr.operations.dynamics.com")

	response <-POST("https://login.microsoftonline.com/ceb88b8e-4e6a-4561-a112-5cf771712517/oauth2/token",add_headers("Cookie: x-ms-gateway-slice=prod; stsservicecookie=ests; fpc=AqQZzzXZjstDgAtb0IfeeFZVotOLAQAAANAmrtYOAAAA"), body=body,encode = "form")

	datatoken <-fromJSON(content(response,type="text")) %>% as.data.frame
	tok_type<-as.character(datatoken[1,1])
	tok<-as.character(datatoken[1,7])
	token <- paste(tok_type," ",tok,"",sep="")


# #Contar el registro de cabeceras de ordenes de venta 

# 	fi <- "2021-02-14"
# 	source("algoritmo_count.r")
# 	url1 <- paste("https://mistr.operations.dynamics.com/data/SalesOrderHeadersV2/$count?$filter=RequestedReceiptDate%20ge%20",fi,"",sep="")
# 	count<-get_count_url(url1,token)

# 	lote<-10000
# 	source("genera_url.r")
# 	urldata1<-paste("https://mistr.operations.dynamics.com/data/SalesOrderHeadersV2?$skip=rvar_s&$top=rvar_t&$filter=RequestedReceiptDate%20ge%20",fi,"%20&$select=SalesOrderNumber,RequestedReceiptDate,SalesOrderStatus",sep="")
# 	vec1<-genera_url(count,lote,urldata1)

# 	source("function_get_collect.r")
# 	saleshead <- get_records_url(vec1,token)
# 	#head(saleshead)

# 	saleshead$RequestedReceiptDate <- as.character(as.POSIXct(saleshead$RequestedReceiptDate, format="%Y-%m-%d",tz="UTC"))

#Uso de la funcion para extraer datos con el Entity proporcionado
	source("functions/function_get_collect.r")
	data1f_collect <- get_records_url("https://mistr.operations.dynamics.com//data/RetailEodStatementAggregations?$filter=ErrorMessage%20ne%20%27null%27",token)
	head(data1f_collect)

#Extrae todos los pedidos de ventas 
	salesid <- data1f_collect$SalesId

	pv = NULL
for (i in length(salesid)) {

	pv = salesid[i]
	source("functions/function_get_collect.r")
		data3f_collect <- get_records_url("https://mistr.operations.dynamics.com/data/SalesOrderHeadersV2?$filter=SalesOrderNumber%20eq%20%27MSA-000006%27&$select=SalesOrderNumber,RequestedReceiptDate",token)
		head(data3f_collect)

}















#Extrae todos los numeros de la calumna mensaje
	dat2 <- as.data.frame(cbind(data1f_collect[,2], data1f_collect[,5], data1f_collect[,7]))
	names(dat2) <- c("StatementId", "SalesOrderNumber", "StoreNumber")
	dat <- as.data.frame(data1f_collect$ErrorMessage)
	dat<- str_extract_all(dat, "\\d+.\\d+\\S")

##Busqueda de datos cod productos
	productos <- NULL
	k = seq(3,length(dat[[1]]), by=9)

	for(i in k){
		productos = rbind(productos, dat[[1]][i])
	}
	productos <- as.data.frame(productos)
	names(productos) <- "productos"

##Busqueda de datos cantidad faltante
	c_req <- NULL
	t = seq(5,length(dat[[1]]), by=9)
	for(i in t){
		c_req = rbind(c_req, dat[[1]][i])
	}
	c_req <- as.numeric(c_req)

##Busqueda de datos Stock
	stock <- NULL
	h = seq(6,length(dat[[1]]), by=9)
	for(i in h){
		stock = rbind(stock, dat[[1]][i])
	}
	stock <- as.numeric(stock)

#Leer bd de productos de allproducts
source("functions/function_get_collect.r")
	data2f_collect <- get_records_url("https://mistr.operations.dynamics.com/data/AllProducts?$select=ProductNumber,ProductName",token)
	head(data2f_collect)





#Leer bd productos ya grup
	prod <- read.csv("prod.csv", sep= ';')
	names(prod)<- c("productos","nameprod")

#Unir columnas
	f1 <- cbind(dat2,productos,c_req,stock)

#Unir con el dataframe prod (contiene el nombre del producto Left Join)
	c1 <- merge(f1,prod, "productos", all.x = TRUE)
	c2 <- merge(c1,saleshead, "SalesOrderNumber", all.x = TRUE)

#Agrupar por columnas tienda y productos, nommbre de producto y Fecha
f2<- c2 %>% 
		group_by(StoreNumber,productos,nameprod,RequestedReceiptDate) %>%
		summarise(c_req = sum(c_req),stock = sum(stock))

		k1 <- f2$StoreNumber
		k2 <- f2$productos
		k3 <- f2$nameprod
		k4 <- f2$RequestedReceiptDate
		k5 <- f2$c_req
		k6 <- f2$stock

#funciÃ³n para asignar nombres a las tiendas
 nametienda <- function(x){
    x <- gsub("000001","MD01_LUZ", x,ignore.case = FALSE)
    x <- gsub("000002","MD02_JRC", x,ignore.case = FALSE)
    x <- gsub("000003","MD03_CRH", x,ignore.case = FALSE)
    x <- gsub("000004","MD04_SUC", x,ignore.case = FALSE)
    x <- gsub("000005","MD05_CRZ", x,ignore.case = FALSE)
    }
  
	k1 <- nametienda(k1)

#Ordenar columnas del dataframe 
	f4 <- as.data.frame(cbind(k1,k2,k3,k4,k5,k6))
	names(f4) <- c("Tienda", "CodPro", "DescripciÃ³n","Cantidad_Req","Fecha", "Stock") 

#Expportar datos en formato csv
	write.csv(f4,"EExtractos.csv")
