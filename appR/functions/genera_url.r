#FUNCION PARA GENERAR URL
genera_url <- function (c,l,u) {

	url<- vector()
	frac<-c / l
	p<-ceiling(frac)

	co<-c
	lacum<-0
	for (i in 1:p) {

		lr <- co-l
		co<-lr

		if (lr > 1) {
			lf = l
		} else {
			lf = l-abs(lr)
		}

		lacum<-as.integer(lacum)

		url_n<-gsub("rvar_s",lacum,u)
		url_s<-gsub("rvar_t",lf,url_n)

		#url[i]<-paste("https://mistr.operations.dynamics.com/data/SalesInvoiceLines?$skip=",lacum,"&$top=",lf,"&$filter=InvoiceDate%20eq%202020-08-10&$select=InvoiceNumber,LineCreationSequenceNumber,InventorySiteId,ProductNumber,CurrencyCode,InventoryWarehouseId,SalesUnitSymbol,InvoicedQuantity,SalesPrice,LineAmount,LineTotalTaxAmount,LineTotalDiscountAmount",sep="")
		url[i]<-url_s
		lacum<-lacum+lf

		#text<-paste("Particion ",i," de ",lf," acumulando ",lacum," ",sep="")
		#print(url)

	}

	return(url)

}
