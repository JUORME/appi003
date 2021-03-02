#Funcion para metodo para obtener el total de registros de una URL odata

get_records_url <- function (vec,token) {

	setfinal<-NULL

	for (i in 1:length(vec)) {


		url_f<- vec[i]
		getdata<-GET(url=url_f, add_headers(Authorization=token))
		df <-fromJSON(content(getdata,type="text",encoding="UTF-8")) %>% as.data.frame
		df <- as.data.frame(df)

		colnames(df) <- gsub("value.", "", colnames(df))
		columns <- colnames(df)
		columns_to_kill <- c("X.odata.nextLink","X.odata.context",".odata.etag")
		columns_valid <- setdiff(columns,columns_to_kill)

		df <-dplyr::select(df, all_of(columns_valid))
		
		setfinal<-rbind(setfinal,df)

	}
		

	return (setfinal)
}

