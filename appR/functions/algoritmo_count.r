#Funcion para metodo para obtener el total de registros de una URL odata

get_count_url <- function (u,token) {

	getcount<-GET(url=u, add_headers(Authorization=token))
	getcountdf <-fromJSON(content(getcount,type="text",encoding="UTF-8")) %>% as.data.frame
	totalc<-as.integer(getcountdf[1,1])

	return (totalc)
}