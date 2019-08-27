# Title     : TODO
# Objective : TODO
# Created by: sbasak
# Created on: 27/08/2019

library(bomrang)
library(bigrquery)
library(DBI)

project <- "wx-bq-poc"
dataset.name <- "weather"

stations_site_list <- end <- name <- NULL
load(system.file("extdata", 'stations_site_list.rda', package = "bomrang"))


con <- DBI::dbConnect(
    bigquery(),
    project = project,
    dataset = dataset.name,
    billing = project
)


# get list of downloaded tables
# downloaded.tables <- DBI::dbListTables(con)


for (row in 1:nrow(stations_site_list)) {
    site <- stations_site_list[row, "site"]
    lat <- stations_site_list[row, "lat"]
    lon <- stations_site_list[row, "lon"]
    message(paste0(site, ', lat-lon:', lat, ', ', lon))
    tryCatch(
      {pred <- get_current_weather(latlon=c(lat, lon),
        emit_latlon_msg = TRUE)
      return(pred)},
    error = function(error_message) {
      message("The following error occured:")
      message(error_message)
      return(data.frame())
      }
    )
}