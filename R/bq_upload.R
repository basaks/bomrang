library(bomrang)
library(bigrquery)
library(DBI)
library(collections)


lib.paths <- .libPaths()

for (p in lib.paths){
  # check if file exists
  stations.rda <- file.path(p, 'bomrang', 'extdata', 'stations_site_list.rda')
  if (file_test("-f", stations.rda)){
    # this brings stations_site_list dataframe
    load(stations.rda)
  }
}

project <- "wx-bq-poc"
dataset.name <- "weather"

con <- DBI::dbConnect(
  bigquery(),
  project = project,
  dataset = dataset.name,
  billing = project
)

# stations.table <- "stations_site_list"

# remove table if exists
# if (DBI::dbExistsTable(conn = con, name = stations.table)) {
#   message(paste0("Removing existing table ", stations.table))
#   DBI::dbRemoveTable(conn = con, name = stations.table)
# }

# DBI::dbWriteTable(con, stations.table, stations_site_list)

# some connection issue without this line
httr::set_config(httr::config(http_version = 0))

# fake a dictionary
measures <- vector(mode="list", length=4)
names(measures) <- c("rain", "min", "max", "solar")
measures[[1]] <- "rainfall"; measures[[2]] <- "min_temperature"
measures[[3]] <- "max_temperature"; measures[[4]] <- "solar_exposure"


CatchGetHistorical <- function(s, t){
  tryCatch(
    {
      df_t <- data.frame(get_historical(s, type = t))
      print(paste0(measures[t], " with length ", nrow(df_t)))
      print(paste0("Got this measure: ", measures[t]))
      # drop beyond 2015
      # drop product_code and station_number, first two columns
      return(df_t[df_t$year > 2015, ][, c('year', 'month', 'day',
                                          paste0(measures[t]))])
    },
    error = function(error_message) {
      message("The following error occured:")
      message(error_message)
      return(data.frame())
    }
  )
}

GetHistoricalForStation <- function(s){
  
  df <- data.frame()
  
  for (t in c("rain", "min", "max", "solar")) {
    message(paste0("Downloading ", t, " data for station ", s))
    df_t <- CatchGetHistorical(s, t)
    if (nrow(df_t) == 0) {
      next  #  we got no data, so df can't be updated
      } else {
        if (nrow(df) == 0) {  # we got data, and df is empty
          df <- df_t
        } else { # we got data, and df is non-empty, merge outer
          df <- merge(df, df_t, by = c ('year', 'month', 'day'), all = TRUE)
        }
      }
  }
  return(df)
}

CatchDbWriteTable <- function(con, s, df){
  # try 10 times before giving up
  tryCatch(
    {
      for (i in 1:10) {
      ret.code <- DBI::dbWriteTable(con, s, df)
      if (ret.code){
        return(TRUE)
        }
      }
    },
    error = function(error_message){
      message("The following error occured while writing table:")
      message(error_message)
      message("Continue without this station")
      return(FALSE)
    }
  )
}

dropped_columns <- c("year","month", "day")

ReplaceByISOdate <- function(df, last_updated){

  # keep information from last_updated date, only the relevant bit that does
  # not exist in BQ
  # also convert year, month and day into date
  # drop year month and day
  df$date <- as.Date(ISOdate(df$year, df$month, df$day))
  if (length(last_updated) == 0) {
    message(paste0("No last updated date available for this station"))
    return (df[, !(names(df) %in% dropped_columns)])
  } else {
    return(df[, !(names(df) %in% dropped_columns)][df$date > last_updated])
  }
}

# get list of downloaded tables
downloaded.tables <- DBI::dbListTables(con)

# table to contain list of active stations
stale.stations <- 'stale_stations'
last.update.table <- 'last_update_date'

if (!DBI::dbExistsTable(conn = con, name = stale.stations)) {
  message(paste0("does not exist: ", stale.stations))
  stale_sts <- c()
} else {
  stale_sts <- DBI::dbReadTable(con, stale.stations)$station
}

if (!DBI::dbExistsTable(conn = con, name = last.update.table)) {
  message(paste0("does not exist: ", last.update.table))
  last.update.date <- data.frame(station=character(),
                                 last_updated=as.Date(character()))
} else {
  last.update.date <- DBI::dbReadTable(con, last.update.table)
}

# TODO: write another table with weather type information for each table/station

i <- 0

# TODO: parallelise this loop
for (s in stations_site_list$site) {
  
  # remove table if exists
  # skip if table exists
  # TODO: instead keep track of last updated date and save that information
  #  for future update
  if ((s %in% downloaded.tables) | (s %in% stale_sts)) {
    message(paste0("====Table ", s, " exists or in stale stations list======"))
    next
    # message(paste0("Removing existing table ", s))
    # DBI::dbRemoveTable(conn = con, name = s)
  } else {
    message(paste0("=========Downloding Data for table ", s, "=============="))
  }

  df <- GetHistoricalForStation(s)
    
  # don't write empty df as table, instead make a note in table stale_stations,
  # and do not attempt to download them again
  if (nrow(df) > 0) {

    # create table to write
    df <- ReplaceByISOdate(
      df,
      last.update.date$last_updated[last.update.date$station == s]
    )

    # update last_update_date table
    s_df = data.frame(station=c(s),
                      last_updated = c(max(df$date)),
                      stringsAsFactors=FALSE)

    upload_job <- insert_upload_job(project = project,
                                    dataset = dataset.name,
                                    table = last.update.table,
                                    values = s_df,
                                    create_disposition = "CREATE_IF_NEEDED",
                                    write_disposition = "WRITE_APPEND")

    wait_for(upload_job)

    message(paste0("=====Updated last update date for stations ", s))

    message(paste0("===========Writing Table: ", s, "======================="))
    return.code <- CatchDbWriteTable(con, s, df)
    if (return.code){
      i <- i + 1
      message(paste0("===========Wrote Table: ", s, " Total: ",i,"=========="))
    }
  } else {
    s_df = data.frame(station=c(s), stringsAsFactors=FALSE)
    upload_job <- insert_upload_job(project = project,
                                    dataset = dataset.name,
                                    table = stale.stations,
                                    values = s_df,
                                    create_disposition = "CREATE_IF_NEEDED",
                                    write_disposition = "WRITE_APPEND")
    wait_for(upload_job)
    message(paste0("Added ", s, " into state stations table"))
  }
}
