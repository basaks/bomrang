library(bomrang)
library(bigrquery)
library(DBI)
library(collections)

stations_site_list <- end <- name <- NULL
load(system.file("extdata", 'stations_site_list.rda', package = "bomrang"))

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

dropped.columns <- c("year","month", "day")
earliest.date <- as.Date(ISOdate(1990, 1, 1))

ReplaceByISOdate <- function(df){

  # keep information from last_updated date, only the relevant bit that does
  # not exist in BQ
  # also convert year, month and day into date
  # drop year month and day
  df$date <- as.Date(ISOdate(df$year, df$month, df$day))
  # if (length(last.updated) == 0) {
  #   message(paste0("No last updated date available for this station"))
  #   return (df[, !(names(df) %in% dropped.columns)])
  # } else {
  #   return(df[, !(names(df) %in% dropped.columns)][df$date > last.updated])
  # }
  return(df[, !(names(df) %in% dropped.columns)])
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


CatchMaxDate  <- function(df) {
    tryCatch(
    {
        max.date <- max(df$date)
        return(max.date)
    },
    error = function(error_message){
        message("The following error occured while writing table:")
        message(error_message)
        message("Continue without this station")
        return(earliest.date)
    }
    )
}


UpdateLastUpdatedTable <- function(s, df) {
    # update last_update_date table

    max.date <- CatchMaxDate(df)

    s_df = data.frame(
        station=c(s),
        last_updated = max.date,
        stringsAsFactors=FALSE
    )

    upload_job <- insert_upload_job(
        project = project,
        dataset = dataset.name,
        table = last.update.table,
        values = s_df,
        create_disposition = "CREATE_IF_NEEDED",
        write_disposition = "WRITE_APPEND"
    )

    wait_for(upload_job)

    message(paste0("=====Updated last update date for stations ", s))
}


UpdateStaleStations <- function(s) {
    s_df = data.frame(
        station=c(s),
        stringsAsFactors=FALSE
    )

    upload_job <- insert_upload_job(
        project = project,
        dataset = dataset.name,
        table = stale.stations,
        values = s_df,
        create_disposition = "CREATE_IF_NEEDED",
        write_disposition = "WRITE_APPEND"
    )
    wait_for(upload_job)

    message(paste0("Added ", s, " into state stations table"))
}

# TODO: write another table with weather type information for each table/station
write_count <- 0
sites <- sample(stations_site_list$site, size=length(stations_site_list$site))

library(doParallel)
# cl <- makePSOCKcluster(detectCores())
cl <- makePSOCKcluster(20)
registerDoParallel(cl)

clusterEvalQ(cl, {
    library('bomrang')
    library('bigrquery')
    project <- "wx-bq-poc"
    dataset.name <- "weather"
    con <- DBI::dbConnect(
        bigquery(),
        project = project,
        dataset = dataset.name,
        billing = project
        )
    NULL
    }
)

# followed this: https://stackoverflow.com/a/24634121/3321542
foreach(s=sites, .inorder=FALSE,
        .noexport="con",
        .packages=c("DBI", "bigrquery", "bomrang")) %dopar% {
  # skip if table exists
  if ((s %in% downloaded.tables) | (s %in% stale_sts) |
    (DBI::dbExistsTable(con, s))) {
    message(paste0("====Table ", s, " exists or in stale stations list======"))
    next
    # message(paste0("Removing existing table ", s))
    # DBI::dbRemoveTable(conn = con, name = s)
  } else {
    message(paste0("=========Downloding Data for table ", s, "=============="))
  }

  # if last_update_date for table is same day skip, else keep going

  # download data from BOM
  df <- GetHistoricalForStation(s)

  # don't write empty df as table, instead make a note in table stale_stations,
  # and do not attempt to download them again
  if (nrow(df) > 0) {

    # create table to write
    # df <- ReplaceByISOdate(df)

    # UpdateLastUpdatedTable(s, df)

    message(paste0("===========Writing Table: ", s, "======================="))
    return.code <- CatchDbWriteTable(con, s, df)

    if (return.code){
      write_count <- write_count + 1
      message(paste0("===========Wrote Table: ", s, " Total: ", write_count,
      "=========="))
    } else {
      UpdateStaleStations(s)
    }
  } else {
    UpdateStaleStations(s)
  }
}

clusterEvalQ(cl, {
    dbDisconnect(con)
})