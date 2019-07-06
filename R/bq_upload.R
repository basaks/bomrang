library('bomrang')
library('bigrquery')
library('collections')

# this brings stations_site_list dataframe
load('/home/sudiptra/R/x86_64-pc-linux-gnu-library/3.6/bomrang/extdata/stations_site_list.rda')

project <- "wx-bq-poc"
dataset_name <- "weather"

con <- DBI::dbConnect(
  bigquery(),
  project = project,
  dataset = dataset_name,
  billing = project
)

stations_table <- "stations_site_list"

# remove table if exists
if (DBI::dbExistsTable(conn = con, name = stations_table)) {
  message(paste0("Removing existing table ", stations_table))
  DBI::dbRemoveTable(conn = con, name = stations_table)
}

# dbListTables(con)
DBI::dbWriteTable(con, stations_table, stations_site_list)

# some connection issue without this line
httr::set_config(httr::config(http_version = 0))

# fake a dictionary
measures <- vector(mode="list", length=4)
names(measures) <- c("rain", "min", "max", "solar")
measures[[1]] <- "rainfall"; measures[[2]] <- "min_temperature"; measures[[3]] <- "max_temperature"; measures[[4]] <- "solar_exposure" 


catch_get_historical <- function(s, t){
  tryCatch(
    {
      df_t <- data.frame(get_historical(s, type = t))
      print(paste0(measures[t], " with length ", nrow(df_t)))
      print(paste0("Got this measure: ", measures[t]))
      # drop beyond 2015
      # drop product_code and station_number, first two columns
      return(df_t[df_t$year > 2015, ][, c('year', 'month', 'day', paste0(measures[t]))])
    },
    error=function(error_message) {
      message("The following error occured:")
      message(error_message)
      return(data.frame())
    }
  )
}

get_historical_for_station <- function(s){
  
  df <- data.frame()
  
  for (t in c("rain", "min", "max", "solar")) {
    message(paste0("Downloading ", t, " data for station ", s))
    df_t <- catch_get_historical(s, t)
    if ((nrow(df_t) > 0) & (nrow(df) > 0 )) {
      df <- merge(df, df_t, by = c ('year', 'month', 'day'), all = TRUE)
      } else if ((nrow(df) == 0) & (nrow(df_t) > 0))  {
        df <- df_t
      }
  }
  
  return(df)
}


for (s in stations_site_list$site){
  
  # remove table if exists
  # skip if table exists
  # TODO: instead keep track of last updated date and save that information for future update
  if (DBI::dbExistsTable(conn = con, name = s)) {
    message(paste0("===========Table ", s, "exists =========================="))
    next
    # message(paste0("Removing existing table ", s))
    # DBI::dbRemoveTable(conn = con, name = s)
  } else {
    message(paste0("=========Downloding Data for table ", s, "==============="))
  }

  df <- get_historical_for_station(s)
    
  # don't write empty df
  if (nrow(df) > 0) {
    DBI::dbWriteTable(con,  s, df)
  }
  
}
