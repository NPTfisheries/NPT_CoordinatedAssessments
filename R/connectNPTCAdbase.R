#--------------------------------------------
# Function to connect to NPT Coordinated Assessments database
#
# Author: Mike Ackerman

connectNPTCAdbase = function(path_to_db) {
  # check if filepath exists before connecting
  if (!file.exists(path_to_db)) {
    stop("Database file does not exist at ", path_to_db,".")
  }
  
  # build connection strings
  driver_string = "Driver={Microsoft Access Driver (*.mdb, *.accdb)};"
  path_string = paste0("DBQ=", path_to_db)
  connection_string = paste0(driver_string, path_string)
  
  con = DBI::dbConnect(odbc::odbc(),
                       .connection_string = connection_string)
  return(con)
  
} # end function
