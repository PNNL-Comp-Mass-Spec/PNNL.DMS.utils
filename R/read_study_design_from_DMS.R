#' Utilities for reading study design files.
#'
#' @description
#' Fetches study design results from DMS.
#' Checks that study design files are internally consistent.
#' DMS functionality not useful outside of PNNL unless connected through VPN.
#'
#' @description
#' * `read_study_design_from_DMS()`: finds data package folder in DMS and calls read_study_design there
#'
#' @param data_package_num (integer) data package number for DMS
#'
#' @importFrom odbc odbc dbConnect dbSendQuery dbFetch dbClearResult dbDisconnect
#' @importFrom readr read_tsv cols
#' @name read_study_design
#'
#' @examples
#' study_design <- read_study_design_from_DMS(3606)
#' 
#' fractions  <- study_design$fractions
#' samples    <- study_design$samples
#' references <- study_design$references


#' @export
path_to_study_design_from_DMS <- function(data_package_num) {
  
  con_str <- sprintf("DRIVER={%s};SERVER=gigasax;DATABASE=DMS_Data_Package;%s",
                     get_driver(),
                     get_auth())
  con <- dbConnect(odbc(), .connection_string=con_str)
  
  ## fetch table with path to DataPackage
  strSQL <- sprintf("
                    SELECT *
                    FROM V_Data_Package_Detail_Report
                    WHERE ID = %s",
                    data_package_num)
  qry <- dbSendQuery(con, strSQL)
  dataPkgReport <- dbFetch(qry)
  dbClearResult(qry)
  
  if(.Platform$OS.type == "unix"){
    local_folder <- "~/temp_study_des"
    if(file.exists(local_folder)){
      unlink(local_folder, recursive = T)
    }
    dir.create(local_folder)
    
    remote_folder <- gsub("\\\\","/", dataPkgReport$`Share Path`)
    remote_folder <- gsub("(", "\\(", remote_folder, fixed = T)
    remote_folder <- gsub(")", "\\)", remote_folder, fixed = T)
    mount_cmd <- sprintf("mount -t smbfs %s %s", remote_folder, local_folder)
    system(mount_cmd)
  }else if(.Platform$OS.type == "windows"){
    local_folder <- dataPkgReport$`Share Path`
  }else{
    stop("Unknown OS type.")
  }
  return(local_folder)
}

#' @export
read_study_design_from_DMS <- function(data_package_num) {
  
  local_folder <- path_to_study_design_from_DMS(data_package_num)
  filenames <- c("fractions.txt", "samples.txt", "references.txt")

  results <- lapply(filenames, function(filename) {
    pathToFile <- list.files(path = local_folder,
                             pattern = filename,
                             full.names = T)
    if (length(pathToFile) == 0) {
      stop(filename, " not found.")
    }
    df <- read_tsv_helper(pathToFile,
                   col_types = cols(.default = "c"),
                   progress = FALSE)
    df <- as.data.frame(df)
  })
  names(results) <- sub(".txt", "", filenames)
  return(results)
}

#' @export
write_study_design_tables <- function(data_package_num, study_design, overwrite = FALSE) {
  
  stopifnot(!is.null(names(study_design)))
  
  local_folder <- path_to_study_design_from_DMS(data_package_num)
  
  for (i in 1:3) {
    table <- study_design[[i]]
    tablename <- paste0(names(study_design)[i], ".txt")
    pathToFile <- list.files(path = local_folder, pattern = paste0("^", tablename, "$"))
    if ((length(pathToFile) == 0) | overwrite) {
      write.table(table, file = file.path(local_folder, tablename),
                  quote = FALSE, sep = "\t", row.names = FALSE)
    } else {
      stop(tablename, " already exists! Try setting overwrite = T.")
    }
  }
}
