#' Utilities for reading study design files.
#'
#' @description
#' Fetches study design results from either local folder of DMS.
#' Checks that study design files are internally consistent.
#' DMS functionality not useful outside of PNNL unless connected through VPN.
#'
#' @description
#' * `read_study_design()`: returns a list of study design tables, accessible by $
#' * `read_study_design_from_DMS()`: finds data package folder in DMS and calls read_study_design there
#'
#'
#' @param path_to_folder (string) path to folder containing study design files
#' @param dataPkgNumber (integer) data package number for DMS
#'
#' @importFrom readr read_tsv
#' @importFrom dplyr filter select rename %>%
#' @importFrom odbc odbc dbConnect dbSendQuery dbFetch dbClearResult dbDisconnect
#'
#' @name read_study_design
#'
#' @examples
#' study_design <- read_study_design("data/study_design_folder")
#' 
#' fractions  <- study_design$fractions
#' samples    <- study_design$samples
#' references <- study_design$references
#' 
#' 
#' study_design <- read_study_design_from_DMS(3606)
#' 
#' fractions  <- study_design$fractions
#' samples    <- study_design$samples
#' references <- study_design$references


#' @export
#' @rdname read_study_design
# gets 3 study design files from local directory
read_study_design <- function(path_to_study_design) {
  
  
  ## fetch samples.txt
  pathToFile <- list.files(path=path_to_study_design,
                           pattern="^samples.txt$",
                           full.names=T)
  if(length(pathToFile) == 0) {
    stop("'samples.txt' not found.")
  }
  
  samples <- readr::read_tsv(pathToFile,
                             col_types=readr::cols(.default = "c"),
                             progress=FALSE)
  required_samples_columns <- c("PlexID",
                                "ReporterName",
                                "ReporterAlias",
                                "MeasurementName")
  if (!all(required_samples_columns %in% colnames(samples))) {
    message("\nRequired column(s) not found in the 'samples.txt' file: ",
            paste(required_samples_columns[!(required_samples_columns %in% colnames(samples))], collapse = ", "))
    stop("Incorrect column names or missing columns in the 'samples' study design table.")
  }
  
  ## fetch fractions.txt
  pathToFile <- list.files(path=path_to_study_design,
                           pattern="^fractions.txt$",
                           full.names=T)
  if (length(pathToFile) == 0){
    stop("'fractions.txt' not found.")
  }
  
  fractions <- readr::read_tsv(pathToFile,
                               col_types=readr::cols(.default = "c"),
                               progress=FALSE)
  required_fractions_columns <- c("PlexID",
                                  "Dataset")
  if (!all(required_fractions_columns %in% colnames(fractions))) {
    message("\nRequired column(s) not found in the 'fractions.txt' file: ", 
            paste(required_fractions_columns[!(required_fractions_columns %in% colnames(fractions))], collapse = ", "))
    stop("Incorrect column names or missing columns in the 'fractions' table.")
  }
  
  ## fetch references.txt
  pathToFile <- list.files(path=path_to_study_design,
                           pattern="^references.txt$",
                           full.names=T)
  if(length(pathToFile) > 0) {
    references <- readr::read_tsv(pathToFile,
                                  col_types=readr::cols(.default = "c"),
                                  progress=FALSE)
    required_references_columns <- c("PlexID",
                                     "Reference")
    if (!all(required_references_columns %in% colnames(references))) {
      message("\nRequired column(s) not found in the 'references.txt' file: ",
              paste(required_references_columns[!(required_references_columns %in% colnames(references))], collapse = ", "))
      stop("Incorrect column names or missing columns in the 'references' table.")
    }
  } else {
    warning("'references.txt' not found. It will be made automatically from `samples.txt`")
    references <- filter(samples, is.na(MeasurementName)) %>%
      select(PlexID, QuantBlock, ReporterAlias) %>%
      rename(Reference = ReporterAlias)
  }
  
  # Check for duplicates
  if (any(duplicated(fractions$Dataset))) {
    stop("Duplicate datasets in 'fractions.txt'")
  }
  if (any(duplicated(samples$MeasurementName[!is.na(samples$MeasurementName)]))) {
    stop("Duplicate sample names in 'samples.txt'")
  }
  if (!setequal(fractions$PlexID, samples$PlexID)) {
    stop("Plex IDs in 'fractions.txt' and 'samples.txt' do not match.")
  }
  
  study_design <- list(samples = samples,
                    fractions = fractions,
                    references = references)
  
  return(study_design)
}

read_study_design_from_DMS <- function(dataPkgNumber) {
  
  con_str <- sprintf("DRIVER={%s};SERVER=gigasax;DATABASE=DMS_Data_Package;%s",
                     get_driver(),
                     get_auth())
  con <- dbConnect(odbc(), .connection_string=con_str)
  
  ## fetch table with path to DataPackage
  strSQL <- sprintf("
                    SELECT *
                    FROM V_Data_Package_Detail_Report
                    WHERE ID = %s",
                    dataPkgNumber)
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
  
  study_design <- read_study_design(local_folder)
  
  if(.Platform$OS.type == "unix"){
    umount_cmd <- sprintf("umount %s", local_folder)
    system(umount_cmd)
    unlink(local_folder, recursive = T)
  }
  
  return(study_des)
}