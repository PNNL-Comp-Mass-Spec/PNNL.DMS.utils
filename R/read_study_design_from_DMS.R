#' @title Reading and Writing Study Design Tables
#'
#' @description Fetches study design results from DMS. Checks that study design
#'   files are internally consistent. DMS functionality not useful outside of
#'   PNNL unless connected through VPN.
#'
#' @description
#'
#' * `path_to_study_design_from_DMS()`: gets location of study design files from
#' the DMS.
#'
#' * `read_study_design_from_DMS()`: reads study design tables from the DMS data
#' package folder.
#'
#' * `write_study_design_to_DMS()`: writes study design tables to the DMS so
#' that they can be accessed by others.
#'
#' @param data_package_num (integer) data package number for DMS
#' @param useHTTP (logical) whether to...
#' @param study_design (list) study design files: fractions, samples, and
#'   references.
#' @param overwrite (logical) whether to replace any existing study design
#'   tables when writing files.
#'
#' @importFrom DBI dbConnect dbSendQuery dbFetch dbClearResult
#'   dbDisconnect
#' @importFrom readr read_tsv cols
#' @importFrom utils write.table
#'
#' @name read_study_design
#'
#' @md
#'
#' @examples
#' if (is_PNNL_DMS_connection_successful()) {
#'   study_design <- read_study_design_from_DMS(3606)
#'
#'   fractions  <- study_design$fractions
#'   samples    <- study_design$samples
#'   references <- study_design$references
#' }

 
#' @export
#' @rdname read_study_design
path_to_study_design_from_DMS <- function(data_package_num) 
{

  con <- get_db_connection()
  
  ## fetch table with path to DataPackage
  strSQL <- sprintf("
                    SELECT *
                    FROM dpkg.v_data_package_detail_report
                    WHERE id = %s",
                    data_package_num)
  qry <- dbSendQuery(con, strSQL)
  dataPkgReport <- dbFetch(qry)
  dbClearResult(qry)
  
  if(.Platform$OS.type == "unix"){
    local_folder <- .new_tempdir()
    
    remote_folder <- gsub("\\\\","/", dataPkgReport$share_path)
    remote_folder <- gsub("(", "\\(", remote_folder, fixed = T)
    remote_folder <- gsub(")", "\\)", remote_folder, fixed = T)
    mount_cmd <- sprintf("mount -t smbfs %s %s", remote_folder, local_folder)
    system(mount_cmd)
  }else if(.Platform$OS.type == "windows"){
    local_folder <- dataPkgReport$share_path
  }else{
    stop("Unknown OS type.")
  }
  return(local_folder)
}


#' @export
#' @rdname read_study_design
#' @importFrom glue glue
read_study_design_from_DMS <- function(data_package_num,
                                       useHTTP = FALSE) 
{

  con <- get_db_connection()
  
  ## fetch table with path to DataPackage
  strSQL <- sprintf("
                    SELECT *
                    FROM dpkg.v_data_package_detail_report
                    WHERE id = %s",
                    data_package_num)
  qry <- dbSendQuery(con, strSQL)
  dataPkgReport <- dbFetch(qry)
  dbClearResult(qry)
  
  # Need to check that all of these columns are present in their
  # respective study design tables.
  required_cols <- list(
    fractions = c("PlexID", "Dataset"),
    samples = c("PlexID", "QuantBlock", "ReporterAlias",
                "ReporterName", "MeasurementName"),
    references = c("PlexID", "QuantBlock", "Reference")
  )
  
  # Files to search for
  file_paths <- file.path(
    dataPkgReport$web_path,
    glue("{names(required_cols)}.txt")
  )
  
  names(file_paths) <- names(required_cols)
  
  # Check that files are present, read in data, check for required columns
  study_design <- lapply(names(file_paths), function(file_name) {
    pathToFile <- file_paths[[file_name]]
    
    if (length(pathToFile) == 0) {
      stop(sprintf("Could not find '%s' file in DMS Data Package folder.",
                   gsub("[^a-z\\.]", "", file_name)))
    }
    x <- readr::read_tsv(pathToFile, 
                         col_types = readr::cols(.default = "c"), 
                         progress = FALSE)
    
    if (!setequal(colnames(x), required_cols[[file_name]])) {
      stop(sprintf("There are incorrect column names or missing columns in the '%s'
         study design table.", 
                   gsub("[^a-z\\.]", "", file_name)))
    }
    return(x)
  })
  names(study_design) <- names(required_cols)
  
  return(study_design)
}

utils::globalVariables("file_i")

# read_study_design_from_DMS <- function(data_package_num) {
#   
#   local_folder <- path_to_study_design_from_DMS(data_package_num)
#   filenames <- c("fractions.txt", "samples.txt", "references.txt")
#   
#   results <- lapply(filenames, function(filename) {
#     pathToFile <- list.files(path = local_folder,
#                              pattern = filename,
#                              full.names = T)
#     if (length(pathToFile) == 0) {
#       stop(filename, " not found.")
#     }
#     df <- read_tsv(pathToFile,
#                           col_types = cols(.default = "c"),
#                           progress = FALSE)
#     df <- as.data.frame(df)
#   })
#   names(results) <- sub(".txt", "", filenames)
#   return(results)
# }


#' @export
#' @rdname read_study_design
write_study_design_to_DMS <- function(data_package_num, 
                                      study_design, 
                                      overwrite = FALSE) 
{
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

