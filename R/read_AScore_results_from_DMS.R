#' Read AScore results from folder
#'
#' This function read AScore results from a local folder.
#'
#' @param data_package_num (Numeric or Character vector) containing Data Package
#'   ID(s) located in DMS
#' @return ascore (data.frame) AScore results
#'
#' @importFrom dplyr rename inner_join filter
#' @importFrom readr read_tsv
#' @importFrom DBI dbConnect dbSendQuery dbFetch dbClearResult
#'   dbDisconnect
#' @importFrom RPostgres Postgres
#' @export read_AScore_results_from_DMS
#'
#' @examples
#' if (is_PNNL_DMS_connection_successful()) {
#'   data_package_num <- 3625
#'   # msnid <- read_msgf_data_from_DMS(data_package_num) # Not run
#'   ascore <- read_AScore_results_from_DMS(data_package_num)
#'   # Use PlexedPiper to make use of the Ascore results
#'   # msnid <- PlexedPiper::best_PTM_location_by_ascore(msnid, ascore)
#' }
#' 
#' @export

# Get AScore results for a given data package (e.g. 3432)
read_AScore_results_from_DMS <- function(data_package_num){
  # Prevent "no visible binding for global variable" note
  Dataset <- NULL
  
  con <- get_db_connection()
  strSQL <- sprintf("SELECT *
                     FROM v_mage_analysis_jobs
                     WHERE (dataset LIKE 'DataPackage_%s%%')", data_package_num)
  qry <- dbSendQuery(con, strSQL)
  job <- dbFetch(qry)
  job <- dplyr::filter(job, tool == "Phospho_FDR_Aggregator")
  dbClearResult(qry)
  dbDisconnect(con)
  
  if(nrow(job) > 1){
    job <- tail(job, 1)
    warning(paste0("Multiple Ascore jobs detected. Selecting the last one: ",
                   job[["Job"]]))
  }
  
  # in case Mac OS
  if(.Platform$OS.type == "unix"){
    local_folder <- "~/temp_AScoreResults"
    if(file.exists(local_folder)){
      unlink(local_folder, recursive = T)
    }
    dir.create(local_folder)
    remote_folder <- gsub("\\\\", "/", job['folder'])
    mount_cmd <- sprintf("mount -t smbfs %s %s", remote_folder, local_folder)
    system(mount_cmd)
    # read the stuff
    ascores <- read_tsv(
      file.path(local_folder,"Concatenated_msgfplus_syn_ascore.txt"))
    job_to_dataset_map <- read_tsv(
      file.path(local_folder,"Job_to_Dataset_Map.txt"))
    # end of read the stuff
    umount_cmd <- sprintf("diskutil unmount %s", local_folder)
    system(umount_cmd)
    unlink(local_folder, recursive = T)
  }else if(.Platform$OS.type == "windows"){
    # in case Windows
    ascores <- read_tsv(
      file.path(job['Folder'],"Concatenated_msgfplus_syn_ascore.txt"))
    job_to_dataset_map <- read_tsv(
      file.path(job['Folder'], "Job_to_Dataset_Map.txt"))
  }else{
    stop("unknown OS")
  }
  
  res <- inner_join(ascores, job_to_dataset_map) %>%
    rename(spectrumFile = Dataset)
  
  return(res)
}


# Prevent "no visible binding for global variable" note.
utils::globalVariables(c("Tool"))

