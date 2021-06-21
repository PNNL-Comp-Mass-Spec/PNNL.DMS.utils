#' Read AScore results from folder
#'
#' This function read AScore results from a local folder.
#'
#' @param DataPkgNumber (Numeric or Character vector) containing Data Package ID(s) located in DMS
#' @return ascore (data.frame) AScore results
#'
#' @importFrom dplyr rename inner_join
#' @importFrom readr read_tsv
#' @importFrom odbc odbc dbConnect dbSendQuery dbFetch dbClearResult dbDisconnect
#' @export read_AScore_results_from_DMS
#'
#'
#' @examples
#' dataPkgNumber <- 3625
#' msnid <- read_msms_data_from_DMS(dataPkgNumber)
#' ascore <- read_AScore_results_from_DMS(dataPkgNumber)
#' msnid <- best_PTM_location_by_ascore(msnid, ascore)
#'



#' @export
#' @rdname pnnl_dms_utils
# Get AScore results for a given data package (e.g. 3432)
read_AScore_results_from_DMS <- function(dataPkgNumber){
  #
  con_str <- sprintf("DRIVER={%s};SERVER=gigasax;DATABASE=dms5;%s",
                     get_driver(),
                     get_auth())
  con <- dbConnect(odbc(), .connection_string=con_str)
  strSQL <- sprintf("SELECT *
                     FROM V_Mage_Analysis_Jobs
                     WHERE (Dataset LIKE 'DataPackage_%s%%')", dataPkgNumber)
  qry <- dbSendQuery(con, strSQL)
  job <- dbFetch(qry)
  dbClearResult(qry)
  dbDisconnect(con)
  
  if(nrow(job) > 1){
    warning("Multiple Ascore jobs detected. Selecting the last one.")
    job <- tail(job, 1)
  }
  
  # in case Mac OS
  if(.Platform$OS.type == "unix"){
    local_folder <- "~/temp_AScoreResults"
    if(file.exists(local_folder)){
      unlink(local_folder, recursive = T)
    }
    dir.create(local_folder)
    remote_folder <- gsub("\\\\","/",job['Folder'])
    mount_cmd <- sprintf("mount -t smbfs %s %s", remote_folder, local_folder)
    system(mount_cmd)
    # read the stuff
    ascores <- read_tsv(
      file.path(local_folder,"Concatenated_msgfplus_syn_ascore.txt"))
    job_to_dataset_map <- read_tsv(
      file.path(local_folder,"Job_to_Dataset_Map.txt"))
    # end of read the stuff
    umount_cmd <- sprintf("umount %s", local_folder)
    system(umount_cmd)
    unlink(local_folder, recursive = T)
  }else if(.Platform$OS.type == "windows"){
    # in case Windows
    ascores <- read_tsv(
      file.path(job['Folder'],"Concatenated_msgfplus_syn_ascore.txt"))
    job_to_dataset_map <- read_tsv(
      file.path(job['Folder'],"Job_to_Dataset_Map.txt"))
  }else{
    stop("unknown OS")
  }
  
  res <- inner_join(ascores, job_to_dataset_map) %>%
    rename(spectrumFile = Dataset)
  
  return(res)
}