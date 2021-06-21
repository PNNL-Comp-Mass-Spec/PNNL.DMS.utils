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
#' @param dataPkgNumber (integer) data package number for DMS
#'
#' @importFrom odbc odbc dbConnect dbSendQuery dbFetch dbClearResult dbDisconnect
#' @importFrom PlexedPiper read_study_design
#'
#' @name read_study_design
#'
#' @examples
#' study_design <- read_study_design_from_DMS(3606)
#' 
#' fractions  <- study_design$fractions
#' samples    <- study_design$samples
#' references <- study_design$references


#' @export
#' @rdname read_study_design
# gets 3 study design files from DMS
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
  
  return(study_design)
}