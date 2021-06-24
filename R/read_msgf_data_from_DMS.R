#' Reading MSGF output from PNNL's DMS
#'
#' @param data_package_num (Numeric or Character vector) containing Data Package ID(s) located in DMS
#' @return (data.frame) concatenated job results
=======
#' @param data_package_num (Numeric or Character vector) containing Data Package ID(s) located in DMS
#' @param param_file (character) MS-GF+ parameter file. 
#'                   If the data package jobs refer to only one parameter 
#'                   file, then NULL (default) can be accepted as the argument.
#' @return (MSnID) MSnID object
#' 
#' @examples
#' results <- read_msgf_data_from_DMS(3442)
#' head(results)

#' @export
read_msgf_data_from_DMS <- function(data_package_num, param_file = NULL) {
  # Fetch job records for data package(s)
  if (length(DataPkgNumber) > 1) {
    job_rec_ls <- lapply(DataPkgNumber, get_job_records_by_dataset_package)
    jobRecords <- Reduce(rbind, job_rec_ls)
  } else {
    jobRecords <- get_job_records_by_dataset_package(DataPkgNumber)
  }
  
  jobRecords <- jobRecords[grepl("MSGFPlus", jobRecords$Tool),]
  
  # THIS PARAMETER ARGUMENT CHECK CAN BE MORE ELEGANT
  # Check parameter file. Is there any redundancy?
  param_files <- unique(jobRecords$Parameter_File)
  if(length(param_files) > 1 & is.null(param_file))
    stop("There are multiple parameter files and exact one to use isn't specified!")
  # it proceeds to this point if `param_file` isn't NULL
  if(length(param_files) > 1 & !(param_file %in% param_files))
    stop("There are multiple parameter, but the specified one isn't one of them!")
  if(!is.null(param_file) & param_file != param_files)
    stop("There specified parameter file ins't the one that is in the data package!")
  # at this point everything should be fine
  if(length(param_files) == 1)
    param_file <- unique(jobRecords$Parameter_File)
  
  jobRecords <- jobRecords[jobRecords$Parameter_File == param_file,]
  
  results <- get_results_for_multiple_jobs.dt(jobRecords) 
  tool <- unique(jobRecords$Tool)
  pattern <- tool2suffix[[tool]]
  results <- results[[pattern]]
  return(results)
}


