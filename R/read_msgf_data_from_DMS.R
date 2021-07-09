#' Reading MSGF output from PNNL's DMS
#'
#' @param data_package_num (Numeric or Character vector) containing Data Package ID(s) located in DMS
#' @return (data.frame) concatenated job results
#' @param data_package_num (Numeric or Character vector) containing Data Package ID(s) located in DMS
#' @param param_file (character) MS-GF+ parameter file. 
#'                   If the data package jobs refer to only one parameter 
#'                   file, then NULL (default) can be accepted as the argument.
#' @return (MSnID) MSnID object
#' 
#' @importFrom MSnID convert_msgf_output_to_msnid
#' 
#' @examples
#' msnid <- read_msgf_data_from_DMS(3606)
#' show(msnid)

#' @export
read_msgf_data_from_DMS <- function(data_package_num, param_file = NULL) {
  # Fetch job records for data package(s)
  if (length(data_package_num) > 1) {
    job_rec_ls <- lapply(data_package_num, get_job_records_by_dataset_package)
    jobRecords <- Reduce(rbind, job_rec_ls)
  } else {
    jobRecords <- get_job_records_by_dataset_package(data_package_num)
  }
  
  jobRecords <- jobRecords[grepl("MSGFPlus", jobRecords$Tool),]
  
  # THIS PARAMETER ARGUMENT CHECK CAN BE MORE ELEGANT
  # Check parameter file. Is there any redundancy?
  param_files <- unique(jobRecords$Parameter_File)
  
  # Check if param_file is NULL
  if (is.null(param_file)) {
    # If there is a single parameter file, assign it to param_file
    if (length(param_files) == 1) {
      param_file <- param_files
    } else {
      # If there are multiple parameter files, and param_file is NULL,
      # halt execution and print an error message.
      stop(paste("There are multiple parameter files.", 
                 "Please specify which parameter file to use with param_file.")
      )
    }
  } else {
    # If param_file is not NULL, check validity.
    if (!(param_file %in% param_files)) {
      stop(paste0("\"", param_file, "\"", " is not a valid parameter file."))
    }
  }
  
  jobRecords <- jobRecords[jobRecords$Parameter_File == param_file, ]
  
  results <- get_results_for_multiple_jobs.dt(jobRecords) 
  tool <- unique(jobRecords$Tool)
  pattern <- tool2suffix[[tool]]
  results <- results[[pattern]]
  msnid <- convert_msgf_output_to_msnid(results)
  return(msnid)
}


