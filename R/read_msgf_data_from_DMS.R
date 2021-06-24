#' Reading MSGF output from PNNL's DMS
#'
#' @param data_package_num (Numeric or Character vector) containing Data Package ID(s) located in DMS
#' @return (data.frame) concatenated job results
#' 
#' @examples
#' results <- read_msgf_data_from_DMS(3442)
#' head(results)

#' @export
read_msgf_data_from_DMS <- function(data_package_num) {
  # Fetch job records for data package(s)
  if (length(data_package_num) > 1) {
    job_rec_ls <- lapply(data_package_num, get_job_records_by_dataset_package)
    jobRecords <- Reduce(rbind, job_rec_ls)
  } else {
    jobRecords <- get_job_records_by_dataset_package(data_package_num)
  }

  jobRecords <- jobRecords[grepl("MSGFPlus", jobRecords$Tool),]
  results <- get_results_for_multiple_jobs.dt(jobRecords) 
  
  tool <- unique(jobRecords$Tool)
  pattern <- tool2suffix[[tool]]
  results <- results[[pattern]]
  return(results)
}

