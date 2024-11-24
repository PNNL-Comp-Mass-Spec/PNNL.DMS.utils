#' Reading generic tool output from PNNL's DMS
#'
#' @param data_package_num numeric or character; the Data Package ID in the DMS.
#' @param tool character; MS-GF+ parameter file. No need to specify this
#'   if there is only one parameter file associated with the jobs.
#'
#' @return (data.table) results in the form of one data.table
#'
#' @md
#'
#' @importFrom data.table data.table rbindlist
#'
#' @examples
#' if (is_PNNL_DMS_connection_successful()) {
#'   x <- read_generic_job_from_DMS(6148, "Decon2LS_V2")
#'   (x)
#' }
#'
#' @export

read_generic_job_from_DMS <- function(data_package_num, tool) 
{
  if (length(data_package_num) > 1) {
    job_rec_ls <- lapply(data_package_num, get_job_records_by_dataset_package)
    jobRecords <- rbindlist(job_rec_ls)
  }
  else {
    jobRecords <- get_job_records_by_dataset_package(data_package_num)
  }
  
  jobRecords <- jobRecords[tool == jobRecords$tool,]
  results <- get_results_for_multiple_jobs.dt(jobRecords)
  results <- rbindlist(results)
  return(results)
}
