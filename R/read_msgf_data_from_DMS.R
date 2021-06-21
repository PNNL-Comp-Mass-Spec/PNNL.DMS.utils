#' Reading MSGF output from PNNL's DMS
#'
#' @param DataPkgNumber (Numeric or Character vector) containing Data Package ID(s) located in DMS
#' @return (MSnID) MSnID object
#' @importFrom PlexedPiper convert_msgf_output_to_msnid
#' 
#' @examples
#' msnid <- read_msgf_data_from_DMS(3442)
#' print(msnid)
#' head(MSnID::psms(msnid))

#' @export
read_msgf_data_from_DMS <- function(DataPkgNumber) {
  # Fetch job records for data package(s)
  if (length(DataPkgNumber) > 1) {
    job_rec_ls <- lapply(DataPkgNumber, get_job_records_by_dataset_package)
    jobRecords <- Reduce(rbind, job_rec_ls)
  } else {
    jobRecords <- get_job_records_by_dataset_package(DataPkgNumber)
  }

  jobRecords <- jobRecords[grepl("MSGFPlus", jobRecords$Tool),]
  results <- get_results_for_multiple_jobs.dt(jobRecords) 
  
  tool <- unique(jobRecords$Tool)
  pattern <- tool2suffix[[tool]]
  results <- results[[pattern]]
  msnid <- convert_msgf_output_to_msnid(results)
  return(msnid)
}

