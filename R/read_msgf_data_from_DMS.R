#' Reading MS-GF+ output from PNNL's DMS
#'
#' @param data_package_num numeric or character; the Data Package ID in the DMS.
#' @param param_file character; MS-GF+ parameter file. No need to specify this
#'   if there is only one parameter file associated with the jobs.
#' @param organism_db character; FASTA file name. This is the same as the
#'   \code{Organism DB} column. No need to specify this if there is only one
#'   FASTA file associated with the jobs.
#' @param use_mzIdentML logical; whether to read mzIdentML files into `psms`
#'   `data.table` slot of the `MSnID`.
#'
#' @details If `use_mzIdentML = TRUE`, a progress bar will not be displayed
#'   while the results are being fetched. Also, the table in the `psms` slot
#'   will not be the same as when `mzIdentML = FALSE`. As a result, it may
#'   require additional modification by the user to prepare it for downstream
#'   processing steps.
#'
#' @return (MSnID) MSnID object
#'
#' @md
#'
#' @importFrom MSnID convert_msgf_output_to_msnid read_mzIDs MSnID
#' @importFrom data.table data.table rbindlist
#'
#' @examples
#' msnid <- read_msgf_data_from_DMS(3606)
#' show(msnid)
#'
#' #
#' msnid <- read_msgf_data_from_DMS(3606, use_mzIdentML = TRUE)
#' show(msnid)

#' @export
read_msgf_data_from_DMS <- function(data_package_num, 
                                    param_file = NULL,
                                    organism_db = NULL,
                                    use_mzIdentML = FALSE) 
{
  # Fetch job records for data package(s)
  if (length(data_package_num) > 1) {
    job_rec_ls <- lapply(data_package_num, get_job_records_by_dataset_package)
    jobRecords <- rbindlist(job_rec_ls)
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
  
  # FASTA files (organism_db)
  organism_dbs <- unique(jobRecords$`Organism DB`)
  
  # Check if organism_db is NULL
  if (is.null(organism_db)) {
    # If there is a single FASTA file, assign it to organism_db
    if (length(organism_dbs) == 1) {
      organism_db <- organism_dbs
    } else {
      # If there are multiple FASTA files, and organism_db is NULL,
      # halt execution and print an error message.
      stop(paste("There are multiple FASTA files.", 
                 "Please specify which FASTA file to use with organism_db.")
      )
    }
  } else {
    # If organism_db is not NULL, check validity.
    if (!(organism_db %in% organism_dbs)) {
      stop(sprintf("'%s' is not a valid FASTA file.", organism_db))
    }
  }
  
  # Subset to specific parameter file and FASTA file
  jobRecords <- jobRecords[jobRecords$Parameter_File == param_file &
                             jobRecords$`Organism DB` == organism_db, ]
  
  if (use_mzIdentML) {
    mzid_files <- list.files(jobRecords$Folder, pattern = ".mzid.gz", 
                             full.names = TRUE)
    
    # Read mzIdentML files into psms slot
    # Note: adding a progress bar with llply drastically  
    # increases memory usage and time
    suppressMessages(
      msnid <- MSnID::read_mzIDs(MSnID(), mzid_files, backend = "mzR")
    )
  } else {
    results <- get_results_for_multiple_jobs.dt(jobRecords) 
    tool <- unique(jobRecords$Tool)
    pattern <- tool2suffix[[tool]]
    results <- results[[pattern]]
    msnid <- convert_msgf_output_to_msnid(results)
  }
  return(msnid)
}

