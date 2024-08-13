#' Reading MSFragger-generated MSstats from PNNL's DMS as MSnSet object
#'
#' @description Function has only been tested with label-free intensity-based
#'   quantification data. It has not been tested with TMT data. MSstats.csv is
#'   an optional output file which needs to be specified in DMS settings file.
#'
#' @param data_package_num numeric or character; the Data Package ID in the DMS.
#' @param param_file character; MSFragger parameter file. No need to specify
#'   this if there is only one parameter file associated with the jobs.
#' @param settings_file character; MSFragger settings file. No need to specify
#'   this if there is only one settings file associated with the jobs.
#' @param organism_db character; FASTA file name. This is the same as the
#'   \code{Organism DB} column. No need to specify this if there is only one
#'   FASTA file associated with the jobs.
#'
#' @return (MSnSet) MSnSet object of FragPipe LFQ results
#'
#' @importFrom MSnbase MSnSet
#' @importFrom MSnSet.utils read_FragPipe_LFQ
#' @importFrom data.table fread
#' @importFrom tidyr pivot_wider
#' @importFrom tibble column_to_rownames
#' @importFrom dplyr %>% select filter distinct relocate everything
#' @importFrom glue glue
#'
#' @examples
#' if (is_PNNL_DMS_connection_successful()) {
#'   msnset <- read_MSstats_from_MSFragger_job(
#'     data_package_num = 4938,
#'     param_file = "MSFragger_Tryp_Dyn_MetOx_ProtNTermAcet_Stat_CysAlk_20ppmParTol.params",
#'     settings_file = "MSFragger_MatchBetweenRuns_Java80GB.xml",
#'     organism_db = "ID_008026_7A1842EC.fasta")
#'   show(msnset)
#' }
#'
#' @export read_MSstats_from_MSFragger_job


read_MSstats_from_MSFragger_job <- function(data_package_num, 
                                            param_file = NULL, 
                                            settings_file = NULL, 
                                            organism_db = NULL)
{
   job_records <- get_job_records_by_dataset_package(data_package_num)
   
   # add filters on tool, parameter file and setting file
   job_records <- filter(job_records, tool == "MSFragger")
   
   if (!is.null(param_file)) {
      job_records <- filter(job_records, parameter_file == param_file)
   }
   
   if (!is.null(settings_file)) {
      job_records <- filter(job_records, settings_file == !!settings_file)
   }
   
   if (!is.null(organism_db)) {
      job_records <- filter(job_records, organism_db == !!organism_db)
   }
   
   path <- unique(job_records$folder)
   remote_folder <- gsub("\\\\", "/", path)
   mount_folder <- local_folder <- .new_tempdir()
   mount_cmd <- sprintf("mount -t smbfs %s %s", remote_folder, local_folder)
   system(mount_cmd)
   on.exit(system(glue("umount {mount_folder}")))

   if (length(path) == 0) {
      stop("No jobs found.")
   }
   
   if (length(path) > 1) {
      stop(paste0("More than one MSFragger search found per data package.\n", 
                  "Please refine the arguments to make sure they uniquely", 
                  " define the MSFragger job."))
   }
   path_to_file <- file.path(local_folder, "MSstats.csv")
   
   if (!file.exists(path_to_file)) {
      stop(sprintf("MSstats.csv file not found in %s", path))
   }
   
   m <- read_FragPipe_LFQ(path_to_file)
   
   return(m)
}




utils::globalVariables(
   c("Parameter_File", "Settings_File", "Organism_DB", "ProteinName",
     "PeptideSequence", "Run", "Intensity", ".", "featureName")
)

