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
#' @return (MSnSet) MSnSet object
#'
#' @importFrom MSnbase MSnSet
#' @importFrom Biobase exprs `exprs<-`
#' @importFrom data.table fread
#' @importFrom tidyr pivot_wider
#' @importFrom tibble column_to_rownames
#' @importFrom dplyr %>% select filter distinct
#' @importFrom stats median
#'
#' @examples
#' if (is_PNNL_DMS_connection_successful()) {
#'   msnset <- read_MSstats_from_MSFragger_job(4938)
#'   show(msnset)
#' }
#'
#' @export read_MSstats_from_MSFragger_job


read_MSstats_from_MSFragger_job <- function(data_package_num, 
                                            param_file = NULL, 
                                            settings_file = NULL, 
                                            organism_db = NULL)
{
   job_records <- 
      PNNL.DMS.utils::get_job_records_by_dataset_package(data_package_num)
   
   # add filters on tool, parameter file and setting file
   job_records <- filter(job_records, Tool == "MSFragger")
   
   if (!is.null(param_file)) {
      job_records <- filter(job_records, Parameter_File == param_file)
   }
   
   if (!is.null(settings_file)) {
      job_records <- filter(job_records, Settings_File == settings_file)
   }
   
   if (!is.null(organism_db)) {
      job_records <- filter(job_records, Organism_DB == organism_db)
   }
   
   path <- unique(job_records$Folder)
   
   if (length(path) == 0) {
      stop("No jobs found.")
   }
   
   if (length(path) > 1) {
      stop(paste0("More than one MSFragger search found per data package.\n", 
                  "Please refine the arguments to make sure they uniquely", 
                  " define the MSFragger job."))
   }
   
   path_to_file <- file.path(path, "MSstats.csv")
   
   if (!file.exists(path_to_file)) {
      stop(sprintf("MSstats.csv file not found in %s", path))
   }
   
   df <- fread(path_to_file, showProgress = TRUE, data.table = FALSE) %>%
      # May add charge col later
      select(ProteinName, PeptideSequence, Run, Intensity)
   
   # Will sum intensity of unique features.
   x_data <- df %>%
      filter(!is.na(Intensity)) %>%
      pivot_wider(id_cols = "PeptideSequence", 
                  names_from = "Run", 
                  values_from = "Intensity", 
                  values_fn = sum) %>% 
      as.data.frame() %>%
      column_to_rownames(var = "PeptideSequence") %>% 
      as.matrix()
   
   f_data <- df %>%
      distinct(ProteinName, PeptideSequence) %>%
      `rownames<-`(., .$PeptideSequence)
   
   p_data <- df %>%
      select(Run) %>%
      `rownames<-`(., .$Run)
   
   x_data <- x_data[rownames(f_data), rownames(p_data)]
   
   m <- MSnSet(exprs = x_data, fData = f_data, pData = p_data)
   m <- log2_zero_center(m)
   
   return(m)
}


# Same as MSnSet.utils::log2_zero_center
log2_zero_center <- function(m) {
   exprs(m) <- log2(exprs(m))
   
   if (any(is.infinite(exprs(m))) == TRUE) {
      stop("After transformation, infinite values are present.")
   }
   
   exprs(m) <- sweep(exprs(m), MARGIN = 1, 
                     STATS = apply(exprs(m), 1, FUN = median, na.rm = TRUE), 
                     FUN = "-")
   
   return(m)
}


utils::globalVariables(
   c("Parameter_File", "Settings_File", "Organism_DB", "ProteinName",
     "PeptideSequence", "Run", "Intensity", ".")
)

