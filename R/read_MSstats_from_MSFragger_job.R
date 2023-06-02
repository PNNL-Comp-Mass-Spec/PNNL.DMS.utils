#' Reading MSFragger-generated MSstats from PNNL's DMS as MSnSet object
#'
#' @description
#' Function has only been tested with label-free intensity-based quantification 
#' data. It has not been tested with TMT data. MSstats.csv is an optional output 
#' file which needs to be specified in DMS settings file.
#' 
#' @param data_package_num numeric or character; the Data Package ID in the DMS.
#' @param param_file character; MSFragger parameter file. No need to specify this
#'   if there is only one parameter file associated with the jobs.
#' @param settings_file character; MSFragger settings file. No need to specify this
#'   if there is only one settings file associated with the jobs.
#' @param organism_db character; FASTA file name. This is the same as the
#'   \code{Organism DB} column. No need to specify this if there is only one
#'   FASTA file associated with the jobs.
#'   
#' @return (MSnSet) MSnSet object
#'
#' @md
#'
#' @importFrom MSnbase MSnSet
#' @importFrom data.table fread
#' @importFrom tidyr pivot_wider
#' @importFrom dplyr %>% select filter distinct
#' @importFrom MSnSet.utils log2_zero_center
#' @examples
#' msnset <- read_MSstats_from_MSFragger_job(4139)
#' show(msnset)
#'

#' @export

read_MSstats_from_MSFragger_job <- function(data_package_num, 
                                         param_file = NULL, 
                                         settings_file = NULL, 
                                         organism_db = NULL)
{
  job_records <- PNNL.DMS.utils::get_job_records_by_dataset_package(data_package_num)
  job_records <- job_records %>%
    filter(Tool == "MSFragger")
  if(!is.null(param_file))
    job_records <- filter(job_records, Parameter_File == param_file)
  if(!is.null(settings_file))
    job_records <- filter(job_records, Settings_File == settings_file)
  if(!is.null(organism_db))
    job_records <- filter(job_records, Organism_DB == organism_db)
  path <- unique(job_records$Folder)
  if (length(path) == 0) {
    stop("No jobs found.")
  }
  if (length(path) > 1) {
    stop("More than one MSFragger search found per data package. \nPlease refine the arguments to make sure they uniquely define the MSFragger job.")
  }
  # path <- r"(\\protoapps\DataPkgs\Public\2023\4857_microglia_union_of_3_batches_selected_datasets\MSF202305221734_Auto2186046)" #DP 4857
  path_to_file <- file.path(path, "MSstats.csv")
  if(!file.exists(path_to_file)){
     stop("MSstats.csv file not found.")
  }

  df <- fread(path_to_file,
            showProgress = TRUE,
            data.table = FALSE)
  
  df <- df %>% #May add charge col later
     select(ProteinName, PeptideSequence, Run, Intensity)
  # Will sum intensity of unique features.
  x_data <- df %>%
     filter(!is.na(Intensity)) %>%
     pivot_wider(id_cols = "PeptideSequence", 
                 names_from = "Run", 
                 values_from = "Intensity", 
                 values_fn = sum) %>% 
     as.data.frame() %>%
     `rownames<-`(., .$PeptideSequence) %>%
     select(-PeptideSequence) %>%
     as.matrix()
  
  f_data <- df %>%
     distinct(ProteinName, PeptideSequence) %>%
     as.data.frame() %>%
     `rownames<-`(., .$PeptideSequence)
  
  p_data <- df %>%
     distinct(Run) %>%
     as.data.frame() %>%
     `rownames<-`(., .$Run)
  
  m <- MSnSet(exprs = x_data[rownames(f_data), rownames(p_data)], 
                   fData = f_data, 
                   pData = p_data)
  
  m <- log2_zero_center(m)

  return(m)
}
