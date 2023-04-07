#' Reading MSFragger output from PNNL's DMS
#'
#' @param data_package_num numeric or character; the Data Package ID in the DMS.
#' @param param_file character; MSFragger parameter file. No need to specify this
#'   if there is only one parameter file associated with the jobs.
#' @param settings_file character; MSFragger settings file. No need to specify this
#'   if there is only one settings file associated with the jobs.
#' @param organism_db character; FASTA file name. This is the same as the
#'   \code{Organism DB} column. No need to specify this if there is only one
#'   FASTA file associated with the jobs.
#' @param assume_inference logical; If `TRUE` then takes the `Protein` column
#'   as protein ids that passed the inference step. Otherwise, the function
#'   will take both `Protein` and `Mapped Proteins` and report all of them on
#'   separate rows for the matching peptides.
#'   
#' @return (MSnID) MSnID object
#'
#' @md
#'
#' @importFrom MSnID MSnID psms
#' @importFrom data.table data.table rbindlist setnames :=
#' @importFrom plyr llply
#' @importFrom readr read_tsv
#' @importFrom purrr map
#' @importFrom tibble enframe
#' @importFrom tidyr unnest
#' @importFrom dplyr %>% any_of select
#'
#' @examples
#' msnid <- read_msfragger_data_from_DMS(4139)
#' show(msnid)
#'


#' @export

read_msfragger_data_from_DMS <- function(data_package_num, 
                                         param_file = NULL, 
                                         settings_file = NULL, 
                                         organism_db = NULL,
                                         assume_inference = FALSE)
{
  on.exit(unlink(tempdir()))
  
  job_records <- get_job_records_by_dataset_package(data_package_num)
  
  # add filters on tool, parameter file and setting file
  job_records <- job_records %>%
    filter(Tool == "MSFragger")
  
  if(!is.null(param_file))
    job_records <- filter(job_records, Parameter_File == param_file)
    
  if(!is.null(param_file))
    job_records <- filter(job_records, Settings_File == settings_file)
  
  if(!is.null(organism_db))
    job_records <- filter(job_records, Organism_DB == organism_db)
  
  
  path <- unique(job_records$Folder)
  
  if (length(path) == 0) {
    stop("No jobs found.")
  }
  
  if (length(path) > 1) {
    stop("More than one MSFragger search found per data package. 
Please refine the arguments to make sure they uniquely define the MSFragger job.")
  }
  
  
  path <- file.path(path, "Dataset_PSM_tsv.zip")
  
  
  fileNamePttrn <- "_psm\\.tsv"
  
  # Copy folder to temporary directory and extract psm files
  unzip(zipfile = path, list = FALSE, exdir = tempdir())
  path_to_files <- list.files(tempdir(), pattern = fileNamePttrn, full.names = TRUE)
  
  short_dataset_names <- unlist(strsplit(basename(path_to_files), split = fileNamePttrn))
  
  dt <- llply(path_to_files, read_tsv, col_types = readr::cols(), 
              guess_max = Inf, progress = FALSE) %>% 
    map(dplyr::select, -any_of("Spectrum File")) %>% 
    setNames(short_dataset_names) %>% 
    enframe(name = "Spectrum File") %>% 
    unnest(value)
  
  if(!assume_inference){
    # prot_mapping <- dt %>%
    #   distinct(Protein, `Mapped Proteins`) %>%
    #   unite(Protein, `Mapped Proteins`, col = "all_prot", na.rm = T, sep = ", ", remove = FALSE) %>% 
    #   separate_rows(all_prot, sep = ", ") %>%
    #   select(Protein, all_prot)
    # 
    # dt <- inner_join(dt, prot_mapping) %>%
    #   select(-Protein) %>%
    #   rename(Protein = all_prot)
    
    dt <- dt %>% 
      unite(Protein, `Mapped Proteins`, col = "Protein", na.rm = T, sep = ", ") %>% 
      separate_rows(Protein, sep = ", ")
  }
  
  dt <- as.data.table(dt)

  new_names <- c("spectrumFile" = "Spectrum File",
                 "accession" = "Protein",
                 "chargeState" = "Charge",
                 "calculatedMassToCharge" = "Calculated M/Z",
                 "experimentalMassToCharge" = "Calibrated Observed M/Z",
                 "pepSeq" = "Peptide",
                 "spectrumID" = "Spectrum")
  setnames(dt, old = new_names, new = names(new_names))
  
  dt[, isDecoy := grepl("^XXX_", accession)]
  
  dt <- mutate(dt, peptide = paste(`Prev AA`, pepSeq,  `Next AA`, sep = "."))
  
  # Create MSnID to store results
  suppressMessages(
    msnid <- MSnID::MSnID()
  )
  psms(msnid) <- dt
  
  return(msnid)
}
