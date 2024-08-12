#' Reading MSFragger output from PNNL's DMS
#'
#' @param data_package_num numeric or character; the Data Package ID in the DMS.
#' @param param_file character; MSFragger parameter file. No need to specify
#'   this if there is only one parameter file associated with the jobs.
#' @param settings_file character; MSFragger settings file. No need to specify
#'   this if there is only one settings file associated with the jobs.
#' @param organism_db character; FASTA file name. This is the same as the
#'   \code{Organism DB} column. No need to specify this if there is only one
#'   FASTA file associated with the jobs.
#' @param assume_inference logical; If `TRUE` then takes the `Protein` column as
#'   protein ids that passed the inference step. Otherwise, the function will
#'   take both `Protein` and `Mapped Proteins` and report all of them on
#'   separate rows for the matching peptides.
#'
#' @return (MSnID) MSnID object
#'
#' @md
#'
#' @importFrom MSnID MSnID psms `psms<-`
#' @importFrom data.table as.data.table `:=` setnames
#' @importFrom readr read_tsv cols
#' @importFrom tidyr fill all_of unite separate_rows
#' @importFrom dplyr %>% mutate bind_rows filter
#' @importFrom utils unzip
#'
#' @examples
#' if (is_PNNL_DMS_connection_successful()) {
#'   msnid <- read_msfragger_data_from_DMS(4139)
#'   show(msnid)
#' }
#'
#' @export


read_msfragger_data_from_DMS <- function(data_package_num, 
                                         param_file = NULL, 
                                         settings_file = NULL, 
                                         organism_db = NULL,
                                         assume_inference = FALSE)
{
   # on.exit(unlink(tempdir()))
   
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

   if (length(path) == 0) {
      stop("No jobs found.")
   }
   
   if (length(path) > 1) {
      stop(paste0("More than one MSFragger search found per data package.\n", 
                  "Please refine the arguments to make sure they uniquely", 
                  " define the MSFragger job."))
   }
   
   aggregate_zip_file_exists <- file.exists(
      file.path(local_folder, "Dataset_PSM_tsv.zip"))
   
   if (aggregate_zip_file_exists) {
      # Copy folder to temporary directory and extract psm files
      aggregate_zip_file <- file.path(local_folder, "Dataset_PSM_tsv.zip")
      exdir <- .new_tempdir()
      unzip(zipfile = aggregate_zip_file, list = FALSE, exdir = exdir)
      local_folder <- exdir
   }
   
   fileNamePttrn <- "_psm\\.tsv"
   # 
   # 
   path_to_files <- list.files(local_folder, pattern = fileNamePttrn, full.names = TRUE)
   cn <- colnames(read_tsv(path_to_files[1], n_max = 1))
   
   # last_col <- which(cn == "Quan Usage") - 1
   keep_cols <- c("Spectrum", "Spectrum File", "Protein", "Mapped Proteins",
                  "Charge", "Calculated M/Z", "Calibrated Observed M/Z",
                  "Peptide", "Prev AA", "Next AA")
   
   dt <- lapply(path_to_files, function(path_i) {
      read_tsv(file = path_i, col_types = readr::cols(),
               col_select = any_of(keep_cols), guess_max = Inf,
               progress = FALSE) %>% 
         tidyr::fill(`Spectrum File`)
   }) %>% 
      bind_rows() %>%
      mutate(`Spectrum File` = basename(`Spectrum File`),
             `Spectrum File` = sub("interact-", "", `Spectrum File`),
             `Spectrum File` = sub("\\.pep\\.xml", "", `Spectrum File`),
             `Spectrum File` = sub(fileNamePttrn, "", `Spectrum File`))
   
   system(glue::glue("umount {mount_folder}"))
   
   if (!assume_inference) {
      dt <- dt %>%
         unite(Protein, `Mapped Proteins`, col = "Protein", 
               na.rm = TRUE, sep = ", ") %>%
         separate_rows(Protein, sep = ", ")
   }
   
   dt <- as.data.table(dt)
   
   new_names <- c("spectrumFile"             = "Spectrum File",
                  "accession"                = "Protein",
                  "chargeState"              = "Charge",
                  "calculatedMassToCharge"   = "Calculated M/Z",
                  "experimentalMassToCharge" = "Calibrated Observed M/Z",
                  "pepSeq"                   = "Peptide",
                  "spectrumID"               = "Spectrum")
   
   setnames(dt, old = new_names, new = names(new_names))
   
   dt[, `:=`(isDecoy = grepl("^XXX_", accession),
             peptide = paste(`Prev AA`, pepSeq, `Next AA`, sep = "."))]
   
   # Create MSnID to store results
   suppressMessages(msnid <- MSnID::MSnID())
   psms(msnid) <- dt
   
   return(msnid)
}


utils::globalVariables(
   c("Parameter_File", "Settings_File", "Organism_DB", "Spectrum File",
     "Protein", "Mapped Proteins", "accession", "Prev AA", "pepSeq", "Next AA")
)

