#' Reading MASIC results from PNNL's DMS
#'
#' @param data_package_num (numeric or character vector) containing Data Package
#'   ID(s) located in DMS.
#'   
#' @param interference_score (logical) read interference score. Default is
#'   \code{FALSE}.
#'   
#' @param idscanpattern Ion fragmentation method used to produce the scan used for 
#'    peptide identification
#'    
#' @param quantScanPattern Ion fragmentation method used to produce the scan used for 
#'    peptide quantification
#'
#' @return (data.frame) with reporter ion intensities and other metrics
#'
#' @importFrom dplyr select contains starts_with any_of rename
#' @importFrom data.table rbindlist
#'
#' @export read_masic_data_from_DMS
#' 
#' @examples
#' if (is_PNNL_DMS_connection_successful()) {
#' test <- read_masic_data_from_DMS(4970)
#' }


read_masic_data_from_DMS <- function(data_package_num, 
                                     interference_score=FALSE, 
                                     idScanPattern = "ms2", 
                                     quantScanPattern = "ms2")
{
   # Prevent "no visible binding for global variable" note
   Dataset <- Scan <- QuantScan <- IDScan <- 
      FragScanNumber <- ScanNumber <- NULL
   
   on.exit(gc(), add = TRUE)
   
   # Fetch job records for data package(s)
   if(length(data_package_num) > 1){
      jobRecords <- lapply(data_package_num, get_job_records_by_dataset_package)
      jobRecords <- rbindlist(jobRecords)
   } else {
      jobRecords <- get_job_records_by_dataset_package(data_package_num)
   }
   
   jobRecords <- jobRecords[grepl("MASIC", jobRecords$Tool),]
   
   # select relevant columns from df, drop redundant dataset column (select(-2))
   masicList <- get_results_for_multiple_jobs.dt(jobRecords)
   masicData <- masicList[["_ReporterIons.txt"]] %>% 
      select(Dataset, 
             ScanNumber, 
             starts_with("Ion"), 
             -contains("Resolution"))
   
   if (idScanPattern != quantScanPattern) {
      quantIDTable <- make_id_to_quant_scan_link_table(masicList[["_ScanStatsEx.txt"]],
                                                       idScanPattern = idScanPattern,
                                                       quantScanPattern = quantScanPattern) %>%
         dplyr::rename(any_of(c("ScanNumber" = "QuantScan")))
      masicData <- inner_join(quantIDTable, masicData, 
                              by = c("Dataset", "ScanNumber")) %>%    
         select(-ScanNumber) %>%    
         dplyr::rename(ScanNumber = IDScan)
   }
   
   if (interference_score) {
      masicStats <- masicList[["_SICstats.txt"]] %>% 
         # select(-2) %>%
         dplyr::rename(any_of(c("ScanNumber" = "FragScanNumber"))) %>% 
         select(Dataset, ScanNumber, contains("InterferenceScore"))
      masicData <- inner_join(masicData, masicStats, 
                              by = c("Dataset", "ScanNumber"))
   }
   return(masicData)
}



