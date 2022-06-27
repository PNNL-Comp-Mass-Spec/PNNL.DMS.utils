#' Reading MASIC results from PNNL's DMS
#'
#' @param data_package_num (numeric or character vector) containing Data Package 
#'     ID(s) located in DMS.
#' @param interference_score (logical) read interference score. Default is 
#'     \code{FALSE}.
#' @param id_quant_table If the MS/MS identification scans and reporter ion 
#'     intensity scans are different for a particular dataset, set this to 
#'     \code{TRUE}. Default is \code{FALSE}.
#' 
#' @return (data.frame) with reporter ion intensities and other metrics
#' 
#' @importFrom dplyr select contains starts_with
#' @importFrom data.table rbindlist
#' 
#' @export read_masic_data_from_DMS


read_masic_data_from_DMS <- function(data_package_num, 
                                     interference_score=FALSE, 
                                     id_quant_table = FALSE)
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
    select(-2) %>% 
    select(Dataset, 
           ScanNumber, 
           starts_with("Ion"), 
           -contains("Resolution"))
  
  if (id_quant_table) {
    quantIDTable <- make_id_to_quant_scan_link_table(masicList[["_ScanStatsEx.txt"]][,-2]) %>%
      rename(ScanNumber = QuantScan)
    masicData <- inner_join(quantIDTable, masicData, 
                            by = c("Dataset", "ScanNumber")) %>%    
      select(-ScanNumber) %>%    
      rename(ScanNumber = IDScan)
  }
  
  if (interference_score) {
    masicStats <- masicList[["_SICstats.txt"]] %>% 
      select(-2) %>%
      select(Dataset, 
             ScanNumber = FragScanNumber, 
             contains("InterferenceScore"))
    masicData <- inner_join(masicData, masicStats, 
                            by = c("Dataset", "ScanNumber"))
  }
  return(masicData)
}



