#' Utilities for interacting with PNNL DMS
#'
#' @description Fetches results of different data analysis tools. Not useful
#'   outside of PNNL unless connected through VPN. Works on windows out of the
#'   box. To make it work on Mac/Linux FreeTDS needs to be installed. Mac/Linux
#'   functionality is not well tested.
#'
#' @description
#' * `get_dms_job_records()`: returns job records given variety keyword patterns
#' * `get_tool_output_files_for_job_number()`: returns the output of the tool given tool name and file pattern
#' * `get_output_folder_for_job_and_tool()`: returns the path given job id and tool name
#' * `get_AScore_results()`: returns Ascore results given data package number
#' * `get_job_records_by_dataset_package()`: returns job records given data package number
#' * `get_results_for_multiple_jobs()`: returns concatenated results given job numbers
#' * `get_results_for_multiple_jobs.dt()`: returns results as concatenated data.table given job numbers
#' * `get_results_for_single_job()`: returns results given job number
#' * `get_results_for_single_job.dt()`: returns results as data.table given job number
#' * `get_datasets_by_data_package()`: returns table with datasets info
#' * `download_datasets_by_data_package()`: downloads datasets into provided location
#' @md
#'
#'
#' @param data_package_num (integer) data package ID number.
#' @param jobs (integer) DMS job ID
#' @param mostRecent (logical) only most recent or all output files
#' @param jobNumber 
#' @param toolName 
#' @param fileNamePttrn 
#' @param instrumentPttrn 
#' @param jobRecords 
#' @param pathToFile 
#' @param datasetPttrn 
#' @param experimentPttrn 
#' @param toolPttrn 
#' @param parPttrn  
#' @param settingsPttrn 
#' @param fastaPttrn 
#' @param proteinOptionsPttrn 
#' @param organism_db (character) FASTA file. This is the same as the
#'   \code{Organism DB} column. No need to specify this if there is only one
#'   FASTA file associated with the jobs.
#' @param dir 
#' @param file_name_segment 
#' @param copy_to folder path to copy files into
#' @param expected_multiple_files do we expect multiple files for a single job
#'   or not. Default is FALSE.
#' @param ncores number of cores to use in cluster
#'
#' @importFrom DBI dbConnect dbSendQuery dbFetch dbClearResult dbDisconnect
#' @importFrom RPostgres Postgres
#' @importFrom plyr ldply
#' @importFrom dplyr %>% rename filter select any_of mutate
#' @importFrom tidyr unnest
#' @importFrom tibble enframe
#' @importFrom readr read_tsv
#' @importFrom data.table data.table rbindlist
#' @importFrom utils read.delim tail download.file
#' @importFrom stringr str_match_all str_match str_replace_all str_replace
#' @importFrom curl curl_download
#' @importFrom RCurl url.exists
#' @importFrom stats setNames
#' @importFrom pbapply pbwalk
#' @importFrom parallel makeCluster stopCluster
#'
#' @name pnnl_dms_utils
#'
#' @examples
#' if (is_PNNL_DMS_connection_successful()) {
#'   get_output_folder_for_job_and_tool(863951, "DTA_Refinery")
#' }

# dictionary that defines the suffix of the files given the analysis tool
tool2suffix <- list("MSGFPlus"               = "_msgfplus_syn.txt",
                    "MSGFPlus_MzML"          = "_msgfplus_syn.txt",
                    "MSGFPlus_MzML_NoRefine" = "_msgfplus_syn.txt",
                    "MSGFPlus_DTARefinery"   = "_msgfplus_syn.txt",
                    "MSGFDB_DTARefinery"     = "_msgfdb_syn.txt",
                    "MASIC_Finnigan"         = c("_ReporterIons.txt",
                                                 "_ScanStatsEx.txt",
                                                 "_SICstats.txt"),
                    "TopPIC"                 = c("_TopPIC_PrSMs.txt",
                                                 "_ms1.feature"),
                    "Decon2LS_V2"            = "_isos.csv")

# link_min <- function(scan2,scan3){
#    if (scan3 > scan2){
#       out <- scan3
#    }
#    else {
#       out <- Inf
#    }
# }
# 
# link_min_vectorized <- Vectorize(link_min)

get_driver <- function(){
   if(.Platform$OS.type == "unix"){
      return("FreeTDS")
   }else if(.Platform$OS.type == "windows"){
      return("SQL Server")
   }else{
      stop("Unknown OS type.")
   }
}

get_auth <- function(){
   if(.Platform$OS.type == "unix"){
      return("PORT=1433;UID=dmsreader;PWD=dms4fun;")
   }else if(.Platform$OS.type == "windows"){
      return("")
   }else{
      stop("Unknown OS type.")
   }
}

#' @importFrom RPostgres Postgres
get_db_connection <- function(
    driver = Postgres,
    driver_args = list(),
    host = "prismdb2.emsl.pnl.gov",
    user = "dmsreader",
    password = "dms4fun",
    dbname = "dms",
    ...) {
   driver_object <- do.call(driver, driver_args)
   con <- dbConnect(driver_object,
      host = host,
      user = user,
      password = password,
      dbname = dbname,
      ...
   )
   return(con)
}

#' @export
#' @rdname pnnl_dms_utils
is_PNNL_DMS_connection_successful <- function()
{
   con_test_res <- try(con <- get_db_connection(), TRUE)
   if(inherits(con_test_res, "try-error")){
      # no connection
      return(FALSE)
   }else{
      # connection present
      dbDisconnect(con)
      return(TRUE)
   }
}

.new_tempdir <- function() {
   tf <- tempfile()
   dir.create(tf)
   return(tf)
}


#' @export
#' @rdname pnnl_dms_utils
get_dms_job_records <- function(
      jobs = NULL,
      datasetPttrn = "",
      experimentPttrn = "",
      toolPttrn = "",
      parPttrn = "",
      settingsPttrn = "",
      fastaPttrn = "",
      proteinOptionsPttrn = "",
      instrumentPttrn = ""){
   
   # first check if the input is valid
   x = as.list(environment())
   x[["jobs"]] = NULL
   if( all(x == "") & is.null(jobs) ){
      stop("insufficients arguments provided")
   }
   if( any(x != "") & !is.null(jobs) ){
      stop("can't provide both: job list and search terms")
   }
   
   # initialize connection
   con <- get_db_connection()
   
   # set-up query based on job list
   if(!is.null(jobs)){
      strSQL = sprintf("SELECT *
                       FROM v_mage_analysis_jobs
                       WHERE job IN ('%s')
                       ",
                       paste(jobs,sep="",collapse="',\n'"))
   }else{
      strSQL = sprintf("SELECT *
                       FROM v_mage_analysis_jobs
                       WHERE dataset LIKE '%%%s%%'
                       AND experiment LIKE '%%%s%%'
                       AND tool LIKE '%%%s%%'
                       AND parameter_file LIKE '%%%s%%'
                       AND settings_file LIKE '%%%s%%'
                       AND protein collection list LIKE '%%%s%%'
                       AND protein options LIKE '%%%s%%'
                       AND instrument LIKE '%%%s%%'
                       ",
                       datasetPttrn,
                       experimentPttrn,
                       toolPttrn,
                       parPttrn,
                       settingsPttrn,
                       fastaPttrn,
                       proteinOptionsPttrn,
                       instrumentPttrn)
   }
   qry <- dbSendQuery(con, strSQL)
   locationPointersToMSMSjobs <- dbFetch(qry)
   dbClearResult(qry)
   dbDisconnect(con)
   return(locationPointersToMSMSjobs)
}


#' @export
#' @rdname pnnl_dms_utils
get_tool_output_files_for_job_number <- function(jobNumber, toolName = NULL,
                                                 fileNamePttrn, mostRecent=TRUE)
{
   # get job records first. This will be useful to get dataset folder
   jobRecord <- get_dms_job_records(jobNumber)
   datasetFolder <- dirname(as.character(jobRecord$folder))
   
   # get tool's subfolder
   if( is.null(toolName) ){
      toolFolder = ''
   }else{
      # return stuff from the main dataset folder
      toolFolder = get_output_folder_for_job_and_tool(jobNumber, toolName, mostRecent)
   }
   #
   candidateFiles = list.files(file.path(datasetFolder, toolFolder),
                               pattern=fileNamePttrn, full.names=TRUE,
                               ignore.case=TRUE)
   #
   if(length(candidateFiles) == 1){
      return(candidateFiles)
   }else{
      return(NA)
   }
}


#' @export
#' @rdname pnnl_dms_utils
get_output_folder_for_job_and_tool <- function(jobNumber, toolName, mostRecent=TRUE)
{
   con <- get_db_connection()
   strSQLPattern = "SELECT output_folder
   FROM sw.v_job_steps_history
   WHERE (job = %s) AND (tool = '%s') AND (most_recent_entry = 1)"
   strSQL <- sprintf(strSQLPattern, jobNumber, toolName)
   qry <- dbSendQuery(con, strSQL)
   res <- dbFetch(qry)
   dbClearResult(qry)
   dbDisconnect(con)
   return(as.character(res[1,1]))
}





# odbc/DBI verson
#' @export
#' @rdname pnnl_dms_utils
get_job_records_by_dataset_package <- function(data_package_num)
{
   con <- get_db_connection()
   strSQL <- sprintf("
                     SELECT *
                     FROM v_mage_data_package_analysis_jobs
                     WHERE Data_Package_ID = %s",
                     data_package_num)
   qry <- dbSendQuery(con, strSQL)
   jr <- dbFetch(qry)
   dbClearResult(qry)
   dbDisconnect(con)
   return(jr)
}



#' @export
#' @rdname pnnl_dms_utils
get_datasets_by_data_package <- function(data_package_num)
{
   con <- get_db_connection()
   strSQL <- sprintf("
                     SELECT *
                     FROM v_mage_data_package_datasets
                     WHERE Data_Package_ID = %s",
                     data_package_num)
   qry <- dbSendQuery(con, strSQL)
   jr <- dbFetch(qry)
   dbClearResult(qry)
   dbDisconnect(con)
   return(jr)
}


#' @export
#' @rdname pnnl_dms_utils
download_datasets_by_data_package <- function(data_package_num, 
                                              copy_to = ".", 
                                              fileNamePttrn = ".raw",
                                              ncores = 2)
{
   dataset_tbl <- get_datasets_by_data_package(data_package_num) %>% 
      dplyr::select(folder)
   if (.Platform$OS.type == "unix") {
      pathToFile <- dataset_tbl %>% 
         dplyr::mutate(folder = get_url_from_dir_and_file(folder, fileNamePttrn))
   } else if (.Platform$OS.type == "windows") {
      pathToFile <- dataset_tbl %>% 
         dplyr::mutate(folder = list.files(path=folder,
                                           pattern=fileNamePttrn,
                                           full.names=TRUE))
   } else {
      stop("Unknown OS type.")
   }
   multiproc_cl <- makeCluster(ncores)
   on.exit(stopCluster(multiproc_cl))
   pbwalk(X = pathToFile$folder, FUN = file.copy, cl = multiproc_cl, to = copy_to)
}


#' @export
#' @rdname pnnl_dms_utils
get_results_for_multiple_jobs <- function(jobRecords){
   if(grepl("Darwin", Sys.info()["sysname"], ignore.case = T)){
      stop("The function `get_results_for_multiple_jobs` will likely crash your Mac!
         (It is not compatible with macOS)")
   }
   
   toolName <- unique(jobRecords[["tool"]])
   if (length(toolName) > 1){
      stop("Contains results of more then one tool.")
   }
   results = ldply(jobRecords[["folder"]],
                   get_results_for_single_job,
                   fileNamePttrn=tool2suffix[[toolName]],
                   .progress = "text")
   return( results )
}


#' @export
#' @rdname pnnl_dms_utils
get_results_for_multiple_jobs.dt <- function(jobRecords, expected_multiple_files = FALSE){
   toolName = unique(jobRecords[["tool"]])
   if (length(toolName) > 1) {
      stop("Contains results of more than one tool.")
   }
   result <- list()
   for (fileNamePttrn in tool2suffix[[toolName]]){
      result[[fileNamePttrn]] <- llply(jobRecords[["folder"]], 
                                       get_results_for_single_job.dt, 
                                       fileNamePttrn = fileNamePttrn, 
                                       expected_multiple_files = expected_multiple_files, 
                                       .progress = "text") %>% 
         rbindlist(fill = TRUE)    # fill = TRUE to handle differing numbers of columns in ScanStatsEx
   }
   return(result)
}


#' @export
#' @rdname pnnl_dms_utils
get_results_for_single_job <- function(pathToFile, fileNamePttrn){
   pathToFile = list.files( path=as.character(pathToFile),
                            pattern=fileNamePttrn,
                            full.names=TRUE)
   if(grepl("Darwin", Sys.info()["sysname"], ignore.case = T)){
      stop("The function `get_results_for_single_job` will likely crash your Mac!
         (It is not compatible with macOS)")
   }
   
   
   if(length(pathToFile) == 0){
      stop("can't find the results file")
   }
   if(length(pathToFile) > 1){
      stop("ambiguous results files")
   }
   results <- read.delim( pathToFile, header=TRUE, stringsAsFactors = FALSE)
   dataset <- strsplit( basename(pathToFile), split=fileNamePttrn)[[1]]
   out <- data.frame(Dataset=dataset, results, stringsAsFactors = FALSE)
   return(out)
}


#' @export
#' @rdname pnnl_dms_utils
get_url_from_dir_and_file <- function(dir, file_name_segment) {
   dir <- as.character(dir)
   if (.Platform$OS.type == "unix") { # Exchange two backslashes for a forward slash in unix environment
      dir <- gsub("\\\\", "/", dir)
   }
   else if (.Platform$OS.type != "windows") {
      stop("Unknown OS type.")
   }
   dir_url <- sub("\\/\\/([^\\/]+)\\/", "https:\\/\\/\\1.pnl.gov/", dir)
   
   # Download directory listing; turn it into string
   temp_filepath <- paste(tempfile(), ".html", sep = "")
   download.file(paste(dir_url, "/", sep = ""), destfile = temp_filepath, quiet = TRUE) 
   dir_listing_vec <- scan(temp_filepath, what = "character", quiet = TRUE)
   dir_listing_str <- paste(dir_listing_vec, sep = "", collapse = "")
   unlink(temp_filepath)
   
   # Build up full URL
   file_name_pattern_escaped <- str_replace_all(file_name_segment, "(\\W)", "\\\\\\1")
   file_name_regex <- paste(">([^\\/]*", file_name_pattern_escaped, "[^\\/]*)<", sep = "")
   file_name <- str_match(dir_listing_str, file_name_regex)[, -1]
   if(is.na(file_name)){
      first_part_of_filnme <- sub(".*\\/([^\\/]+)\\/[^\\/]*$", "\\1", dir_url)
      file_name <- paste(first_part_of_filnme, file_name_segment, sep = "")
   }
   
   complete_url <- paste(dir_url, "/", file_name, sep = "")
   if (!RCurl::url.exists(complete_url)){
      stop("Requested URL does not exist.")
   }
   return(complete_url)
}


#' @export
#' @rdname pnnl_dms_utils
get_results_for_single_job.dt <- function(pathToFile, fileNamePttrn, expected_multiple_files = FALSE)
{
   pathToFile <- as.character(pathToFile)
   
   if (.Platform$OS.type == "unix") {
      pathToFile <- get_url_from_dir_and_file(pathToFile, fileNamePttrn)
   } else if (.Platform$OS.type == "windows") {
      local_folder <- pathToFile
      pathToFile <- list.files(path=local_folder,
                               pattern=fileNamePttrn,
                               full.names=TRUE)
   } else {
      stop("Unknown OS type.")
   }
   
   # if(length(pathToFile) == 0){
   #   stop("can't find the results file")
   # }
   # if(length(pathToFile) > 1){
   #   stop("ambiguous results files")
   # }
   # 
   # results <- read_tsv(pathToFile, col_types=readr::cols(), progress=FALSE)
   # 
   # dataset <- strsplit(basename(pathToFile), split=fileNamePttrn)[[1]]
   # out <- data.table(Dataset=dataset, results)
   # return(out)
   
   # checks all the file number problems
   if (length(pathToFile) == 0) {
      stop("can't find the results file")
   }
   if (length(pathToFile) > 1 & !expected_multiple_files) {
      stop("ambiguous results files")
   }
   
   short_dataset_names <- unlist(strsplit(basename(pathToFile), 
                                          split = fileNamePttrn))
   
   read_fun <- read_tsv
   if(grepl(".csv$", pathToFile))
      read_fun <- read_csv
   
   out <- llply(pathToFile, 
                read_fun,
                col_types = readr::cols(),
                guess_max = Inf,
                progress = FALSE) %>%
      #lapply(function(xi) { dplyr::select(xi, -one_of("Dataset"))}) %>%
      # map(dplyr::select, -any_of("Dataset")) %>%
      lapply(function(xi) dplyr::select(xi, -any_of("Dataset"))) %>% 
      setNames(short_dataset_names) %>%
      enframe(name = "Dataset") %>% 
      unnest(value) %>%
      data.table()
   return(out)
}



#' @export
#' @rdname pnnl_dms_utils
# Returns path to FASTA. Note FASTA will be in temp directory.
path_to_FASTA_used_by_DMS <- function(data_package_num, organism_db = NULL)
{
   # make sure it was the same fasta used for all msgf jobs
   # at this point this works only with one data package at a time
   jobRecords <- get_job_records_by_dataset_package(data_package_num)
   # jobRecords <- jobRecords[grepl("MSGFPlus", jobRecords$tool),]
   # if(length(unique(jobRecords$organism_db)) != 1){
   #    stop("There should be exactly one FASTA file per data package!")
   # }
   
   # All FASTA files
   fasta_files <- setdiff(unique(jobRecords$organism_db), "na")
   if (length(fasta_files) == 0)
      stop(paste0("There are no FASTA files in the package."))
   
   # If organism_db is not provided, check that there are not multiple
   # FASTA files.
   if (is.null(organism_db)) {
      if (length(fasta_files) > 1) {
         stop(paste0("There are multiple FASTA files. Please specify ",
                     "which one to return with organism_db:\n", 
                     paste(fasta_files, collapse = "\n")))
      }
      organism_db <- fasta_files
   }
   # Filter to specific FASTA file
   jobRecords <- jobRecords[jobRecords$organism_db == organism_db, ]
   
   strSQL <- sprintf("Select organism_db,
                            organism_db_storage_path
                     From v_analysis_job_detail_report_2
                     Where job = %s", jobRecords$job[1])
   
   
   con <- get_db_connection()
   qry <- dbSendQuery(con, strSQL)
   res <- dbFetch(qry)
   dbClearResult(qry)
   dbDisconnect(con)
   
   temp_dir <- .new_tempdir()
   
   # OS-specific download
   if (.Platform$OS.type == "unix") {
      url <- get_url_from_dir_and_file(res['organism_db_storage_path'],
                                       res['organism_db'])
      temp_filepath <- paste(tempfile(), ".fasta", sep = "")
      message("Pre-downloading FASTA file to local tempdir (and returning local path)")
      curl_download(url, destfile = temp_filepath, quiet = F)
      path_to_FASTA <- temp_filepath
      
   } else if (.Platform$OS.type == "windows") {
      path_to_FASTA <- file.path(res['organism_db_storage_path'],
                                 res['organism_db'])
      file.copy(path_to_FASTA, temp_dir)
      path_to_FASTA <- file.path(temp_dir, res['organism_db'])
      
   } else {
      stop("unknown OS")
   }
   
   return(path_to_FASTA)
}


# Prevent "no visible binding for global variable" note.
utils::globalVariables(c("tool", "value"))


