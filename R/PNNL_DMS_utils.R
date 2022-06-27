#' Utilities for interacting with PNNL DMS
#'
#' @description
#' Fetches results of different data analysis tools.
#' Not useful outside of PNNL unless connected through VPN.
#' Works on windows out of the box. To make it work on Mac/Linux FreeTDS needs
#' to be installed. Mac/Linux functionality is not well tested.
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
#' @md
#'
#'
#' @param data_package_num (integer) data package ID number.
#' @param jobs (integer) DMS job ID
#' @param mostRecent (logical) only most recent or all output files
#' @param jobNumber placeholder
#' @param toolName placeholder
#' @param fileNamePttrn placeholder
#' @param instrumentPttrn placeholder
#' @param jobRecords placeholder
#' @param pathToFile placeholder
#' @param datasetPttrn placeholder
#' @param experimentPttrn placeholder
#' @param toolPttrn placeholder
#' @param parPttrn  placeholder
#' @param settingsPttrn placeholder
#' @param fastaPttrn placeholder
#' @param proteinOptionsPttrn placeholder
#' @param organism_db (character) FASTA file. This is the same as the 
#'        \code{Organism DB} column. No need to specify this if there is only 
#'        one FASTA file associated with the jobs.
#' @param dir placeholder
#' @param file_name_segment placeholder
#'
#' @importFrom odbc odbc dbConnect dbSendQuery dbFetch dbClearResult dbDisconnect
#' @importFrom plyr ldply
#' @importFrom dplyr %>% rename filter
#' @importFrom readr read_tsv
#' @importFrom data.table data.table rbindlist
#' @importFrom utils read.delim tail download.file
#' @importFrom stringr str_match_all str_match str_replace_all str_replace
#' @importFrom curl curl_download
#'
#'
#' @name pnnl_dms_utils
#'
#' @examples
#' get_output_folder_for_job_and_tool(863951, "DTA_Refinery")

# dictionary that defines the suffix of the files given the analysis tool
tool2suffix <- list("MSGFPlus" = "_msgfplus_syn.txt",
                    "MSGFPlus_MzML" = "_msgfplus_syn.txt",
                    "MSGFPlus_MzML_NoRefine" = "_msgfplus_syn.txt",
                    "MSGFPlus_DTARefinery" = "_msgfplus_syn.txt",
                    "MSGFDB_DTARefinery"="_msgfdb_syn.txt",
                    "MASIC_Finnigan" = c("_ReporterIons.txt",
                                         "_ScanStatsEx.txt",
                                         "_SICstats.txt"),
                    "TopPIC" = c("_TopPIC_PrSMs.txt",
                                 "_ms1.feature"))

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


#' @export
#' @rdname pnnl_dms_utils
is_PNNL_DMS_connection_successful <- function()
{
  con_str <- sprintf("DRIVER={%s};SERVER=gigasax;DATABASE=dms5;%s",
                     get_driver(),
                     get_auth())
  
  con_test_res <- try(con <- dbConnect(odbc(), .connection_string=con_str),
                      TRUE)
  if(inherits(con_test_res, "try-error")){
    # no connection
    return(FALSE)
  }else{
    # connection present
    dbDisconnect(con)
    return(TRUE)
  }
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
  con_str <- sprintf("DRIVER={%s};SERVER=gigasax;DATABASE=dms5;%s",
                     get_driver(),
                     get_auth())
  con <- dbConnect(odbc(), .connection_string=con_str)
  
  # set-up query based on job list
  if(!is.null(jobs)){
    strSQL = sprintf("SELECT *
                       FROM V_Mage_Analysis_Jobs
                       WHERE [Job] IN ('%s')
                       ",
                     paste(jobs,sep="",collapse="',\n'"))
  }else{
    strSQL = sprintf("SELECT *
                       FROM V_Mage_Analysis_Jobs
                       WHERE [Dataset] LIKE '%%%s%%'
                       AND [Experiment] LIKE '%%%s%%'
                       AND [Tool] LIKE '%%%s%%'
                       AND [Parameter_File] LIKE '%%%s%%'
                       AND [Settings_File] LIKE '%%%s%%'
                       AND [Protein Collection List] LIKE '%%%s%%'
                       AND [Protein Options] LIKE '%%%s%%'
                       AND [Instrument] LIKE '%%%s%%'
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
  datasetFolder <- dirname( as.character(jobRecord$Folder))
  
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
  con_str <- sprintf("DRIVER={%s};SERVER=gigasax;DATABASE=DMS_Pipeline;%s",
                     get_driver(),
                     get_auth())
  con <- dbConnect(odbc(), .connection_string=con_str)
  strSQLPattern = "SELECT Output_Folder
   FROM V_Job_Steps_History
   WHERE (Job = %s) AND (Tool = '%s') AND (Most_Recent_Entry = 1)"
  strSQL <- sprintf( strSQLPattern, jobNumber, toolName)
  qry <- dbSendQuery(con, strSQL)
  res <- dbFetch(qry)
  dbClearResult(qry)
  dbDisconnect(con)
  return(as.character(res[1,1]))
}


# #' @export
# #' @rdname pnnl_dms_utils
# # Get AScore results for a given data package
# get_AScore_results <- function(data_package_num)
# {
#    #
#    con_str <- sprintf("DRIVER={%s};SERVER=gigasax;DATABASE=dms5;%s",
#                       get_driver(),
#                       get_auth())
#    con <- dbConnect(odbc(), .connection_string=con_str)
#    strSQL <- sprintf("SELECT *
#                      FROM V_Mage_Analysis_Jobs
#                      WHERE (Dataset LIKE 'DataPackage_%s%%')", data_package_num)
#    qry <- dbSendQuery(con, strSQL)
#    jobs <- dbFetch(qry)
#    dbClearResult(qry)
#    dbDisconnect(con)
#    #
#    if(nrow(jobs) == 1){
#       # library("RSQLite")
#       dlist <- basename(as.character(jobs["Folder"]))
#       idx <- which.max(as.numeric(sub("Step_(\\d+)_.*", "\\1", dlist))) # the Results supposed to be in the last folder
#       ascoreResultDB <- file.path( jobs["Folder"], dlist[idx], "Results.db3")
#       db <- dbConnect(SQLite(), dbname = ascoreResultDB)
#       AScores <- dbGetQuery(db, "SELECT * FROM t_results_ascore")
#       dbDisconnect(db)
#       return(AScores)
#    }else{
#       return(NULL)
#    }
# }

# #' @export
# #' @rdname pnnl_dms_utils
# # Get AScore results for a given data package (e.g. 3432)
# get_AScore_results_2 <- function(data_package_num){
#   # Prevent "no visible binding for global variable" note
#   Dataset <- NULL
#   
#   con_str <- sprintf("DRIVER={%s};SERVER=gigasax;DATABASE=dms5;%s",
#                      get_driver(),
#                      get_auth())
#   con <- dbConnect(odbc(), .connection_string=con_str)
#   strSQL <- sprintf("SELECT *
#                      FROM V_Mage_Analysis_Jobs
#                      WHERE (Dataset LIKE 'DataPackage_%s%%')", data_package_num)
#   qry <- dbSendQuery(con, strSQL)
#   job <- dbFetch(qry)
#   job <- dplyr::filter(job, Tool == "Phospho_FDR_Aggregator")
#   dbClearResult(qry)
#   dbDisconnect(con)
#   
#   if(nrow(job) > 1){
#     warning("Multiple Ascore jobs detected. Selecting the last one.")
#     job <- tail(job, 1)
#   }
#   
#   ascores_url <- get_url_from_dir_and_file(job['Folder'],"Concatenated_msgfplus_syn_ascore.txt")
#   job_to_dataset_url <- get_url_from_dir_and_file(job['Folder'],"Job_to_Dataset_Map.txt")
#   
#   ascores <- read_tsv(ascores_url)
#   job_to_dataset_map <- read_tsv(job_to_dataset_url)
#   
#   res <- inner_join(ascores, job_to_dataset_map) %>%
#     rename(spectrumFile = Dataset)
#   
#   return(res)
# }

# RODBC version

#' @export
#' @rdname pnnl_dms_utils
# Get AScore results for a given data package (e.g. 3432)
get_AScore_results <- function(data_package_num){
  # if(grepl("Darwin", Sys.info()["sysname"], ignore.case = T)){
  #   stop("The function `get_AScore_results` will likely crash your Mac!
  #        (It is not compatible with macOS)
  #        Please use `get_AScore_results_2` instead.
  #        Both functions have the same interface.")
  # }
  
  # Prevent "no visible binding for global variable" note
  Dataset <- NULL
  
  con_str <- sprintf("DRIVER={%s};SERVER=gigasax;DATABASE=dms5;%s",
                     get_driver(),
                     get_auth())
  con <- dbConnect(odbc(), .connection_string=con_str)
  strSQL <- sprintf("SELECT *
                     FROM V_Mage_Analysis_Jobs
                     WHERE (Dataset LIKE 'DataPackage_%s%%')", data_package_num)
  qry <- dbSendQuery(con, strSQL)
  job <- dbFetch(qry)
  job <- dplyr::filter(job, Tool == "Phospho_FDR_Aggregator")
  dbClearResult(qry)
  dbDisconnect(con)
  
  if(nrow(job) > 1){
    warning("Multiple Ascore jobs detected. Selecting the last one.")
    job <- tail(job, 1)
  }
  
  # in case Mac OS
  if(.Platform$OS.type == "unix"){
    # local_folder <- "~/temp_AScoreResults"
    # if(file.exists(local_folder)){
    #   unlink(local_folder, recursive = TRUE)
    # }
    # dir.create(local_folder)
    # remote_folder <- gsub("\\\\","/",job['Folder'])
    # mount_cmd <- sprintf("mount -t smbfs %s %s", remote_folder, local_folder)
    # system(mount_cmd)
    # # read the stuff
    # ascores <- read_tsv(
    #   file.path(local_folder,"Concatenated_msgfplus_syn_ascore.txt"))
    # job_to_dataset_map <- read_tsv(
    #   file.path(local_folder,"Job_to_Dataset_Map.txt"))
    # # end of read the stuff
    # umount_cmd <- sprintf("umount %s", local_folder)
    # system(umount_cmd)
    # unlink(local_folder, recursive = TRUE)
    
    ascores_url <- get_url_from_dir_and_file(job['Folder'],"Concatenated_msgfplus_syn_ascore.txt")
    job_to_dataset_url <- get_url_from_dir_and_file(job['Folder'],"Job_to_Dataset_Map.txt")
    
  } else if (.Platform$OS.type == "windows") {
    # in case Windows
    ascores_url <- file.path(job['Folder'],
                             "Concatenated_msgfplus_syn_ascore.txt")
    job_to_dataset_url <- file.path(job['Folder'],
                                    "Job_to_Dataset_Map.txt")
  } else {
    stop("unknown OS")
  }
  
  ascores <- read_tsv(ascores_url)
  job_to_dataset_map <- read_tsv(job_to_dataset_url)
  
  res <- inner_join(ascores, job_to_dataset_map) %>%
    dplyr::rename(spectrumFile = Dataset)
  
  return(res)
}

# RODBC version
# #' @export
# #' @rdname pnnl_dms_utils
# get_job_records_by_dataset_package <- function(data_package_num)
# {
#    con_str <- sprintf("DRIVER={%s};SERVER=gigasax;DATABASE=dms5;%s",
#                       get_driver(),
#                       get_auth())
#    con <- odbcDriverConnect(con_str)
#    strSQL = sprintf("
#                     SELECT *
#                     FROM V_Mage_Data_Package_Analysis_Jobs
#                     WHERE Data_Package_ID = %s",
#                     data_package_num)
#    jr <- sqlQuery(con, strSQL, stringsAsFactors=FALSE)
#    close(con)
#    return(jr)
# }


# odbc/DBI verson
#' @export
#' @rdname pnnl_dms_utils
get_job_records_by_dataset_package <- function(data_package_num)
{
  con_str <- sprintf("DRIVER={%s};SERVER=gigasax;DATABASE=dms5;%s",
                     get_driver(),
                     get_auth())
  con <- dbConnect(odbc(), .connection_string=con_str)
  strSQL <- sprintf("
                     SELECT *
                     FROM V_Mage_Data_Package_Analysis_Jobs
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
get_results_for_multiple_jobs <- function(jobRecords){
  if(grepl("Darwin", Sys.info()["sysname"], ignore.case = T)){
    stop("The function `get_results_for_multiple_jobs` will likely crash your Mac!
         (It is not compatible with macOS)")
  }
  
  toolName <- unique(jobRecords[["Tool"]])
  if (length(toolName) > 1){
    stop("Contains results of more then one tool.")
  }
  results = ldply(jobRecords[["Folder"]],
                  get_results_for_single_job,
                  fileNamePttrn=tool2suffix[[toolName]],
                  .progress = "text")
  return( results )
}


#' @export
#' @rdname pnnl_dms_utils
get_results_for_multiple_jobs.dt <- function(jobRecords){
  toolName = unique(jobRecords[["Tool"]])
  if (length(toolName) > 1) {
    stop("Contains results of more than one tool.")
  }
  result <- list()
  for (fileNamePttrn in tool2suffix[[toolName]]){
    result[[fileNamePttrn]] <- llply(jobRecords[["Folder"]], get_results_for_single_job.dt, 
                                     fileNamePttrn = fileNamePttrn, .progress = "text") %>% 
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
  
  complete_url <- paste(dir_url, "/", file_name, sep = "")
  return(complete_url)
}


#' @export
#' @rdname pnnl_dms_utils
get_results_for_single_job.dt <- function(pathToFile, fileNamePttrn)
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
  
  if(length(pathToFile) == 0){
    stop("can't find the results file")
  }
  if(length(pathToFile) > 1){
    stop("ambiguous results files")
  }
  
  results <- read_tsv(pathToFile, col_types=readr::cols(), progress=FALSE)
  
  dataset <- strsplit(basename(pathToFile), split=fileNamePttrn)[[1]]
  out <- data.table(Dataset=dataset, results)
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
  jobRecords <- jobRecords[grepl("MSGFPlus", jobRecords$Tool),]
  # if(length(unique(jobRecords$`Organism DB`)) != 1){
  #    stop("There should be exactly one FASTA file per data package!")
  # }
  
  # All FASTA files
  fasta_files <- unique(jobRecords$`Organism DB`)
  # If organism_db is not provided, check that there are not multiple
  # FASTA files.
  if (is.null(organism_db)) {
    if (length(fasta_files) != 1) {
      stop(paste0("There are multiple FASTA files. Please specify ",
                  "which one to return with organism_db:\n", 
                  paste(fasta_files, collapse = "\n")))
    } else {
      organism_db <- fasta_files
    }
  }
  # Filter to specific FASTA file
  jobRecords <- jobRecords[jobRecords$`Organism DB` == organism_db, ]
  
  strSQL <- sprintf("Select [Organism DB],
                             [Organism DB Storage Path]
                     From V_Analysis_Job_Detail_Report_2
                     Where Job = %s", jobRecords$Job[1])
  
  con_str <- sprintf("DRIVER={%s};SERVER=gigasax;DATABASE=dms5;%s",
                     get_driver(),
                     get_auth())
  
  con <- dbConnect(odbc(), .connection_string=con_str)
  qry <- dbSendQuery(con, strSQL)
  res <- dbFetch(qry)
  dbClearResult(qry)
  dbDisconnect(con)
  
  temp_dir <- tempdir()
  
  # OS-specific download
  if (.Platform$OS.type == "unix") {
    url <- get_url_from_dir_and_file(res['Organism DB Storage Path'],
                                     res['Organism DB'])
    temp_filepath <- paste(tempfile(), ".fasta", sep = "")
    message("Pre-downloading FASTA file to local tempdir (and returning local path)")
    curl_download(url, destfile = temp_filepath, quiet = F)
    path_to_FASTA <- url
    
  } else if (.Platform$OS.type == "windows") {
    path_to_FASTA <- file.path(res['Organism DB Storage Path'],
                               res['Organism DB'])
    file.copy(path_to_FASTA, temp_dir)
    path_to_FASTA <- file.path(temp_dir, res['Organism DB'])
    
  } else {
    stop("unknown OS")
  }
  
  return(path_to_FASTA)
}


# Prevent "no visible binding for global variable" note.
utils::globalVariables(c("Tool"))


