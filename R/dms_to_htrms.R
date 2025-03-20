##+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
##  on-the-fly .htrms converter
##+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
##+## David Hall - david.hall@pnnl.gov
## 20250319 --- written with assistance from generative AI
##+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
## Description:
##    Collection of functions that serve to convert .raw files on DMS
##    to .htrms format for use with Biognosys Spectronaut

#' Get full paths to .raw files from a DMS data package
#'
#' @description Retrieves the complete file paths to all .raw files associated with a 
#'   specified DMS data package. This function uses get_datasets_by_data_package() to 
#'   fetch dataset information and then locates the .raw files in each dataset folder.
#'
#' @param data_package_num (integer) DMS data package ID number
#'
#' @return A character vector containing full paths to all .raw files in the data package
#'
#' @details
#' This function searches each dataset folder in the specified data package for files
#' with the .raw extension. It's primarily used as a helper function for 
#' convert_data_package_to_htrms(), but can also be used independently to get file paths.
#'
#' @importFrom dplyr %>%
#'
#' @examples
#' \dontrun{
#' # Get all .raw file paths from data package 6447
#' raw_files <- get_raw_files_from_data_package(6447)
#' }
#'
#' @export
get_raw_files_from_data_package <- function(data_package_num) {
   # Get all datasets in the package
   datasets <- get_datasets_by_data_package(data_package_num)
   
   # Initialize list to store file paths
   raw_files <- list()
   
   # For each dataset folder, find the .raw file
   for (i in seq_len(nrow(datasets))) {
      folder_path <- datasets$folder[i]
      files <- list.files(folder_path, pattern = "\\.raw$", full.names = TRUE)
      if (length(files) > 0) {
         raw_files <- c(raw_files, files)
      }
   }
   
   return(unlist(raw_files))
}

#' Write dataframe to log file in TSV format
#'
#' @description Helper function to write a dataframe to a log file in tab-separated format
#'
#' @param df (data.frame) The dataframe to write
#' @param file (character) Path to the log file
#'
#' @return No return value, called for side effect of writing to log file
#'
#' @keywords internal
write_tsv_to_log <- function(df, file) {
   header <- paste(names(df), collapse = "\t")
   rows <- apply(df, 1, function(row) paste(row, collapse = "\t"))
   cat(header, "\n", paste(rows, collapse = "\n"), "\n\n", file = file, append = TRUE)
}

#' Convert single .raw file to HTRMS format
#'
#' @description Helper function for convert_data_package_to_htrms that converts 
#'   a single .raw file to .htrms format using HTRMSConverter.exe
#'
#' @param raw_file_path (character) Full path to source .raw file
#' @param output_folder (character) Folder where converted .htrms file will be saved
#' @param converter_path (character) Full path to HTRMSConverter.exe
#' @param settings (character) Optional path or name of conversion settings schema
#' @param log_file (character) Path to log file for recording conversion details
#' @param timeout_seconds (numeric) Maximum time in seconds to wait for conversion
#'
#' @return A list containing:
#'   \item{file}{Path to the source .raw file}
#'   \item{output}{Path to the converted .htrms file}
#'   \item{input_size}{Size of the input file in bytes}
#'   \item{output_size}{Size of the output file in bytes (NA if file doesn't exist)}
#'   \item{size_ratio}{Ratio of output file size to input file size}
#'   \item{success}{Logical indicating if conversion was successful}
#'   \item{timed_out}{Logical indicating if conversion timed out}
#'   \item{error_message}{Character string with error message, or NA if no error}
#'
#' @details
#' This function is not typically called directly but is used by convert_data_package_to_htrms.
#' It handles the execution of the HTRMSConverter.exe command line tool for a single file.
#'
#' @keywords internal
convert_raw_to_htrms <- function(raw_file_path, output_folder, converter_path, 
                                 settings = NULL, log_file = NULL, timeout_seconds = 1800) {
   # Create output file path
   file_name <- basename(raw_file_path)
   output_file <- file.path(output_folder, gsub("\\.raw$", ".htrms", file_name))
   
   # Get input file size before conversion
   input_size <- file.size(raw_file_path)
   
   # Construct command with full path to converter - using original working format
   cmd <- paste0('"', converter_path, '" -i "', raw_file_path, '" -o "', output_file, '" -nogui')
   
   # Add settings if provided
   if (!is.null(settings)) {
      cmd <- paste0(cmd, ' -s "', settings, '"')
   }
   
   # Log the command
   if (!is.null(log_file)) {
      cat(format(Sys.time()), "Running:", cmd, "\n", file = log_file, append = TRUE)
   } else {
      cat("Running:", cmd, "\n")
   }
   
   # Generate a timeout command prefix if running on Windows
   if (.Platform$OS.type == "windows") {
      # Use timeout command on Windows
      timeout_cmd <- paste0("timeout /t ", timeout_seconds, " /nobreak > NUL & ")
      cmd_with_timeout <- paste0("start /b cmd /c \"", cmd, " & if errorlevel 1 (echo Conversion failed) else (echo Conversion completed)\"")
      full_cmd <- paste0(timeout_cmd, cmd_with_timeout)
      
      # Run the command with simple timeout
      tryCatch({
         # Run the command using system and wait for it to complete
         system_result <- system(cmd, timeout = timeout_seconds)
         result <- (system_result == 0)
         timed_out <- FALSE
      }, error = function(e) {
         if (grepl("timed out", e$message, ignore.case = TRUE)) {
            if (!is.null(log_file)) {
               cat(format(Sys.time()), "ERROR: Conversion timed out after", timeout_seconds, "seconds.\n", 
                   file = log_file, append = TRUE)
            } else {
               cat("ERROR: Conversion timed out after", timeout_seconds, "seconds.\n")
            }
            system(paste0("taskkill /F /IM ", basename(converter_path)), show.output.on.console = FALSE)
            result <<- FALSE
            timed_out <<- TRUE
         } else {
            if (!is.null(log_file)) {
               cat(format(Sys.time()), "Error:", e$message, "\n", file = log_file, append = TRUE)
            } else {
               cat("Error:", e$message, "\n")
            }
            result <<- FALSE
            timed_out <<- FALSE
         }
      })
   } else {
      # For non-Windows platforms, use a simpler approach
      result <- tryCatch({
         system(cmd, timeout = timeout_seconds)
         TRUE
      }, error = function(e) {
         if (grepl("timed out", e$message, ignore.case = TRUE)) {
            if (!is.null(log_file)) {
               cat(format(Sys.time()), "ERROR: Conversion timed out after", timeout_seconds, "seconds.\n", 
                   file = log_file, append = TRUE)
            } else {
               cat("ERROR: Conversion timed out after", timeout_seconds, "seconds.\n")
            }
            timed_out <<- TRUE
         } else {
            if (!is.null(log_file)) {
               cat(format(Sys.time()), "Error:", e$message, "\n", file = log_file, append = TRUE)
            } else {
               cat("Error:", e$message, "\n")
            }
            timed_out <<- FALSE
         }
         FALSE
      })
   }
   
   # Verify that the output file exists and has content after conversion
   if (file.exists(output_file)) {
      output_size <- file.size(output_file)
      if (output_size > 0) {
         # Check if we can read the file
         can_read <- tryCatch({
            con <- file(output_file, "rb")
            close(con)
            TRUE
         }, error = function(e) {
            FALSE
         })
         
         if (!can_read) {
            if (!is.null(log_file)) {
               cat(format(Sys.time()), "WARNING: Output file exists but cannot be read\n", 
                   file = log_file, append = TRUE)
            }
            result <- FALSE
            error_message <- "Output file exists but cannot be read"
         } else {
            error_message <- NA
         }
      } else {
         if (!is.null(log_file)) {
            cat(format(Sys.time()), "WARNING: Output file has zero size\n", 
                file = log_file, append = TRUE)
         }
         result <- FALSE
         output_size <- 0
         error_message <- "Output file has zero size"
      }
   } else {
      if (!is.null(log_file)) {
         cat(format(Sys.time()), "WARNING: Output file was not created\n", 
             file = log_file, append = TRUE)
      }
      result <- FALSE
      output_size <- NA
      error_message <- "Output file was not created"
   }
   
   return(list(
      file = raw_file_path,
      output = output_file,
      input_size = input_size,
      output_size = output_size,
      size_ratio = if(!is.na(output_size) && input_size > 0 && output_size > 0) output_size / input_size else NA,
      success = result,
      timed_out = if(exists("timed_out")) timed_out else FALSE,
      error_message = if(exists("error_message")) error_message else NA
   ))
}

#' Convert .raw mass spectrometry files to HTRMS format
#'
#' @description Converts .raw files from DMS to .htrms format for use with Biognosys Spectronaut.
#'   Files are processed sequentially with a delay between conversions for stability.
#'   Converted files are saved in a dedicated subfolder, and detailed logs and results are generated.
#'   Files that have already been converted are automatically skipped.
#'
#' @param data_package_num (integer) DMS data package ID number
#' @param output_folder (character) Local folder where conversion results will be saved
#' @param converter_path (character) Full path to HTRMSConverter.exe; default is path on KAIJU
#' @param settings (character) Optional path or name of conversion settings schema to use
#' @param delay_seconds (integer) Number of seconds to wait between file conversions (default: 5)
#' @param force_reconvert (logical) Whether to reconvert files that already exist (default: FALSE)
#' @param timeout_seconds (numeric) Maximum time in seconds to wait for a single conversion (default: 1800)
#'
#' @return A list containing:
#'   \item{raw_results}{A list with detailed results for each file conversion}
#'   \item{results_df}{A data frame summarizing all conversion results}
#'   \item{log_file}{Path to the generated log file}
#'   \item{csv_file}{Path to the saved CSV results file}
#'   \item{htrms_folder}{Path to the folder containing converted .htrms files}
#'   \item{elapsed_time}{Total time taken for the conversion process}
#'
#' @details
#' This function automates the process of converting Thermo .raw files stored on DMS to 
#' the Biognosys HTRMS format for analysis in Spectronaut. It uses the HTRMSConverter
#' command line tool and processes files sequentially for maximum stability.
#'
#' The function:
#' \itemize{
#'   \item Retrieves file paths from DMS based on the data package number
#'   \item Creates a dedicated output folder structure
#'   \item Converts files one by one with a delay between conversions
#'   \item Verifies that output files exist and have valid content
#'   \item Logs conversion details including timing and file sizes
#'   \item Identifies potential outliers in conversion size ratios
#'   \item Returns detailed results for further analysis
#' }
#'
#' Converted .htrms files are saved in a 'htrms' subfolder under the specified output folder.
#' Log files and CSV reports are saved in the main output folder with date-based filenames.
#'
#' @importFrom dplyr %>% filter summarize
#' @importFrom tibble tibble
#' @importFrom readr write_csv
#'
#' @examples
#' \dontrun{
#' # Path to the HTRMSConverter.exe
#' # This is the local path on KAIJU
#' converter_path <- "C:/Program Files (x86)/Biognosys/HTRMS Converter/HTRMSConverter.exe"
#' 
#' # Convert a data package
#' results <- convert_data_package_to_htrms(
#'    data_package_num = 6447, 
#'    output_folder = "E:/Data/Converted_Files",
#'    converter_path = converter_path,
#'    delay_seconds = 5
#' )
#' }
#'
#' @export
convert_data_package_to_htrms <- function(data_package_num, 
                                          output_folder,
                                          converter_path = "C:/Program Files (x86)/Biognosys/HTRMS Converter/HTRMSConverter.exe",
                                          settings = NULL, 
                                          delay_seconds = 5,
                                          force_reconvert = FALSE,
                                          timeout_seconds = 1800) {
   # First check if the converter exists
   if (!file.exists(converter_path)) {
      stop("HTRMSConverter.exe not found at path: ", converter_path, 
           ". Please provide the correct path to the converter executable.")
   }
   
   # Create date-based prefix for log and output files
   date_prefix <- format(Sys.time(), "%Y%m%d")
   log_file_name <- paste0(date_prefix, "_dataPackage", data_package_num, "_conversion.log")
   csv_file_name <- paste0(date_prefix, "_dataPackage", data_package_num, "_results.csv")
   
   # Create main output folder if it doesn't exist
   if (!dir.exists(output_folder)) {
      dir.create(output_folder, recursive = TRUE)
   }
   
   # Create htrms subfolder for converted files
   htrms_folder <- file.path(output_folder, "htrms")
   if (!dir.exists(htrms_folder)) {
      dir.create(htrms_folder, recursive = TRUE)
   }
   
   # Set paths for log and CSV files (in main output folder)
   log_file <- file.path(output_folder, log_file_name)
   csv_file <- file.path(output_folder, csv_file_name)
   
   # Initialize log file
   cat("HTRMS Conversion Log\n", 
       "Started:", format(Sys.time()), "\n",
       "Data Package:", data_package_num, "\n",
       "Output Folder:", output_folder, "\n",
       "HTRMS Files Folder:", htrms_folder, "\n",
       "Converter Path:", converter_path, "\n",
       "Settings:", ifelse(is.null(settings), "Default", settings), "\n",
       "Delay Between Files:", delay_seconds, "seconds\n",
       "Force Reconvert:", ifelse(force_reconvert, "Yes", "No"), "\n",
       "Timeout Seconds:", timeout_seconds, "\n\n",
       file = log_file)
   
   # Get all raw files
   raw_files <- get_raw_files_from_data_package(data_package_num)
   cat("Found", length(raw_files), "raw files to convert\n", file = log_file, append = TRUE)
   
   # Process files sequentially with a delay between each
   cat("Starting sequential conversion of", length(raw_files), "files...\n")
   start_time <- Sys.time()
   
   results <- vector("list", length(raw_files))
   
   for (i in seq_along(raw_files)) {
      file <- raw_files[i]
      file_name <- basename(file)
      output_file_name <- gsub("\\.raw$", ".htrms", file_name)
      output_file_path <- file.path(htrms_folder, output_file_name)
      
      # Check if this file has already been converted and whether to force reconversion
      if (file.exists(output_file_path) && !force_reconvert) {
         # Also verify the file has content
         output_size <- file.size(output_file_path)
         if (output_size > 0) {
            cat("File", i, "of", length(raw_files), ":", file_name, "already exists, skipping...\n")
            
            # Log file skip
            if (!is.null(log_file)) {
               cat(format(Sys.time()), "File", i, "of", length(raw_files), 
                   ":", file_name, "already exists, skipping.\n", file = log_file, append = TRUE)
            }
            
            # Create a result entry for the existing file
            input_size <- file.size(file)
            
            results[[i]] <- list(
               file = file,
               output = output_file_path,
               input_size = input_size,
               output_size = output_size,
               size_ratio = if(input_size > 0) output_size / input_size else NA,
               success = TRUE,
               skipped = TRUE,  # Add flag to indicate this was skipped
               timed_out = FALSE,
               error_message = NA
            )
            
            # Continue to the next file
            next
         }
      }
      
      cat("Processing file", i, "of", length(raw_files), ":", file_name, "\n")
      
      # Log file start
      if (!is.null(log_file)) {
         cat(format(Sys.time()), "Starting file", i, "of", length(raw_files), 
             ":", file_name, "\n", file = log_file, append = TRUE)
      }
      
      # Process this file with simple timeout
      results[[i]] <- convert_raw_to_htrms(
         file, 
         htrms_folder, 
         converter_path, 
         settings, 
         log_file,
         timeout_seconds
      )
      
      # Add a delay before starting the next file (if not the last one)
      if (i < length(raw_files)) {
         cat("Waiting", delay_seconds, "seconds before next conversion...\n")
         Sys.sleep(delay_seconds)
      }
   }
   
   end_time <- Sys.time()
   elapsed <- difftime(end_time, start_time, units = "mins")
   
   # After processing, create the results dataframe
   results_df <- tibble(
      input_file = sapply(results, function(x) x$file),
      output_file = sapply(results, function(x) x$output),
      input_size_MB = round(sapply(results, function(x) x$input_size) / (1024*1024), 2),
      output_size_MB = round(sapply(results, function(x) { if(!is.na(x$output_size)) x$output_size / (1024*1024) else NA }), 2),
      size_ratio = round(sapply(results, function(x) x$size_ratio), 3),
      success = sapply(results, function(x) x$success),
      skipped = sapply(results, function(x) ifelse(!is.null(x$skipped), x$skipped, FALSE)),
      timed_out = sapply(results, function(x) ifelse(!is.null(x$timed_out), x$timed_out, FALSE)),
      error_message = sapply(results, function(x) ifelse(!is.null(x$error_message), as.character(x$error_message), NA))
   )
   
   # Save results dataframe to CSV
   write_csv(results_df, csv_file)
   
   # Summarize results
   successes <- sum(results_df$success)
   failures <- nrow(results_df) - successes
   
   # Calculate size statistics (checking if we have any successful conversions first)
   if (successes > 0) {
      successful_df <- results_df %>% filter(success)
      skipped_count <- sum(results_df$skipped)
      timed_out_count <- sum(results_df$timed_out)
      new_conversions <- successes - skipped_count
      
      # Only calculate size statistics if we have output sizes
      if (any(!is.na(successful_df$output_size_MB))) {
         size_stats <- successful_df %>%
            filter(!is.na(output_size_MB)) %>%
            summarize(
               avg_input_size_MB = mean(input_size_MB),
               avg_output_size_MB = mean(output_size_MB, na.rm = TRUE),
               avg_size_ratio = mean(size_ratio, na.rm = TRUE),
               min_size_ratio = min(size_ratio, na.rm = TRUE),
               max_size_ratio = max(size_ratio, na.rm = TRUE)
            )
         
         # Write summary to log file
         cat("\nConversion Summary\n",
             "Completed:", format(Sys.time()), "\n",
             "Total files:", nrow(results_df), "\n",
             "Successful conversions:", successes, "\n",
             "New conversions:", new_conversions, "\n",
             "Skipped (already existed):", skipped_count, "\n",
             "Timed out conversions:", timed_out_count, "\n",
             "Failed conversions:", failures - timed_out_count, "\n",
             "Total time:", round(as.numeric(elapsed), 2), "minutes\n",
             "Average time per file:", round(as.numeric(elapsed) / (length(raw_files) - skipped_count), 2), "minutes\n\n",
             "Size Statistics:\n",
             "Average input size: ", size_stats$avg_input_size_MB, " MB\n",
             "Average output size: ", size_stats$avg_output_size_MB, " MB\n",
             "Average size ratio (output/input): ", size_stats$avg_size_ratio, "\n",
             "Min size ratio: ", size_stats$min_size_ratio, "\n",
             "Max size ratio: ", size_stats$max_size_ratio, "\n\n",
             "Results saved to: ", csv_file, "\n",
             "HTRMS files saved to: ", htrms_folder, "\n\n",
             file = log_file, append = TRUE)
      } else {
         # Write summary to log file without size statistics
         cat("\nConversion Summary\n",
             "Completed:", format(Sys.time()), "\n",
             "Total files:", nrow(results_df), "\n",
             "Successful conversions:", successes, "\n",
             "New conversions:", new_conversions, "\n",
             "Skipped (already existed):", skipped_count, "\n",
             "Timed out conversions:", timed_out_count, "\n",
             "Failed conversions:", failures - timed_out_count, "\n",
             "Total time:", round(as.numeric(elapsed), 2), "minutes\n",
             "Average time per file:", round(as.numeric(elapsed) / (length(raw_files) - skipped_count), 2), "minutes\n\n",
             "Size Statistics: Not available (no valid output file sizes)\n\n",
             "Results saved to: ", csv_file, "\n",
             "HTRMS files saved to: ", htrms_folder, "\n\n",
             file = log_file, append = TRUE)
      }
   } else {
      cat("\nConversion Summary\n",
          "Completed:", format(Sys.time()), "\n",
          "Total files:", nrow(results_df), "\n",
          "Successful conversions:", successes, "\n",
          "Failed conversions:", failures, "\n",
          "Total time:", round(as.numeric(elapsed), 2), "minutes\n",
          "Results saved to: ", csv_file, "\n",
          "HTRMS files saved to: ", htrms_folder, "\n\n",
          file = log_file, append = TRUE)
   }
   
   # Write detailed file size table to log
   cat("Detailed File Size Report:\n", file = log_file, append = TRUE)
   write_tsv_to_log(results_df, log_file)
   
   # If there were failures, list them
   if (failures > 0) {
      cat("Failed Files:\n", file = log_file, append = TRUE)
      write_tsv_to_log(filter(results_df, !success), log_file)
      
      # Also list error messages for reference
      error_msgs <- results_df %>% 
         filter(!success & !is.na(error_message)) %>%
         select(input_file, error_message)
      
      if (nrow(error_msgs) > 0) {
         cat("\nError Messages:\n", file = log_file, append = TRUE)
         write_tsv_to_log(error_msgs, log_file)
      }
   }
   
   # Print summary to console
   skipped_count <- sum(results_df$skipped)
   timed_out_count <- sum(results_df$timed_out)
   new_conversions <- successes - skipped_count
   
   cat("\nConversion Summary:\n")
   cat("Total files:", nrow(results_df), "\n")
   cat("Successful conversions:", successes, "\n")
   cat("New conversions:", new_conversions, "\n")
   cat("Skipped (already existed):", skipped_count, "\n")
   cat("Timed out conversions:", timed_out_count, "\n")
   cat("Failed conversions:", failures - timed_out_count, "\n")
   cat("Total time:", round(as.numeric(elapsed), 2), "minutes\n")
   cat("Log file:", log_file, "\n")
   cat("Results CSV:", csv_file, "\n")
   cat("HTRMS files:", htrms_folder, "\n")
   
   # Return both the raw results and the organized data frame
   return(list(
      raw_results = results,
      results_df = results_df,
      log_file = log_file,
      csv_file = csv_file,
      htrms_folder = htrms_folder,
      elapsed_time = elapsed
   ))
}