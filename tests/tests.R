library(testthat)

path_check <- function(path) {
    expect_true(grepl("Rtmp", path, ignore.case = FALSE))
    if (.Platform$OS.type == "unix") {
        system(glue::glue("umount {path}"))
    }
}
# cat("1------\n")
expect_true(is_PNNL_DMS_connection_successful())
# cat("2------\n")
expect_equal(dim(get_dms_job_records(4232)), c(1, 23))
# cat("3------\n")
expect_equal(get_output_folder_for_job_and_tool(2344924, "DiaNN"), "DNN202408121049_Auto2344924")
# cat("4------\n")
expect_equal(dim(get_job_records_by_dataset_package(4232)), c(2, 22))
# cat("5------\n")
expect_equal(dim(get_datasets_by_data_package(4232)), c(1, 13))
# cat("6------\n")
path <- path_to_FASTA_used_by_DMS(4232)
path_check(path)
# cat("7------\n")
path <- path_to_study_design_from_DMS(4232)
if (.Platform$OS.type == "unix") {
    path_check(path)
}
# cat("8------\n")
expect_equal(dim(read_masic_data_from_DMS(4232)), c(20938, 22)) # check args
# cat("9------\n")
expect_equal(dim(read_msfragger_data_from_DMS( # Fail
    4804,
    "MSFragger_Tryp_ProlineRule_ProtNTermAcet_Stat_CysAlk_TMT_6Plex_20ppmParTol.params",
    "MSFragger_PepFDR_0.99_ProtFDR_0.99_IncludeDecoys.xml",
    "ID_008350_56958B5A.fasta"
)), c(266203, 11))
# cat("11------\n")
expect_equal(dim(read_AScore_results_from_DMS(4136)), c(916130, 12)) # check args
# get_tool_output_files_for_job_number(2344924, fileNamePttrn = ".") -> res
