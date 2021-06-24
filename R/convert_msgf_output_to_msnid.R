#' Converting MS-GF+ output to MSnID
#'
#' @param results (data.frame) concatenated output of MS-GF+
#' @return (MSnID) MSnID object
#' 
#' @importFrom MSnID MSnID psms
#' @importFrom dplyr mutate rename %>%


#' @export
convert_msgf_output_to_msnid <- function(x) {
  suppressMessages(msnid <- MSnID("."))
  x <- x %>% mutate(accession = Protein,
                    calculatedMassToCharge = (MH + (Charge-1)*MSnID:::.PROTON_MASS)/Charge,
                    chargeState = Charge,
                    experimentalMassToCharge = PrecursorMZ,
                    isDecoy = grepl("^XXX", Protein),
                    spectrumFile = Dataset,
                    spectrumID = Scan) %>%
    rename(peptide = Peptide)
  # clean peptide sequences
  x <- mutate(x, pepSeq = MSnID:::.get_clean_peptide_sequence(peptide))
  
  psms(msnid) <- x
  
  return(msnid)
}