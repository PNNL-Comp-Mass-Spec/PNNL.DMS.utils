# PNNL.DMS.utils v0.2.0

## v0.2.0
- Update `read_msfragger_from_DMS`
- Add `read_MSstats_from_MSFragger_job` to create an MSnSet from an MSFragger-generated MSstats.csv file.
- Fix errors caught by `devtools::check()`
- Modify examples to only run if connection to PNNL's DMS is successful
- Minor documentation updates
- Implement `R-CMD-check` and `pkgdown` GitHub actions. The former only currently runs on a Windows machine, due to needing ODBC drivers for Mac and Unix. `pkgdown` runs when new releases are generated, though it can also be initiated manually.

## v0.1.9
- Initial version with MSFragger output reading capability
