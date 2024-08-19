# PNNL.DMS.utils

<!-- badges: start -->
[![R-CMD-check](https://github.com/PNNL-Comp-Mass-Spec/PNNL.DMS.utils/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/PNNL-Comp-Mass-Spec/PNNL.DMS.utils/actions/workflows/R-CMD-check.yaml)
<!-- badges: end -->

A collection of tools for accessing PNNL's Data Management System (DMS).

## R Installation and Usage

```R
if(!require("remotes", quietly = T)) install.packages("remotes")
remotes::install_github("PNNL-Comp-Mass-Spec/PNNL.DMS.utils")
library(PNNL.DMS.utils)
```

### Installation Tips

If within PNNL network there may be an error associated with `mount_smbfs`. This happens due to network access credentials. Options are either to wait or proactively access one of the PNNL servers. For example try mounting one of the public directories from the terminal window. Enter your network password once requested. 
`mount -t smbfs //protoapps/DataPkgs/Public/ ~/temp_msms_results`
Then compilation of the vignettes that imply access to PNNL DMS should proceed smoothly.

