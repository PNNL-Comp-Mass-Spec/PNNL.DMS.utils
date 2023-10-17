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

## MacOS installation

1. Install [Homebrew](https://brew.sh/)
2. Install libraries and utilities with the commands
    ```Shell
    brew install unixodbc
    brew install freetds
    ```
    Note: depending on your privileges, you may need to prepend `sudo` to the above commands.
<!--Note, the `--with-unixodbc` option in freetds installation is deprecated.-->

3. Create a configuration file `~/.odbcinst.ini` and add
```INI
[FreeTDS]
Driver = <driver_location>
```
- The `<driver_location>` varies depending on your Mac's architecture. To determine the architecture, navigate to <br> System Settings > General > About > Chip
  - On Intel-based Macs, `<driver_location>` is `/usr/local/lib/libtdsodbc.so`.
  - On Macs with Apple Silicon (e.g., Macs whose chip is an M1, M2, or variant thereof), `<driver_location>` is `/opt/homebrew/lib/libtdsodbc.so`.

  Note: If your location of `libtdsodbc.so` differs, use the proper location.

4. In October 2023, it was discovered that there was a change to the `odbc` package in R that prevented the FreeTDS driver from being found. As a [workaround](https://stackoverflow.com/a/76765063), reinstall this package from source using the following commands:
```R
remove.packages("odbc")
install.packages("odbc", type = "source")
```

### Installation Tips

If within PNNL network there may be an error associated with `mount_smbfs`. This happens due to network access credentials. Options are either to wait or proactively access one of the PNNL servers. For example try mounting one of the public directories from the terminal window. Enter your network password once requested. 
`mount -t smbfs //protoapps/DataPkgs/Public/ ~/temp_msms_results`
Then compilation of the vignettes that imply access to PNNL DMS should proceed smoothly.

