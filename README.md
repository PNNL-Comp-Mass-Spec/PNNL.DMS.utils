# PNNL.DMS.utils

A collection of tools for accessing PNNL's Data Management System (DMS).

## R Installation and Usage

```R
if(!require("remotes", quietly = T)) install.packages("remotes")
remotes::install_github("PNNL-Comp-Mass-Spec/PNNL.DMS.utils")
library(PNNL.DMS.utils)
```

## MacOS installation

On MacOS, install [Homebrew](https://brew.sh/), then use

```Shell
brew install unixodbc
brew install freetds
```
Note, the `--with-unixodbc` option in freetds installation is deprecated.

Create `~/.odbcinst.ini` file and add
```INI
[FreeTDS]
Driver = <driver_location>
```
On Intel-based Macs, `<driver_location>` is `/usr/local/lib/libtdsodbc.so`. On M Series-based Macs, `<driver_location>` is `/opt/homebrew/lib/libtdsodbc.so`.

If your location of `libtdsodbc.so` differs, use the proper location.

### Installation Tips

If within PNNL network there may be an error associated with `mount_smbfs`. This happens due to network access credentials. Options are either to wait or proactively access one of the PNNL servers. For example try mounting one of the public directories from the terminal window. Enter your network password once requested. 
`mount -t smbfs //protoapps/DataPkgs/Public/ ~/temp_msms_results`
Then compilation of the vignettes that imply access to PNNL DMS should proceed smoothly.

