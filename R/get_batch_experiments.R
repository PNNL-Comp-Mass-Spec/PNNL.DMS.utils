#' Get experiment details for all LC-MS runs in a batch
#'
#' @description This function retrieves detailed experiment information for all LC-MS runs
#' in a specified batch from the DMS database. It can return either a simplified view with
#' key columns or all available experiment details.
#'
#' @param batch_id The batch number to query (numeric)
#' @param simplified Logical; if TRUE returns simplified view with key columns, 
#'        if FALSE returns all experiment columns. Default is TRUE.
#' @return A data frame containing experiment details for all runs in the batch.
#'   The returned columns depend on the simplified parameter.
#'
#' @examples
#' \dontrun{
#'   if (is_PNNL_DMS_connection_successful()) {
#'     # Get simplified view
#'     batch_details <- get_batch_experiments(11495)
#'     
#'     # Get all experiment columns
#'     batch_details_full <- get_batch_experiments(11495, simplified = FALSE)
#'   }
#' }
#'
#' @importFrom DBI dbConnect dbSendQuery dbFetch dbClearResult dbDisconnect
#' @importFrom dplyr %>% select arrange distinct
#' @export
get_batch_experiments <- function(batch_id, simplified = TRUE) {
   # Input validation
   if (!is.numeric(batch_id)) {
      stop("batch_id must be numeric")
   }
   
   # Get database connection
   con <- get_db_connection()
   on.exit(dbDisconnect(con))
   
   if (simplified) {
      # Original simplified query with key columns
      strSQL <- sprintf("
      SELECT DISTINCT
        bt.batch,
        bt.name as batch_name,
        bt.status,
        bt.dataset_id,
        bt.dataset,
        bt.instrument,
        bt.lc_column,
        bt.start as acquisition_start,
        bt.block,
        bt.run_order,
        d.exp_id,
        exp.experiment as experiment,
        exp.campaign_id,
        c.campaign,
        exp.reason,
        exp.comment,
        exp.sample_concentration,
        exp.created as experiment_created
      FROM v_batch_tracking_list_report bt
      LEFT JOIN t_dataset d ON bt.dataset_id = d.dataset_id
      LEFT JOIN t_experiments exp ON d.exp_id = exp.exp_id
      LEFT JOIN t_campaign c ON exp.campaign_id = c.campaign_id
      WHERE bt.batch = %d
      ORDER BY bt.start;", 
                        batch_id)
   } else {
      # Expanded query with all experiment columns
      strSQL <- sprintf("
      SELECT DISTINCT
        bt.batch,
        bt.name as batch_name,
        bt.status,
        bt.dataset_id,
        bt.dataset,
        bt.instrument,
        bt.lc_column,
        bt.start as acquisition_start,
        bt.block,
        bt.run_order,
        exp.*,  -- Include all experiment columns
        c.campaign
      FROM v_batch_tracking_list_report bt
      LEFT JOIN t_dataset d ON bt.dataset_id = d.dataset_id
      LEFT JOIN t_experiments exp ON d.exp_id = exp.exp_id
      LEFT JOIN t_campaign c ON exp.campaign_id = c.campaign_id
      WHERE bt.batch = %d
      ORDER BY bt.start;", 
                        batch_id)
   }
   
   qry <- dbSendQuery(con, strSQL)
   results <- dbFetch(qry)
   dbClearResult(qry)
   
   if (nrow(results) == 0) {
      warning(sprintf("No experiments found for batch %d", batch_id))
      return(NULL)
   }
   
   return(results)
}