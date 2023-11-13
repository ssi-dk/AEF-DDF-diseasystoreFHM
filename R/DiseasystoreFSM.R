#' @title feature store handler of Swedish Folkesundhedsmyndighedens data
#'
#' @description
#'   This `DiseasystoreFSM` [R6][R6::R6Class] brings support for using the Swedish Folkesundhedsmyndigheden data.
#' @examples
#'   ds <- DiseasystoreFSM$new(source_conn = ".",
#'                             target_conn = DBI::dbConnect(RSQLite::SQLite()))
#'
#' @return
#'   A new instance of the `DiseasystoreFSM` [R6][R6::R6Class] class.
#' @export
DiseasystoreFSM <- R6::R6Class( # nolint: object_name_linter.
  classname = "DiseasystoreFSM",
  inherit = diseasystore::DiseasystoreBase,

  private = list(
    fs_generic = NULL,
    fs_specific = list("population" = "n_population",
                       "admission"  = "n_admission"),
    .label = "Folkesundhedsmyndigheden",

    folkesundhedsmyndigheden_population = NULL,
    folkesundhedsmyndigheden_admission  = NULL,

    initialize_feature_handlers = function() {

      # Here we initialize each of the feature handlers for the class
      # See the documentation above at the corresponding methods
      private$folkesundhedsmyndigheden_population <- folkesundhedsmyndigheden_population_()
      private$folkesundhedsmyndigheden_admission  <- folkesundhedsmyndigheden_admission_()
    }
  )
)


folkesundhedsmyndigheden_population_ <- function() {
  diseasystore::FeatureHandler$new(
    compute = function(start_date, end_date, slice_ts, source_conn) {
      coll <- checkmate::makeAssertCollection()
      checkmate::assert_date(start_date, lower = as.Date("2020-03-02"), add = coll)
      checkmate::assert_date(end_date,   upper = as.Date("2022-03-27"), add = coll)
      checkmate::reportAssertions(coll)

      # We use demography data from `diseasy`s contact_basis data set
      out <- diseasy::contact_basis$SE$demography |>
        dplyr::transmute("key_age" = .data$age, .data$age, .data$population) |>
        dplyr::mutate(valid_from = as.Date("2020-01-01"), valid_until = as.Date(NA))

      return(out)
    },
    key_join = diseasystore::key_join_sum
  )
}


folkesundhedsmyndigheden_admission_ <- function() {
  diseasystore::FeatureHandler$new(
    compute = function(start_date, end_date, slice_ts, source_conn) {
      coll <- checkmate::makeAssertCollection()
      checkmate::assert_date(start_date, lower = as.Date("2020-03-02"), add = coll)
      checkmate::assert_date(end_date,   upper = as.Date("2022-03-27"), add = coll)
      checkmate::reportAssertions(coll)

      library(data.table)

      # There is no one data source for the admission data stratified at the level we want.
      # So we have to impute data based on three different data sources

      # The "weekly_admissions" data contains the weekly distribution of ALL admissions by
      # age group. We collect this data and transform to a long format
      weekly_admissions <- purrr::pluck(source_conn, "weekly_admissions") |>
        readr::read_csv(show_col_types = FALSE) |>
        dplyr::select(!"Obs") |>
        dplyr::rename("age_group" = "aldrar") |>
        tidyr::pivot_longer(!"age_group", values_to = "n_admission",
                            names_to = "week", names_transform = ~ stringr::str_replace(., "V", "-W"))


      # The "daily_admissions" data contains, among other things, number of people in hospital stratified
      # into "ICU" and "non-ICU" groups.
      daily_admissions <- purrr::pluck(source_conn, "daily_admissions") |>
        readr::read_csv(show_col_types = FALSE) |>
        dplyr::transmute("date" = .data$datum,
                         "n_admission_non_icu" = .data$`Inskrivna i slutenvård - antal`,
                         "n_admission_icu" = .data$`Inskrivna i intensivvård - antal`) |>
        dplyr::mutate(across(tidyselect::starts_with("n_admission_"), ~ as.numeric(ifelse(. == "-", NA, .))))



      # Add the week to the daily admissions data so everything can be coupled by week.
      # Further, combine stratified admission data to get total admissions as in the weekly data
      daily_admissions <- daily_admissions |>
        dplyr::rowwise() |>
        dplyr::transmute(.data$date, week = format(.data$date, "%G-W%V"),
                         "n_admission" = sum(.data$n_admission_non_icu, .data$n_admission_icu, na.rm = TRUE)) |>
        dplyr::ungroup()

      # Perform the imputing
      out <- impute_proportionally(daily_admissions, weekly_admissions,
                                   value_name = "n_admission", merge_name = "week") |>
        as.data.frame() |>
        dplyr::select(!"week")

      return(out)
    },
    key_join = diseasystore::key_join_sum
  )
}


# Set default options for the package related to the Google COVID-19 store
rlang::on_load({
  options("diseasystore.DiseasystoreFSM.remote_conn" = list(
    "weekly_admissions" = "https://static.dwcdn.net/data/JQWM4.csv",
    "daily_admissions"  = "https://static.dwcdn.net/data/fJECo.csv"
  ))
  options("diseasystore.DiseasystoreFSM.source_conn" = diseasystore::diseasyoption("remote_conn", "DiseasystoreFSM"))
  options("diseasystore.DiseasystoreFSM.target_conn" = "")
  options("diseasystore.DiseasystoreFSM.target_schema" = "")
})
