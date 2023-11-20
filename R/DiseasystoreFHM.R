#' @title feature store handler of Swedish FHMs data
#'
#' @description
#'   This `DiseasystoreFhm` [R6][R6::R6Class] brings support for using the Swedish FHM data.
#' @examples
#'   ds <- DiseasystoreFhm$new(source_conn = ".",
#'                             target_conn = DBI::dbConnect(RSQLite::SQLite()))
#'
#' @return
#'   A new instance of the `DiseasystoreFhm` [R6][R6::R6Class] class.
#' @export
DiseasystoreFhm <- R6::R6Class( # nolint: object_name_linter.
  classname = "DiseasystoreFhm",
  inherit = diseasystore::DiseasystoreBase,

  private = list(
    fs_generic = NULL,
    fs_specific = list("population" = "n_population",
                       "population" = "age_group",
                       "admission"  = "n_admission"),
    .label = "FHM",

    fhm_population = NULL,
    fhm_admission  = NULL,

    initialize_feature_handlers = function() {

      # Here we initialize each of the feature handlers for the class
      # See the documentation above at the corresponding methods
      private$fhm_population <- fhm_population_()
      private$fhm_admission  <- fhm_admission_()
    }
  )
)


#' @importFrom rlang .data
fhm_population_ <- function() {
  diseasystore::FeatureHandler$new(
    compute = function(start_date, end_date, slice_ts, source_conn) {
      coll <- checkmate::makeAssertCollection()
      checkmate::assert_date(start_date, lower = as.Date("2020-03-02"), add = coll)
      checkmate::assert_date(end_date,   upper = as.Date("2022-03-27"), add = coll)
      checkmate::reportAssertions(coll)

      # We use demography data from `diseasy`s contact_basis data set
      out <- diseasy::contact_basis$SE$demography |>
        dplyr::transmute(.data$age, "n_population" = .data$population) |>
        dplyr::mutate(age_group = cut(.data$age, breaks = c((0:9)*10, Inf), right = FALSE,
                                      labels = diseasystore::age_labels((0:9)*10))) |>
        dplyr::summarise(n_population = sum(.data$n_population), .by = "age_group") |>
        dplyr::transmute("age_group" = as.character(.data$age_group), .data$n_population) |>
        dplyr::mutate(valid_from = as.Date("2020-01-01"), valid_until = as.Date(NA))

      return(out)
    },
    key_join = diseasystore::key_join_sum
  )
}


#' @importFrom rlang .data
fhm_admission_ <- function() {
  diseasystore::FeatureHandler$new(
    compute = function(start_date, end_date, slice_ts, source_conn) {
      coll <- checkmate::makeAssertCollection()
      checkmate::assert_date(start_date, lower = as.Date("2020-03-02"), add = coll)
      checkmate::assert_date(end_date,   upper = as.Date("2022-03-27"), add = coll)
      checkmate::reportAssertions(coll)

      # There is no one data source for the admission data stratified at the level we want.
      # So we have to impute data based on three different data sources

      # The "weekly_admissions" data contains the weekly distribution of ALL admissions by
      # age group. We collect this data and transform to a long format
      weekly_admissions <- purrr::pluck(source_conn, "weekly_admissions") |>
        readr::read_csv(show_col_types = FALSE) |>
        dplyr::select(!"Obs") |>
        dplyr::mutate("age_group" = dplyr::if_else(.data$aldrar == "0-9", "00-09", .data$aldrar)) |>
        dplyr::select("age_group", tidyselect::matches(r"{202\dV\d+}")) |>
        tidyr::pivot_longer(!"age_group", values_to = "n_admission",
                            names_to = "week", names_transform = ~ stringr::str_replace(., "V", "-W"))


      # The "daily_admissions" data contains, among other things, number of people in hospital stratified
      # into "ICU" and "non-ICU" groups.
      daily_admissions <- purrr::pluck(source_conn, "daily_admissions") |>
        readr::read_csv(show_col_types = FALSE) |>
        dplyr::transmute("date" = .data$datum,
                         "n_admission_non_icu" = .data$`Inskrivna i slutenvård - antal`,
                         "n_admission_icu" = .data$`Inskrivna i intensivvård - antal`) |>
        dplyr::mutate(dplyr::across(tidyselect::starts_with("n_admission_"), ~ as.numeric(ifelse(. == "-", NA, .))))



      # Add the week to the daily admissions data so everything can be coupled by week.
      # Further, combine stratified admission data to get total admissions as in the weekly data
      daily_admissions <- daily_admissions |>
        dplyr::rowwise() |>
        dplyr::transmute(.data$date, week = format(.data$date, "%G-W%V"),
                         "n_admission" = sum(.data$n_admission_non_icu, .data$n_admission_icu, na.rm = TRUE)) |>
        dplyr::ungroup()


      # Trim to requested weeks
      # The imputing function works with full weeks, so we roughly trim to the
      # corresponding weeks before trimming fully after imputing
      daily_admissions <- daily_admissions |>
        dplyr::filter(lubridate::floor_date(start_date, week_start = 1, unit = "day") <= .data$date,
                      .data$date <= lubridate::ceiling_date(end_date, week_start = 1, unit = "week"))

      # Perform the imputing
      out <- impute_proportionally(daily_admissions, weekly_admissions,                                                 # nolint: object_usage_linter
                                   value_name = "n_admission", merge_name = "week") |>
        as.data.frame() |>
        dplyr::select(!"week")

      # Fully trim
      out <- out |>
        dplyr::filter(start_date <= date, date <= end_date) |>
        dplyr::transmute(.data$age_group, .data$n_admission,
                         "valid_from" = .data$date, "valid_until" = .data$date + lubridate::days(1))

      return(out)
    },
    key_join = diseasystore::key_join_sum
  )
}


# Set default options for the package related to the Google COVID-19 store
rlang::on_load({
  options("diseasystore.DiseasystoreFhm.remote_conn" = list(
    "weekly_admissions" = "https://static.dwcdn.net/data/JQWM4.csv",
    "daily_admissions"  = "https://static.dwcdn.net/data/fJECo.csv"
  ))
  options("diseasystore.DiseasystoreFhm.source_conn" = diseasystore::diseasyoption("remote_conn", "DiseasystoreFhm"))
  options("diseasystore.DiseasystoreFhm.target_conn" = "")
  options("diseasystore.DiseasystoreFhm.target_schema" = "")
})
