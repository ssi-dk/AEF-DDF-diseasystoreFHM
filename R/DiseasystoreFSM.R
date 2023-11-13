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

      x <- daily_admissions
      y <- weekly_admissions
      
      # funtion to do proportional imputing by group
      imputeprop <- function(x, y, value.name, merge.name,
                            na.zero = FALSE) {

        if (dim(x)[2]!=3) stop("x must be on long format with 3 columns")
        if (dim(y)[2]!=3) stop("y must be on long format with 3 columns")
        if (sum(names(x) %in% names(y))!=2) stop("there must be a common group and a common value column")
        if (!(value.name %in% names(x))) stop("value.name must be in x")
        if (!(value.name %in% names(y))) stop("value.name must be in y")

        x <- as.data.table(x)
        y <- as.data.table(y)

        if ("impute_value" %in% names(x)) stop("'impute_value' is a reserved name for the function")
        if ("impute_value" %in% names(y)) stop("'impute_value' is a reserved name for the function")

        names(x)[which(names(x) == value.name)] <- "impute_value"
        names(y)[which(names(y) == value.name)] <- "impute_value"

        if ("impute_group_com" %in% names(x)) stop("'impute_group_com' is a reserved name for the function")
        if ("impute_group_com" %in% names(y)) stop("'impute_group_com' is a reserved name for the function")

        names(x)[which(names(x) == merge.name)] <- "impute_group_com"
        names(y)[which(names(y) == merge.name)] <- "impute_group_com"

        name_impute_group_x <- names(x)[which(!(names(x) %in% c("impute_value","impute_group_com")))]
        names(x)[which(!(names(x) %in% c("impute_value","impute_group_com")))] <- "impute_group_x"

        name_impute_group_y <- names(y)[which(!(names(y) %in% c("impute_value","impute_group_com")))]
        names(y)[which(!(names(y) %in% c("impute_value","impute_group_com")))] <- "impute_group_y"

        if (any(is.na(x[, impute_value])) && na.zero==FALSE) stop("NAs are not allowed in x when na.zero = FALSE")
        if (any(is.na(y[, impute_value])) && na.zero==FALSE) stop("NAs are not allowed in y when na.zero = FALSE")

        if (any(is.na(x[,impute_value])) && na.zero==TRUE) {
          warning("NAs in x are replaced by zeroes")
          x[is.na(impute_value), impute_value := 0]
        }

        if (any(is.na(y[,impute_value])) && na.zero==TRUE) {
          warning("NAs in y are replaced by zeroes")
          y[is.na(impute_value), impute_value := 0]
        }

        # check common group sums, and adust id needed
        tmpx <- x[, .(sum = sum(impute_value)),by=.(impute_group_com)]
        tmpy <- y[, .(sum = sum(impute_value)),by=.(impute_group_com)]
        tmp <- merge(tmpx, tmpy, by = "impute_group_com")

        # matplot(tmp[,-1],type="l")
        # plot(tmp[,(sum.x-sum.y)/(sum.x+sum.y)/2],type="l"); grid()

        if (NROW(tmpx)!=NROW(tmpy)) warning("not all impute_groups present, data omitted")

        if (!all(tmp$sum.x==tmp$sum.y)) {
          warning("sums are not equal in groups, values are adjusted to mean")
          tmp[, adj.sum := round((sum.x + sum.y)/2.)]
          tmp[, ':='(rel.x = adj.sum/sum.x,
                    rel.y = adj.sum/sum.y)]

          x <- merge(x,tmp[,.(impute_group_com, rel.x, adj.sum)],by="impute_group_com")
          y <- merge(y,tmp[,.(impute_group_com, rel.y, adj.sum)],by="impute_group_com")

          x[, impute_value := round(impute_value * rel.x)]
          y[, impute_value := round(impute_value * rel.y)]

          x[,diff.sum := sum(impute_value) - adj.sum[1], by =.(impute_group_com)]
          x[,id.val.max := which.max(impute_value), by = .(impute_group_com)]
          x[,impute_value := impute_value - diff.sum * ((1:.N)==id.val.max), by = .(impute_group_com)]
          #x[,diff.sum := sum(impute_value) - adj.sum[1], by =.(impute_group_com)]

          y[,diff.sum := sum(impute_value) - adj.sum[1], by =.(impute_group_com)]
          y[,id.val.max := which.max(impute_value), by = .(impute_group_com)]
          y[,impute_value := impute_value - diff.sum * ((1:.N)==id.val.max), by = .(impute_group_com)]
          #y[,diff.sum := sum(impute_value) - adj.sum[1], by =.(impute_group_com)]

          x <- x[,1:3]
          y <- y[,1:3]

          # check common group sums to see if adjusting has gone ok
          tmpx <- x[, .(sum = round(sum(impute_value))),by=.(impute_group_com)]
          tmpy <- y[, .(sum = round(sum(impute_value))),by=.(impute_group_com)]
          tmp <- merge(tmpx, tmpy, by = "impute_group_com")
        }

        if (!all(tmp$sum.x==tmp$sum.y)) stop("error in adjusting group values")

        # find share imputing
        x[,impute_share := impute_value/sum(impute_value), impute_group_com]
        y[,impute_share := impute_value/sum(impute_value), impute_group_com]

        res <- merge(x, y, by="impute_group_com", allow.cartesian = TRUE)

        res[, impute_share := impute_share.x * impute_share.y]

        # include sum
        res <- merge(res,tmp[,.(impute_group_com,sum=sum.x)],by="impute_group_com")

        # get decimal number
        res[, impute_value_dec := impute_share * sum]
        res[, impute_value := floor(impute_share * sum)]

        res[, impute_value_dec := impute_value_dec - impute_value]

        # maximal number of missing values in the groups
        n.max.add <- res[,sum(impute_value_dec), by=.(impute_group_com)][, max(V1)]

        # impute by finding maximm residual in rows/cols with missing values
        for (i in 1:n.max.add) {

          res[,res.x := impute_value.x[1] - sum(impute_value), by = .(impute_group_com, impute_group_x)]
          res[,res.y := impute_value.y[1] - sum(impute_value), by = .(impute_group_com, impute_group_y)]

          add.values <- res[res.x > 0 & res.y > 0,
                            .(.I[which.max(impute_value_dec)]),
                            by = .(impute_group_com)][,V1]

          res[add.values, ':='(impute_value = impute_value + 1L,
                  impute_value_dec = impute_value_dec - 1)]

        }

        if (!res[, .(sum(impute_value), sum[1]), by= .( impute_group_com)][, all(V1==V2)]) {
          stop("imputing failed")
        }

        # clean up and rename to input names
        res <- res[,.(impute_group_com, impute_group_x, impute_group_y, impute_value)]
        names(res) <- c(merge.name, name_impute_group_x, name_impute_group_y, value.name)

        return(res)

      }

      out <- imputeprop(x, y, value.name = "n_admissions", merge.name = "week")

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
