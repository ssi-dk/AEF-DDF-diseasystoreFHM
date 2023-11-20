# Store the current options
remote_conn <- diseasystore::diseasyoption("remote_conn", "DiseasystoreFhm")
source_conn <- diseasystore::diseasyoption("source_conn", "DiseasystoreFhm")
target_conn <- diseasystore::diseasyoption("target_conn", "DiseasystoreFhm")

# We assume the data is made available to us in a "testdata" folder
# If it isn't, and we have an internet connection, we download the FHM files for testing
fhm_files <- paste0(names(source_conn), ".csv")
local_conn <- "testdata"

# Check that the files are available
test_data_missing <- purrr::some(fhm_files, ~ !file.exists(file.path(local_conn, .)))

if (test_data_missing && curl::has_internet()) {

  # Ensure download folder exists
  if (!checkmate::test_directory_exists(local_conn)) dir.create(local_conn)

  # Then we download the first n rows of each data set of interest
  purrr::walk2(names(remote_conn), remote_conn, \(file_name, url) {
    readr::read_csv(url, show_col_types = FALSE, progress = FALSE) |>
      readr::write_csv(file.path(local_conn, paste0(file_name, ".csv")))
  })
}

# Check that the files are available after attempting to download
if (purrr::some(fhm_files, ~ !file.exists(file.path(local_conn, .)))) {
  stop("DiseasystoreFhm: test data not available and could not be downloaded")
}


test_that("DiseasystoreFhm initializes correctly", {

  # Initialize without start_date and end_date
  expect_no_error(ds <- DiseasystoreFhm$new(
    verbose = FALSE,
    target_conn = DBI::dbConnect(RSQLite::SQLite())
  ))

  # Check feature store has been created
  checkmate::expect_class(ds, "DiseasystoreFhm")
  expect_equal(ds$label, "FHM")


  # Check all FeatureHandlers have been initialized
  private <- ds$.__enclos_env__$private
  feature_handlers <- purrr::keep(ls(private), ~ startsWith(., "fhm_")) |>
    purrr::map(~ purrr::pluck(private, .))

  purrr::walk(feature_handlers, ~ {
    checkmate::expect_class(.x, "FeatureHandler")
    checkmate::expect_function(.x$compute)
    checkmate::expect_function(.x$get)
    checkmate::expect_function(.x$key_join)
  })
})


test_that("DiseasystoreFhm works with URL source_conn", {

  if (curl::has_internet()) {

    # Ensure source is set as the remote
    options("diseasystore.DiseasystoreFhm.source_conn" = remote_conn)

    expect_no_error(ds <- DiseasystoreFhm$new(
      target_conn = DBI::dbConnect(RSQLite::SQLite()),
      start_date = as.Date("2020-03-02"),
      end_date = as.Date("2020-03-02"),
      verbose = FALSE
    ))

    expect_no_error(ds$get_feature("n_admission"))

    rm(ds)
  }
})


test_that("DiseasystoreFhm works with directory source_conn", {

  # Ensure source is set as the local directory
  options("diseasystore.DiseasystoreFhm.source_conn" =
            purrr::map(names(remote_conn), ~ file.path(local_conn, paste0(., ".csv"))) |>
            stats::setNames(names(remote_conn)))

  expect_no_error(ds <- DiseasystoreFhm$new(
    target_conn = DBI::dbConnect(RSQLite::SQLite()),
    start_date = as.Date("2020-03-02"),
    end_date = as.Date("2020-03-02"),
    verbose = FALSE
  ))

  expect_no_error(ds$get_feature("n_admission"))

  rm(ds)
})


test_that("DiseasystoreFhm can retrieve features from a fresh state", {
  for (conn in get_test_conns()) {

    # Initialize without start_date and end_date
    expect_no_error(ds <- DiseasystoreFhm$new(verbose = FALSE, target_conn = conn))

    # Attempt to get features from the feature store
    # then check that they match the expected value from the generators
    purrr::walk2(ds$available_features, names(ds$fs_map), ~ {
      start_date <- as.Date("2020-03-02")
      end_date   <- as.Date("2020-03-05")

      feature <- ds$get_feature(.x, start_date = start_date, end_date = end_date) |>
        dplyr::collect()

      feature_checksum <- feature |>
        SCDB::digest_to_checksum() |>
        dplyr::pull("checksum") |>
        sort()

      reference_generator <- eval(parse(text = paste0(.y, "_()")))$compute

      reference <- reference_generator(start_date  = start_date,
                                       end_date    = end_date,
                                       slice_ts    = ds$slice_ts,
                                       source_conn = ds$source_conn) %>%
        dplyr::copy_to(ds$target_conn, ., overwrite = TRUE) |>
        dplyr::collect()

      reference_checksum <- reference |>
        SCDB::digest_to_checksum() |>
        dplyr::pull("checksum") |>
        sort()

      expect_identical(feature_checksum, reference_checksum)
    })

    rm(ds)
  }
})


test_that("DiseasystoreFhm can extend existing features", {
  for (conn in get_test_conns()) {

    # Initialize without start_date and end_date
    expect_no_error(ds <- DiseasystoreFhm$new(verbose = FALSE, target_conn = conn))

    # Attempt to get features from the feature store (using different dates)
    # then check that they match the expected value from the generators
    purrr::walk2(ds$available_features, names(ds$fs_map), ~ {
      start_date <- as.Date("2020-03-02")
      end_date   <- as.Date("2020-03-10")

      feature <- ds$get_feature(.x, start_date = start_date, end_date = end_date) |>
        dplyr::collect() |>
        SCDB::digest_to_checksum() |>
        dplyr::pull("checksum") |>
        sort()

      reference_generator <- eval(parse(text = paste0(.y, "_()")))$compute

      reference <- reference_generator(start_date  = start_date,
                                       end_date    = end_date,
                                       slice_ts    = ds$slice_ts,
                                       source_conn = ds$source_conn) %>%
        dplyr::copy_to(ds$target_conn, ., name = "ds_tmp", overwrite = TRUE) |>
        dplyr::collect() |>
        SCDB::digest_to_checksum() |>
        dplyr::pull("checksum") |>
        sort()

      expect_identical(feature, reference)
    })

    rm(ds)
  }
})


# Helper function that checks the output of key_joins
key_join_features_tester <- function(output, start_date, end_date) {
  # The output dates should match start and end date
  testthat::expect_true(min(output$date) == start_date)
  testthat::expect_true(max(output$date) == end_date)
}


# Set start and end dates for the rest of the tests
start_date <- as.Date("2020-03-02")
end_date   <- as.Date("2020-03-10")



test_that("DiseasystoreFhm can key_join features", {
  for (conn in get_test_conns()) {

    # Initialize without start_date and end_date
    expect_no_error(ds <- DiseasystoreFhm$new(verbose = FALSE, target_conn = conn))

    # Attempt to perform the possible key_joins
    available_observables  <- ds$available_features |>
      purrr::keep(~ startsWith(., "n_") | endsWith(., "_temperature"))
    available_stratifications <- ds$available_features |>
      purrr::discard(~ startsWith(., "n_") | endsWith(., "_temperature"))



    # First check we can aggregate without a stratification
    purrr::walk(available_observables,
                ~ expect_no_error(ds$key_join_features(observable = as.character(.),
                                                       stratification = NULL,
                                                       start_date, end_date)))


    # Then test combinations with non-NULL stratifications
    expand.grid(observable     = available_observables,
                stratification = available_stratifications) |>
      purrr::pwalk(~ {
        # This code may fail (gracefully) in some cases. These we catch here
        output <- tryCatch({
          ds$key_join_features(
            observable = as.character(..1),
            stratification = eval(parse(text = glue::glue("rlang::quos({..2})"))),
            start_date, end_date
          )
        },
        warning = function(w) {
          checkmate::expect_character(w$message, pattern = "observable already stratified by")
          return(NULL)
        },
        error = function(e) {
          expect_equal(
            e$message,
            paste("(At least one) stratification feature does not match observable aggregator. Not implemented yet.")
          )
          return(NULL)
        })

        # If the code does not fail, we test the output
        if (!is.null(output)) {
          key_join_features_tester(dplyr::collect(output), start_date, end_date)
        }
      })

    rm(ds)
  }
})


test_that("DiseasystoreFhm key_join fails gracefully", {
  for (conn in get_test_conns()) {

    # Initialize without start_date and end_date
    expect_no_error(ds <- DiseasystoreFhm$new(verbose = FALSE, target_conn = conn))


    # Attempt to perform the possible key_joins
    available_observables  <- ds$available_features |>
      purrr::keep(~ startsWith(., "n_") | endsWith(., "_temperature"))
    available_stratifications <- ds$available_features |>
      purrr::discard(~ startsWith(., "n_") | endsWith(., "_temperature"))


    # Test key_join with malformed inputs
    expand.grid(observable     = available_observables,
                stratification = "non_existent_stratification") |>
      purrr::pwalk(~ {
        # This code may fail (gracefully) in some cases. These we catch here
        output <- tryCatch({
          ds$key_join_features(
            observable = as.character(..1),
            stratification = ..2,
            start_date = start_date,
            end_date = end_date
          )
        }, error = function(e) {
          checkmate::expect_character(e$message, pattern = "Must be element of set")
          return(NULL)
        })

        # If the code does not fail, we test the output
        if (!is.null(output)) {
          key_join_features_tester(dplyr::collect(output), start_date, end_date)
        }
      })


    expand.grid(observable     = available_observables,
                stratification = "test = non_existent_stratification") |>
      purrr::pwalk(~ {
        # This code may fail (gracefully) in some cases. These we catch here
        output <- tryCatch({
          ds$key_join_features(
            observable = as.character(..1),
            stratification = eval(parse(text = glue::glue("rlang::quos({..2})"))),
            start_date = start_date,
            end_date = end_date
          )
        }, error = function(e) {
          checkmate::expect_character(
            e$message,
            pattern = glue::glue("Stratification variable not found. ",
                                 "Available stratification variables are: ",
                                 "{toString(available_stratifications)}")
          )
          return(NULL)
        })

        # If the code does not fail, we test the output
        if (!is.null(output)) {
          key_join_features_tester(dplyr::collect(output), start_date, end_date)
        }
      })


    # Cleanup
    rm(ds)
  }
})


# Reset the options
options("diseasystore.DiseasystoreFhm.source_conn" = source_conn)
options("diseasystore.DiseasystoreFhm.target_conn" = target_conn)
