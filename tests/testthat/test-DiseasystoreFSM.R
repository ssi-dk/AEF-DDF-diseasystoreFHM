test_that("DiseasystoreFSM works", {

  # Check we can read the data directly online
  if (curl::has_internet()) {
    expect_no_error(ds <- DiseasystoreFSM$new(
      target_conn = DBI::dbConnect(RSQLite::SQLite()),
      start_date = as.Date("2020-03-02"),
      end_date = as.Date("2020-03-02"),
      verbose = FALSE
    ))

    ds$available_features |>
      purrr::walk(~ expect_no_error(ds$get_feature(.)))
  }


  # Then we create a temporary directory for the Folkesundhedsmyndigheden data
  remote_conn <- options() %.% diseasystore.DiseasystoreFSM.remote_conn

  tmp_dir <- stringr::str_replace_all(tempdir(), r"{\\}", .Platform$file.sep)
  options(diseasystore.DiseasystoreFSM.source_conn = tmp_dir)

  sqlite_path <- file.path(tmp_dir, "diseasystore_fsm.sqlite")
  if (file.exists(sqlite_path)) {
    closeAllConnections()
    stopifnot("Could not delete SQLite DB before tests" = file.remove(sqlite_path))
  }

  target_conn <- \() DBI::dbConnect(RSQLite::SQLite(), sqlite_path)
  options(diseasystore.DiseasystoreFSM.target_conn = target_conn)


  # Then we download the first n rows of each data set of interest
  purrr::walk2(remote_conn, names(remote_conn), ~ {
    readr::read_csv(.x, show_col_types = FALSE, progress = FALSE) |>
      readr::write_csv(file.path(tmp_dir, .y))
  })

  # Initialize without start_date and end_date
  expect_no_error(ds <- DiseasystoreFSM$new(verbose = FALSE))

  # Check feature store has been created
  checkmate::expect_class(ds, "DiseasystoreFSM")
  expect_equal(ds %.% label, "Google COVID-19")


  # SCDB v0.1 gives a warning if database has no tables when used. We suppress this warning here
  # if this warning is no longer cast, remove this test
  expect_warning(SCDB::get_tables(ds$target_conn),
                 "No tables found. Check user privileges / database configuration")
  dplyr::copy_to(ds$target_conn, mtcars)
  expect_no_warning(SCDB::get_tables(ds$target_conn))


  # Check all FeatureHandlers have been initialized
  private <- ds$.__enclos_env__$private
  feature_handlers <- purrr::keep(ls(private), ~ startsWith(., "google_covid_19")) |>
    purrr::map(~ purrr::pluck(private, .))

  purrr::walk(feature_handlers, ~ {
    checkmate::expect_class(.x, "FeatureHandler")
    checkmate::expect_function(.x %.% compute)
    checkmate::expect_function(.x %.% get)
    checkmate::expect_function(.x %.% key_join)
  })

  # Attempt to get features from the feature store
  # then check that they match the expected value from the generators
  purrr::walk2(ds$available_features, names(ds$fs_map), ~ {
    start_date <- as.Date("2020-03-01")
    end_date   <- as.Date("2020-04-30")

    feature <- ds$get_feature(.x, start_date = start_date, end_date = end_date) |>
      dplyr::collect()

    feature_checksum <- feature |>
      SCDB::digest_to_checksum() |>
      dplyr::pull("checksum") |>
      sort()

    reference_generator <- eval(parse(text = paste0(.y, "_()"))) %.% compute

    reference <- reference_generator(start_date  = start_date,
                                     end_date    = end_date,
                                     slice_ts    = ds %.% slice_ts,
                                     source_conn = ds %.% source_conn) %>%
      dplyr::copy_to(ds %.% target_conn, ., overwrite = TRUE) |>
      dplyr::collect()

    reference_checksum <- reference |>
      SCDB::digest_to_checksum() |>
      dplyr::pull("checksum") |>
      sort()

    expect_identical(feature_checksum, reference_checksum)
  })


  # Attempt to get features from the feature store (using different dates)
  # then check that they match the expected value from the generators
  purrr::walk2(ds$available_features, names(ds$fs_map), ~ {
    start_date <- as.Date("2020-03-01")
    end_date   <- as.Date("2020-05-31")

    feature <- ds$get_feature(.x, start_date = start_date, end_date = end_date) |>
      dplyr::collect() |>
      SCDB::digest_to_checksum() |>
      dplyr::pull("checksum") |>
      sort()

    reference_generator <- eval(parse(text = paste0(.y, "_()"))) %.% compute

    reference <- reference_generator(start_date  = start_date,
                                     end_date    = end_date,
                                     slice_ts    = ds %.% slice_ts,
                                     source_conn = ds %.% source_conn) %>%
      dplyr::copy_to(ds %.% target_conn, ., name = "fs_tmp", overwrite = TRUE) |>
      dplyr::collect() |>
      SCDB::digest_to_checksum() |>
      dplyr::pull("checksum") |>
      sort()

    expect_identical(feature, reference)
  })

  # Attempt to perform the possible key_joins
  available_observables  <- ds$available_features |>
    purrr::keep(~ startsWith(., "n_") | endsWith(., "_temperature"))
  available_aggregations <- ds$available_features |>
    purrr::discard(~ startsWith(., "n_") | endsWith(., "_temperature"))

  key_join_features_tester <- function(output, start_date, end_date) {
    # The output dates should match start and end date
    expect_true(min(output$date) == start_date)
    expect_true(max(output$date) == end_date)
  }

  # Set start and end dates for the rest of the tests
  start_date <- as.Date("2020-03-01")
  end_date   <- as.Date("2020-04-30")

  # First check we can aggregate without an aggregation
  purrr::walk(available_observables,
              ~ expect_no_error(ds$key_join_features(observable = as.character(.),
                                                     aggregation = NULL,
                                                     start_date, end_date)))

  # Then test combinations with non-NULL aggregations
  expand.grid(observable  = available_observables,
              aggregation = available_aggregations) |>
    purrr::pwalk(~ {
      # This code may fail (gracefully) in some cases. These we catch here
      output <- tryCatch({
        ds$key_join_features(observable = as.character(..1),
                             aggregation = eval(parse(text = glue::glue("rlang::quos({..2})"))),
                             start_date, end_date)
      }, error = function(e) {
        expect_equal(e$message, paste("(At least one) aggregation feature does not match observable aggregator.",
                                      "Not implemented yet."))
        return(NULL)
      })

      # If the code does not fail, we test the output
      if (!is.null(output)) {
        key_join_features_tester(dplyr::collect(output), start_date, end_date)
      }
    })


  # Test key_join with malformed inputs
  expand.grid(observable  = available_observables,
              aggregation = "non_existent_aggregation") |>
    purrr::pwalk(~ {
      # This code may fail (gracefully) in some cases. These we catch here
      output <- tryCatch({
        ds$key_join_features(observable = as.character(..1),
                             aggregation = ..2,
                             start_date, end_date)
      }, error = function(e) {
        checkmate::expect_character(e$message, pattern = "Must be element of set")
        return(NULL)
      })

      # If the code does not fail, we test the output
      if (!is.null(output)) {
        key_join_features_tester(dplyr::collect(output), start_date, end_date)
      }
    })


  expand.grid(observable  = available_observables,
              aggregation = "test = non_existent_aggregation") |>
    purrr::pwalk(~ {
      # This code may fail (gracefully) in some cases. These we catch here
      output <- tryCatch({
        ds$key_join_features(observable = as.character(..1),
                             aggregation = eval(parse(text = glue::glue("rlang::quos({..2})"))),
                             start_date, end_date)
      }, error = function(e) {
        checkmate::expect_character(e$message,
                                    pattern = glue::glue("Aggregation variable not found. ",
                                                         "Available aggregation variables are: ",
                                                         "{toString(available_aggregations)}"))
        return(NULL)
      })

      # If the code does not fail, we test the output
      if (!is.null(output)) {
        key_join_features_tester(dplyr::collect(output), start_date, end_date)
      }
    })


  # Cleanup
  rm(ds)
})
