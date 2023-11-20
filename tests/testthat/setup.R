# Configure diseasystore for testing
target_schema <- "test_ds"
options("diseasystore.target_schema" = target_schema)

# This function can be copied to your tests/testthat folder

#' Get a list of data base connections to test on
#' @return
#'   If you run your tests locally, it returns a list of connections corresponding to conn_list and conn_args
#'   If you run your tests on GitHub, it return a list of connection corresponding to the environment variables.
#'   i.e. the GitHub workflows will configure the testing backeds
#' @importFrom rlang `:=`
get_test_conns <- function() {

  # Check if we run remotely
  running_locally <- !identical(Sys.getenv("CI"), "true")

  # Define list of connections to check
  if (running_locally) {

    # Define our local connection backends
    conn_list <- list(
      # Backend string = package::function
      "SQLite"     = "RSQLite::SQLite",
      "PostgreSQL" = "RPostgres::Postgres"
    )

  } else {
    # Use the connection configured by the remote
    conn_list <- tibble::lst(
      !!Sys.getenv("BACKEND", unset = "SQLite") := !!Sys.getenv("BACKEND_DRV", unset = "RSQLite::SQLite")         # nolint: object_name_linter
    )
  }

  # Define list of args to conns
  if (running_locally) {

    # Define our local connection arguments
    conn_args <- list(
      # Backend string = list(named args)
      "SQLite" = list(dbname = tempfile())
    )

  } else {
    # Use the connection configured by the remote
    conn_args <- tibble::lst(
      !!Sys.getenv("BACKEND", unset = "SQLite") :=                                                                # nolint: object_name_linter
        !!Sys.getenv("BACKEND_ARGS", unset = "list(dbname = tempfile())")                                         # nolint: object_name_linter
    ) |>
      purrr::map(~ eval(parse(text = .)))
  }


  get_driver <- function(x = character(), ...) {
    if (!grepl(".*::.*", x)) stop("Package must be specified with namespace (e.g. RSQLite::SQLite)!\n",
                                  "Received: ", x)
    parts <- strsplit(x, "::")[[1]]

    # Skip unavailable packages
    if (!requireNamespace(parts[1], quietly = TRUE)) {
      return()
    }

    drv <- getExportedValue(parts[1], parts[2])

    tryCatch(suppressWarnings(SCDB::get_connection(drv = drv(), ...)),  # We expect a warning if no tables are found
             error = function(e) {
               NULL # Return NULL, if we cannot connect
             })
  }

  # Create connection generator
  conn_configuration <- dplyr::left_join(
    tibble::tibble(backend = names(conn_list), conn_list = unname(unlist(conn_list))),
    tibble::tibble(backend = names(conn_args), conn_args),
    by = "backend"
  )

  test_conns <- purrr::pmap(conn_configuration, ~ purrr::partial(get_driver, x = !!..2, !!!..3)())
  names(test_conns) <- conn_configuration$backend
  test_conns <- purrr::discard(test_conns, is.null)

  return(test_conns)
}

conns <- get_test_conns()

# Ensure the target conns are empty and configured correctly
for (conn in conns) {

  # SQLite back-ends gives an error in SCDB if there are no tables (it assumes a bad configuration)
  # We create a table to suppress this warning
  tryCatch(
    SCDB::get_tables(conn),
    warning = function(w) {
      checkmate::assert_character(w$message, pattern = "No tables found")
      DBI::dbWriteTable(conn, "mtcars", mtcars)
    }
  )

  # Try to write to the target schema
  test_id <- SCDB::id(paste(target_schema, "mtcars", sep = "."), conn)

  # Delete existing
  if (DBI::dbExistsTable(conn, test_id)) {
    DBI::dbRemoveTable(conn, test_id)
  }

  # Check write permissions
  DBI::dbWriteTable(conn, test_id, mtcars)
  if (!DBI::dbExistsTable(conn, test_id)) {
    stop("Cannot write to test schema (", target_schema, "). Check DB permissions.")
  }

  # Delete the existing data in the schema
  diseasystore::drop_diseasystore(schema = target_schema, conn = conn)
  diseasystore::drop_diseasystore(schema = paste0("not_", target_schema), conn = conn)

}



# Report testing environment to the user
message("#####\n",
        "Following drivers will be tested:\n",
        sprintf("  %s\n", names(conns)),
        sep = "")
message("#####")


# Close conns
purrr::walk(conns, ~ {
  DBI::dbDisconnect(.)
  rm(.)
})
