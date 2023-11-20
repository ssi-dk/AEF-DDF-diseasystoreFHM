if (require(curl) && require(usethis) && require(tibble) && require(diseasystore)) {

  # We express all population data in 10 10-year age groups
  age_cuts <- (0:9) * 10
  age_labels <- diseasystore::age_labels(age_cuts)

  # We get age population data like the contactdata package does using this source:
  # https://www.census.gov/programs-surveys/international-programs/about/idb.html
  # Terms of the data can be read here:
  # https://www.census.gov/data/developers/about/terms-of-service.html

  # Alternative data source is here:
  # https://population.un.org/wpp/Download/Standard/Population/
  idbzip <- "https://www.census.gov/data-tools/demo/data/idb/dataset/idbzip.zip"
  curl::curl_fetch_disk(idbzip, file.path(tempdir(), "idbzip.zip"))
  idb1yr <- readr::read_delim(unz(file.path(tempdir(), "idbzip.zip"), "idbsingleyear.txt"),
                              delim = "|", show_col_types = FALSE)

  # Get 1-year age-group data for all countries in the data set
  # We use population data from 2020 to match the study year of `contactdata`s contact matrices
  # US Census data uses their "GEO_ID" as geographical identifier. In this case, we only need the
  # country code (last two characters of GEO_ID)
  demography <- idb1yr |>
    dplyr::rename_with(tolower) |>
    dplyr::filter(`#yr` == 2020, .data$sex == 0) |>
    dplyr::transmute("key_country" = stringr::str_sub(geo_id, -2, -1),
                     .data$age,
                     "population" = .data$pop,
                     "proportion" = .data$pop / sum(.data$pop))

  # Project into 10-year age-groups
  swedens_population <- demography |>
    dplyr::filter(.data$key_country == "SE") |>
    dplyr::mutate(age_group = purrr::map_chr(demography$age, ~ age_labels[max(which(age_cuts <= .))])) |>
    dplyr::summarise(population = sum(population), .by = c("age_group"))


  # Store the data in the package
  attr(swedens_population, "creation_datetime") <- Sys.time()
  usethis::use_data(swedens_population, overwrite = TRUE)
}
