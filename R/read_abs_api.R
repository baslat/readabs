#' Get tidy data from ABS
#'
#' This function returns a tidy tibble of flat-view and JSON format data queried
#' using the ABS' API URL. It first identifies whether the URL entered uses the
#' ABS' old or new API URL structure. If the URL is super long, it's also passed
#' through the \code{api_split_url} function, which splits up the large URL into
#' a combination of smaller URLs to increase efficiency. The resulting URL/s
#' is/are then passed through \code{tidy_api_data}, which returns a tidy tibble
#' of your data.
#'
#' @param query_url (character) the
#' @param raw (logical; default = \code{FALSE})
#' @param structure_url (character; default = \code{NULL})
#'
#' @return (tibble) Returns a tidy \code{tibble} containing the ABS data you queried.
#' @export
#'
#' @examples
#' \dontrun{
#' ### Get tidy dataset from a large URL (4087+ characters):
#' # First copy the URL
#' url <- abs_clipboard()
#' tidy_data <- read_abs_api(url)
#'
#' ### Get tidy ALC dataset using the ABS.Stat structure:
#' old_url <- paste0("http://stat.data.abs.gov.au/sdmx-json/data/ALC/",
#'  "all/ABS?startTime=2010&endTime=2016&detail=DataOnly",
#'   "&dimensionAtObservation=AllDimensions")
#' tidy_data <- read_abs_api(old_url)
#' tidy_data
#' }
read_abs_api <- function(
  query_url,
  structure_url = NULL) {

  raw_dat <- readsdmx::read_sdmx(query_url) %>%
    tibble::as_tibble()

# if (raw) {
#   return(raw_dat)
# }

  structure_url <- structure_url %||% guess_structure_url(query_url)

  tidy_api_data(
    .data = raw_dat,
    structure_url = structure_url
  )

  # same_size <- all(dim(raw_dat) == dim(cleaned))
  # if (!same_size) {
  #   "The cleaned data isn't the same shape as the raw data. This usually means that a value in the data dictionary (i.e. what is returned by the `structure_url`) doesn't exactly match a column name (e.g. dictionary uses 'state' and data column is 'region'), or the ABS left something out of the data dictionary (e.g. 'Frequency' is in the data but not the dictionary). Change the `raw` argument to `TRUE` and take a look at what's different." %>%
  #     stringr::str_wrap(80) %>%
  #     rlang::warn()
  # }


}

#' Guess the structure URL for the API
#'
#' The structure URL will return information about how certain variables are
#' encoded. This function can guess what it should be, given a query URL.
#'
#' @inheritParams read_abs_api
#'
#' @return a URL string
guess_structure_url <- function(query_url) {

  components <- urltools::url_parse(query_url)
  # take the path, and the bit in between the /, and swap the , for /, and add dataflow
  new_path <- components$path %>%
    stringr::str_extract("(?<=/)(.*)(?=/)") %>%
    stringr::str_replace_all(",", "/") %>%
    paste0("dataflow/", .)

  components$path <- new_path
  components$parameter <- "references=all&detail=referencepartial"

  urltools::url_compose(components)
}



structure_join <- function(
  .data,
  structure,
  measure
) {
  prep <- structure %>%
    dplyr::filter(id == measure) %>%
    dplyr::select(!!measure := id_description,
                  !!.$en[[1]] := en_description)

  .data %>%
    dplyr::left_join(prep) %>%
    dplyr::select(-!!measure)
}
