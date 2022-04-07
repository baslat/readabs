#' Get tidy data from ABS' API
#'
#' This function returns a tidy \code{tibble} of data queried from the
#' \href{https://explore.data.abs.gov.au/}{ABS' API}. See the vignette for a
#' detailed guide on how to use this to get monthly labour force data or
#' download the last decade of Data by Region at SA2.
#'
#' @param query_url (character) the 'Data query' URL from the 'Developer API'
#'   section from the \href{https://explore.data.abs.gov.au/}{ABS' API}. I'm not
#'   sure what the difference between 'Flat' and 'Time series' is, because even
#'   though the URLs are different they return the same data.
#' @param structure_url (character; default = \code{NULL}) the 'Structure query'
#'   URL from the same place. This provides the information for the data
#'   dictionary. If \code{NULL} it will be guessed from \code{query_url}.
#' @param verbose (logical; default = \code{TRUE}) print status update messages?
#' @param check (logical; default = \code{TRUE}) check the URL can be queried?
#'   Some URLs are too long to be queried, so this can provide some advice on
#'   how to deal with them (at the expense of speed).
#'
#' @return (\code{tibble}) Returns a tidy \code{tibble} containing the ABS data
#'   you queried. Each \code{tibble} returns a column \code{value} along with a
#'   series of other columns for the metadata (suffixed \code{_code},
#'   \code{_name}, and \code{_notes}). Some metadata returns just one suffixed
#'   column, others two, and others three (eg querying labour force data returns
#'   \code{sex_code} and \code{sex_name}, but \code{age_code}, \code{age_name}
#'   and \code{age_notes}. Generally, the \code{_code} column shows the encoded
#'   value (eg \code{sex_code} might show a value of \code{3}), the \code{_name}
#'   shows a human interpretable translation of \code{_code} (eg \code{sex_name}
#'   would show \code{Persons}), and \code{_notes} is a bit of a mixed bag.
#'   Sometimes it's missing, sometimes it's the same as \code{_name}, and
#'   sometimes it's useful context.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' # Labour force data query URL, taken from ABS API site
#' lf_url <- "https://api.data.abs.gov.au/data/ABS,LF,1.0.0/M9.3.1599.30+10+20.AUS.M?startPeriod=2021-01&dimensionAtObservation=AllDimensions"
#'
#' lf <- read_abs_api(lf_url)
#' }
read_abs_api <- function(
  query_url,
  structure_url = NULL,
  verbose = TRUE,
  check = TRUE,
  batch_mode = FALSE) {

  # Arg checks
  stopifnot(is.character(query_url))
  if (check) {
    if (verbose) {
      message("Checking the URL is reachable...")
    }
    ok <- qu_ok(query_url)
    if (batch_mode & !ok) {
      if (verbose) {
        message("No data there but moving forward because of `batch_mode`.")
      }
      return(tibble::tibble())
    }
    if (!ok) {
      stop(attr(ok, "msg"),
           call. = FALSE)
    }


  }

  # Get data
  if (verbose) {
    res <- httr::GET(query_url) %>%
      httr::content()

    desc <- res$structure$name
    glue::glue("Querying the raw data for '{desc}'...") %>%
      message()
  }



  raw_dat <- tibble::as_tibble(rsdmx::readSDMX(query_url))

  structure_url <- structure_url %||% guess_structure_url(query_url)
  if (verbose) {
    message("Querying the data dictionary and matching it to the raw data...")
  }

  # Tidy data
  tidy_api_data(
    .data = raw_dat,
    structure_url = structure_url
  ) %>%
    tibble::as_tibble()
}

#' Guess the structure URL for the API
#'
#' The structure URL will return information about how certain variables are
#' encoded. This function can guess what it should be, given a query URL.
#'
#' @inheritParams read_abs_api
#'
#' @return (\code{character}) a URL string
guess_structure_url <- function(query_url) {

  components <- urltools::url_parse(query_url)
  # take the path, and the bit in between the /, and swap the , for /, and add
  # dataflow
  new_path <- components$path %>%
    stringr::str_extract("(?<=/)(.*)(?=/)") %>%
    stringr::str_replace_all(",", "/") %>%
    paste0("dataflow/", .)

  components$path <- new_path
  components$parameter <- "references=all&detail=referencepartial"

  urltools::url_compose(components)
}


#' Superficial check if a \code{query_url} is reachable
#'
#' Some URLs provided by the ABS' API are actually duds, so it can help to check
#' if they can be reached. This does that by calling \code{httr::GET()},
#' checking the status response, and printing a (hopefully) helpful error
#' message.
#'
#' @inheritParams read_abs_api
#'
#' @return (\code{logical}) \code{TRUE} if the status code is 200, \code{FALSE}
#'   otherwise (with an error message printed to the console)
#'
qu_ok <- function(query_url) {
  res <- httr::GET(query_url)

  all_good <- res$status_code == 200

  # Early return if all good
  if (all_good) {
    return(TRUE)
  }

  error_msg <- httr::content(res, as = "text", encoding = "utf8") %>%
    stringr::str_remove_all(pattern = "<.*?>") %>%
    stringr::str_replace_all(pattern = "\n+",
                             replacement = "\n")

  msg <- glue::glue("`query_url` cannot be queried at this time (the ABS might be down or the URL requests too much data).
             The error message from the ABS is:
             {error_msg}
             You could also try running `chunk_query_url(query_url)` and iterating `read_abs_api()` over that list of URLs (this often helps if you get a 500 or 414 error).")

  attr(all_good, "msg") <- msg
  all_good

}


