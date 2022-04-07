#' Check if a \code{query_url} has specified start and end periods
#'
#' This is helpful for chunking up a long \code{query_url} into smaller
#' requests.
#'
#' @inheritParams read_abs_api
#'
#' @return (\code{logical}) \code{TRUE} if \code{query_url} has
#'   \code{startPeriod} and \code{endPeriod}
qu_has_span <- function(query_url) {
  components <- urltools::url_parse(query_url)

  c("startPeriod", "endPeriod") %>%
    purrr::map_lgl(stringr::str_detect,
                   string = components$parameter) %>%
    all()
}


#' What is the biggest dimension component in a \code{query_url}?
#'
#' This function returns the biggest dimension (ie the most verbose, so often
#' geography) in a \code{query_url}. This is helpful for chunking up a long
#' \code{query_url} into smaller requests.
#'
#' @inheritParams read_abs_api
#'
#' @return (\code{character}) the longest part of \code{query_url} between two
#'   \code{.}.
qu_biggest_dimesion <- function(query_url) {
  components <- urltools::url_parse(query_url)
  dims <- components$path %>%
    stringr::str_remove("data/ABS(.*)/") %>%
    stringr::str_split(pattern = "\\.") %>%
    unlist()

  ranks <- dims %>%
    purrr::map_int(nchar) %>%
    order() %>%
    rev()

  dims[ranks[1]]
}


#' Chunk a \code{query_url} into individual years
#'
#' One way to break up a \code{query_url} is by year. This function does that.
#'
#' @inheritParams read_abs_api
#'
#' @return (\code{character} vector) a vector of URLs, one for each year of data
#'   requested.
chunk_span <- function(query_url) {

  start_pat <- "(?<=startPeriod\\=)(\\d+[[:punct:]]*){4,7}(?=&)"
  start <- stringr::str_extract(query_url,
                                start_pat)

  has_months <- nchar(start) > 4

  has_span <- qu_has_span(query_url)

  # If the query_url doesn't have an end period, the API treats it as today.
  # So explicitly add that in
  if (!has_span) {

    this_year <- lubridate::year(Sys.Date())
    this_month <- lubridate::month(Sys.Date()) %>%
      stringr::str_pad(2, side = "left", pad = "0")

    explicit_end <- ifelse(has_months,
                           glue::glue("{start}&endPeriod={this_year}-{this_month}"),
                           glue::glue("{start}&endPeriod={this_year}"))

    query_url <- query_url %>%
      stringr::str_replace(start_pat,
                           explicit_end)
  }

  end_pat <- "(?<=endPeriod\\=)(\\d+[[:punct:]]*){4,7}(?=&)"

  end <- stringr::str_extract(query_url,
                              end_pat)

  # Build a tibble of start and end dates

  # Deal with months

  if (has_months) {

    start_year <- as.numeric(stringr::str_split(start, pattern = "-")[[1]][1])
    start_month <- as.numeric(stringr::str_split(start, pattern = "-")[[1]][2])
    end_year <- as.numeric(stringr::str_split(end, pattern = "-")[[1]][1])
    end_month <- as.numeric(stringr::str_split(end, pattern = "-")[[1]][2])

    first_end <- paste(start_year, 12, sep = "-")
    last_start <- paste(end_year, "01", sep = "-")
    mid_starts <- seq(start_year + 1, end_year - 1) %>%
      paste("01", sep = "-")
    mid_ends <- seq(start_year + 1, end_year - 1) %>%
      paste("12", sep = "-")

    new_spans <- tibble::tibble(
      new_start = c(start, mid_starts, last_start),
      new_end = c(first_end, mid_ends, end))

  } else {
    span <- seq(start, end) %>%
      as.character()

    new_spans <- tibble::tibble(
      new_start = span,
      new_end = span)
  }


  new_spans %>%
    dplyr::mutate(url = query_url %>%
                    stringr::str_replace(pattern = start_pat,
                                         replacement = new_start) %>%
                    stringr::str_replace(pattern = end_pat,
                                         replacement = new_end)) %>%
    dplyr::pull(url)
}


#' Chunk a \code{query_url} into requests for fewer dimensions
#'
#' One way to break up a \code{query_url} is by requesting fewer dimensions
#' (such as only a few SA2s as a time). This function does that for the
#' dimension with the most levels.
#'
#' @inheritParams read_abs_api
#' @param n (\code{numeric}; default = \code{300}) the number of dimension
#'   levels to request in each URL. Higher means fewer URLs (and thus faster),
#'   but increases the risk of the resulting URLs not being valid. Cursory
#'   testing indicates that more than 400 results in data loss (ie some URLs are
#'   still too big).
#'
#' @return (\code{character} vector) a vector of URLs, one for subgroup of the
#'   largest dimension.
chunk_big_dim <- function(query_url,
                          n = 300) {

  big_dims <- qu_biggest_dimesion(query_url)

  dims <- big_dims %>%
    stringr::str_split(pattern = "\\+") %>%
    unlist()

  new_dims <- split(dims, ceiling(seq_along(dims) / n)) %>%
    purrr::map_chr(paste0,
                   collapse = "+")

  new_dims %>%
    purrr::map_chr(stringr::fixed) %>%
    purrr::map_chr(stringr::str_replace,
                   string = query_url,
                   pattern = stringr::fixed(big_dims))
}


#' Break up a \code{query_url} into chunks, by date and dimension
#'
#' Some URLs provided by the ABS' API are too long to actually work. This
#' function breaks up a \code{query_url} into multiple smaller ones. It does
#' this by looking at the time period the request covers, and separating it into
#' individual years, and by looking at the dimension with the most levels
#' (usually a geography) and creating URLs that only request a smaller number of
#' levels.
#'
#' @inheritParams chunk_big_dim
#' @return (\code{character} vector) a \code{character} vector of URLs for the
#'   ABS API
#' @export
#'
#' @examples
#' \dontrun{
#' # This URL covers multiple years of data
#' lf_url <- "https://api.data.abs.gov.au/data/ABS,LF,1.0.0/M9.3.1599.30+10+20.AUS.M?startPeriod=2010-02&endPeriod=2022-05&dimensionAtObservation=AllDimensions"
#'
#' }
chunk_query_url <- function(query_url, n = 100) {
  # only called if the big request fails
  # if there is a span, chunk it up into single years
  # need to consider if the span is years or year mons
  # returns vector of urls

  query_url <- chunk_span(query_url)


  purrr::map(query_url,
             chunk_big_dim,
             n = n) %>%
    unlist() %>%
    purrr::set_names(NULL)

}
