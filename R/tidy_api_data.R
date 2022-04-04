#' Tidy the raw data returned using the ABS' API
#'
#' This function gets the nested list of raw data returned using the API URL and
#' transforms it into into a tidy tibble based on the structure of the API URL
#' (i.e. whether it follows the old or new structure, as stated in the
#' \code{old_api} parameter).
#'
#' @inheritParams read_abs_api
#'
#' @return (tibble) Returns a tibble containing the ABS data you queried.
#'
tidy_api_data <- function(
  .data,
  structure_url
) {

  # get an id
  rd2 <- .data %>%
    dplyr::mutate(row = dplyr::row_number()) %>%
      janitor::clean_names() %>%
      dplyr::rename_with(.fn = ~ paste0(., "_code"))

  values <- rd2 %>%
    dplyr::transmute(.data$row_code,
      value_code = readr::parse_number(.data$obs_value_code),
      time_period_code = .data$time_period_code
    )

  # get the structure info
  structure_data <- readsdmx::read_sdmx(structure_url) %>%
    tibble::as_tibble()

  # oh fuck you ABS
  # TODO CL_STATE in id is the colname REGION
  # TODO TIME_PERIOD is a col name but not in the id column
  # TODO FREQUENCY? just missing some times

  measures <- colnames(.data)
  val_loc <- which(measures == "ObsValue")

  # ## MANUALLY ADD IN STATE if REGION detected
  # got_region <- "REGION" %in% measures
  # # CHECK FOR STATE?
  # if (got_region) {
  #   rlang::warn("There's a region column here (which is going to be renamed to state), lookout!")
  # }


  # could pull the last bit from id
  details <- structure_data %>%
    # The ABS uses REGION as a column name but CL_STATE as the dictionary key
    dplyr::mutate(
      id = dplyr::case_when(
        .data$id == "CL_STATE" ~ "REGION",
        TRUE ~ .data$id)
      ) %>%
    dplyr::mutate(id = stringr::str_extract(.data$id,
                                                  pattern = stringr::str_c(measures, collapse = "|"))) %>%

    dplyr::filter(.data$id %in% measures) %>%
    dplyr::select(-.data$agencyID,
                  -.data$version,
                  -.data$isFinal)

  clean_up <- function(.data) {
    clean_data <- .data %>%
      dplyr::rename(!!.$id[[1]] := id_description,
                    !!.$en[[1]] := en_description) %>%
      dplyr::select(
        -en,
        -id
      )

      # make new name vector
      new_names <- colnames(clean_data)[[2]] %>%
        stringr::str_remove(pattern = "_code$") %>%
        paste0(., c("_name", "_code"))


      clean_data %>%
        dplyr::rename_with(.fn = ~new_names)

  }


all_the_details <- details %>%
  dplyr::mutate(id = paste0(tolower(id), "_code")) %>%
    split(.$id) %>%
    purrr::map(clean_up)



mini_join <- function(.data,
                      details,
                      var) {
  .data %>%
    dplyr::select(.data$row_code, !!var) %>%
    dplyr::left_join(details) %>%
    dplyr::select(-!!var)

}

# Using suppressMessages because of all the different joining_by messages in the
# reduce
suppressMessages(
  purrr::imap(all_the_details,
    .f = mini_join,
    .data = rd2
  ) %>%
    purrr::reduce(
      dplyr::inner_join,
      by = "row_code"
    ) %>%
    dplyr::inner_join(rd2) %>%
    dplyr::rename(
      value = .data$obs_value_code,
    ) %>%
    dplyr::select(-.data$row_code) %>%
    dplyr::select(sort(colnames(.))) %>%
    dplyr::relocate(
      .data$value,
      .before = 1
    )
)


}
