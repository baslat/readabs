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

  # Clean the raw data names, and add an ID
  rd <- .data %>%
    dplyr::mutate(row = dplyr::row_number()) %>%
    janitor::clean_names() %>%
    # Need to rename region to state because the data dictionary uses region
    dplyr::rename("state" = dplyr::contains("region"))

  # Suffix the columns with '_code'
  rd2 <- rd %>%
    dplyr::rename_with(.fn = ~ paste0(., "_code"))

  # Extract the values and row ids
  values <- rd2 %>%
    dplyr::transmute(
      .data$row_code,
      value_code = .data$obs_value_code)

  # get the structure info
  struc_raw <- rsdmx::readSDMX(structure_url)

  # Bunch of convoluted work to tidy up the structure (Data Structure Definition (DSD))
  codes_raw <- slot(struc_raw, "codelists")
  codes <- sapply(slot(codes_raw, "codelists"), function(x) slot(x, "id")) #get list of codelists



  standardise_measure <- function(measure, all_measures) {
    measure %>%
      tolower() %>%
      stringr::str_remove("CL_") %>%
      stringr::str_extract(pattern = stringr::str_c(all_measures,
                                                    collapse = "|"))
  }



  tidy_codes <- function(code, structure_data, all_measures) {
    clean_measure <- standardise_measure(code, all_measures = all_measures)
    raw <- as.data.frame(slot(structure_data, "codelists"), codelistId = code)  %>% #get a codelist
      dplyr::as_tibble()



   names_descs_labels <- raw %>%
      select(-id) %>%
      colnames()

   raw %>%
      dplyr::mutate(full_label = dplyr::coalesce(!!!syms(names_descs_labels))) %>%
      dplyr::select(
        id = id,
        label = full_label,
        notes = dplyr::contains("desc")) %>%
     dplyr::mutate(code = clean_measure)

  }


  # Still has the stupid region/state problem
  all_measures <- colnames(rd)

  details <- codes %>%
    purrr::set_names() %>%
    purrr::map_dfr(tidy_codes,
                   structure_data = struc_raw,
                   all_measures = all_measures) %>%
    dplyr::filter(.data$code %in% all_measures)



  clean_up_data_dict <- function(.data) {

    new_names <- .data$code[[1]] %>%
      paste0(c("_code", "_name", "_notes"))


    clean <- .data %>%
      dplyr::select(-.data$code) %>%
      dplyr::rename_with(~new_names)


    drop_desc <- sum(is.na(clean[[3]])) == nrow(clean)

    if (drop_desc) {
      clean <- dplyr::select(clean,
                             -dplyr::contains("notes"))
    }

    clean
  }


all_the_details <- details %>%
  dplyr::mutate(code = tolower(code)) %>%
    split(.$code) %>%
    purrr::map(clean_up_data_dict) %>%
  purrr::set_names(~paste0(., "_code"))



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
