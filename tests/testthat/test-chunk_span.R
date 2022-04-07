test_that("chunk_span works", {

  good_years <- "https://api.data.abs.gov.au/data/ABS,LF,1.0.0/M9.3.1599.30+10+20.AUS.M?startPeriod=2010&endPeriod=2022&dimensionAtObservation=AllDimensions"
  bad_years <- "https://api.data.abs.gov.au/data/ABS,LF,1.0.0/M9.3.1599.30+10+20.AUS.M?startPeriod=2021&endPeriod=2021&dimensionAtObservation=AllDimensions"

  good_months <- "https://api.data.abs.gov.au/data/ABS,LF,1.0.0/M9.3.1599.30+10+20.AUS.M?startPeriod=2010-01&endPeriod=2022-12&dimensionAtObservation=AllDimensions"
  bad_months <- "https://api.data.abs.gov.au/data/ABS,LF,1.0.0/M9.3.1599.30+10+20.AUS.M?startPeriod=2010-02&endPeriod=2022-05&dimensionAtObservation=AllDimensions"

  no_end <-  "https://api.data.abs.gov.au/data/ABS,LF,1.0.0/M9.3.1599.30+10+20.AUS.M?startPeriod=2015-02&dimensionAtObservation=AllDimensions"

  c(good_years,
    bad_years,
    good_months,
    bad_months,
    no_end
    ) %>%
    purrr::map(chunk_span)

})


x <- read_abs_api(no_end)
sg(x)[[17]] %>% tail()


query_url <- "https://api.data.abs.gov.au/data/ABS,LF,1.0.0/M9.3.1599.30+10+20.AUS.M?startPeriod=2015-02&dimensionAtObservation=AllDimensions"

dbr_2020 <- readLines("dbr_2020.txt")

n <- c(100, 200)
n <- c(50, 100, 200)
# erp

# 800 failed
# 700 failed
# 600 was 5 minutes ()
# 500 was 6 minutes (169119 rows)
# 400 was 18 minutes (602295 rows)
# 300 was 19 minutes (602295 rows)
# 200 was 21 minutes (602295 rows)


n_range <- n %>%
  map(chunk_query_url,
      query_url = dbr_2020)

t100 <- stopwatch(try100 <- n_range[[1]] %>%
            map_dfr(read_abs_api,
                    batch_mode = TRUE))


t200 <- stopwatch(try200 <- n_range[[2]] %>%
                    map_dfr(read_abs_api,
                            batch_mode = TRUE))

t500 <- stopwatch(try500 <- n_range[[3]] %>%
                    map_dfr(read_abs_api,
                            batch_mode = TRUE))

t600 <- stopwatch(try600 <- n_range[[4]] %>%
                    map_dfr(read_abs_api,
                            batch_mode = TRUE))

t700 <- stopwatch(try700 <- n_range[[5]] %>%
                    map_dfr(read_abs_api,
                            batch_mode = TRUE))



try1
try2
