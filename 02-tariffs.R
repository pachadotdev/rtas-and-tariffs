library(purrr)
library(readr)
library(dplyr)
library(tidyr)
library(stringr)
library(arrow)

if (!dir.exists("mfn")) {
  unzip("MFN_APPLIED_RATES.ZIP")
  file.rename("MFN", "mfn")
  fzip <- list.files("mfn", full.names = T, pattern = "zip")
  for (x in fzip) {
    unzip(x, exdir = "mfn")
  }
}

if (!file.exists("mfn/year=2002")) {
  fcsv <- list.files("mfn", pattern = "CSV$", full.names = T)
  
  tariffs <- map_df(
    fcsv, 
    function(x) {
      read_csv(x, 
               col_types = cols(
                 Year = col_character(),
                 Reporter_ISO_N = col_character(),
                 ProductCode = col_character(),
                 Sum_Of_Rates = col_character(),
                 Min_Rate = col_character(),
                 Max_Rate = col_character(),
                 SimpleAverage = col_character(),
                 Nbr_NA_Lines = col_character(),
                 Nbr_Free_Lines = col_character(),
                 Nbr_AVE_Lines = col_character(),
                 Nbr_Dutiable_Lines = col_character(),
                 TotalNoOfValidLines = col_character(),
                 TotalNoOfLines = col_character()
               )
      ) %>% 
        mutate(filename = x)
    }
  )
  
  gc()
  
  tariffs %>% 
    filter(Reporter_ISO_N == "784", ProductCode == "010310")
  
  tariffs <- tariffs %>% 
    janitor::clean_names() %>% 
    mutate_if(is.character, str_trim) %>% 
    mutate(
      reporter_iso_n = as.integer(reporter_iso_n),
      year = as.integer(year),
      sum_of_rates = as.numeric(sum_of_rates),
      min_rate = as.numeric(min_rate),
      max_rate = as.numeric(max_rate),
      simple_average = as.numeric(simple_average),
      nbr_na_lines = as.numeric(nbr_na_lines),
      nbr_free_lines = as.numeric(nbr_free_lines),
      nbr_ave_lines = as.numeric(nbr_ave_lines),
      nbr_dutiable_lines = as.numeric(nbr_dutiable_lines),
      total_no_of_valid_lines = as.numeric(total_no_of_valid_lines),
      total_no_of_lines = as.numeric(total_no_of_lines)
    ) %>% 
    filter(year >= 2002)
  
  tariffs <- tariffs %>% 
    # select(filename) %>% 
    # filter(row_number() <= 10) %>% 
    mutate(
      filename = sub(".*(_[^_]+_)", "\\1", filename),
      filename = sub("(_[^_]+_).*", "\\1", filename),
      filename = gsub("_", "", filename),
      reporter_iso3 = tolower(filename)
    ) %>% 
    select(-filename)
  
  load("../comtrade-codes/01-2-tidy-country-data/country-codes.RData")
  
  country_codes <- country_codes %>%
    select(country_code, iso3_digit_alpha) %>%
    mutate(iso3_digit_alpha = tolower(iso3_digit_alpha))
  
  country_codes_hs02 <- open_dataset(
    "../uncomtrade-datasets-arrow/hs-rev2002/parquet/",
    partitioning = c("aggregate_level", "trade_flow", "year", "reporter_iso")
  )
  
  country_codes_hs02_2 <- tibble()
  
  country_codes_hs02_2 <- map_df(
    2002:2020,
    function(t) {
      country_codes_hs02_2 %>% 
        bind_rows(
          country_codes_hs02 %>% 
            filter(aggregate_level == "aggregate_level=0", 
                   trade_flow == "trade_flow=export",
                   year == paste0("year=",t)) %>% 
            select(reporter_code, reporter_iso) %>% 
            collect()
        ) %>% 
        distinct()
    }
  )
  
  country_codes_hs02_2 <- country_codes_hs02_2 %>% 
    mutate(
      reporter_iso = gsub("reporter_iso=", "", reporter_iso)
    ) %>% 
    distinct()
  
  tariffs %>% 
    select(reporter_iso3) %>% 
    distinct() %>% 
    filter(!reporter_iso3 %in% country_codes$iso3_digit_alpha) %>% 
    View()
  
  tariffs %>% 
    select(reporter_iso3) %>% 
    distinct() %>% 
    filter(!reporter_iso3 %in% country_codes_hs02_2$reporter_iso) %>% 
    View()
  
  # see https://en.wikipedia.org/wiki/Country_codes:_A etc
  # https://en.wikipedia.org/wiki/Country_codes_of_Serbia
  fix_iso_codes <- function(val) {
    case_when(
      val == "rom" ~ "rou", # Romania
      val == "tmp" ~ "tls", # East Timor
      val == "ser" ~ "srb", # Serbia
      val == "sud" ~ "sdn", # Sudan
      val == "zar" ~ "cod", # Congo (Democratic Republic of the)
      TRUE ~ val
    )
  }
  
  tariffs <- tariffs %>% 
    mutate(reporter_iso3 = fix_iso_codes(reporter_iso3))
  
  tariffs <- tariffs %>% 
    select(nomen_code, reporter_iso3, everything()) %>% 
    select(-reporter_iso_n)
  
  tariffs <- tariffs %>% 
    arrange(year, reporter_iso3)
  
  # H0: HS92
  # H1: HS96
  # H2: HS02
  # H3: HS07
  # H4: HS12
  # H5: HS17
  
  unique(tariffs$nomen_code)
  
  tariffs <- tariffs %>% 
    group_by(nomen_code) %>% 
    nest() %>% 
    arrange(nomen_code)
  
  load("../comtrade-codes/02-2-tidy-product-data/product-correlation.RData")
  
  product_correlation <- product_correlation %>%
    select(hs92:hs17) %>%
    select(hs07, everything()) %>% 
    arrange(hs07) %>%
    pivot_longer(hs92:hs17, "equivalence") %>% 
    mutate(
      equivalence = case_when(
        equivalence == "hs92" ~ "H0",
        equivalence == "hs96" ~ "H1",
        equivalence == "hs02" ~ "H2",
        # equivalence == "hs07" ~ "H3",
        equivalence == "hs12" ~ "H4",
        equivalence == "hs17" ~ "H5",
      )
    ) %>% 
    arrange(hs07, equivalence) %>% 
    group_by(equivalence) %>% 
    nest()
  
  tariffs <- map_df(
    paste0("H", 0:5),
    function(h) {
      d1 <- tariffs %>% 
        filter(nomen_code == h) %>% 
        unnest(cols = data)
      
      if (h != "H3") {
        d2 <- product_correlation %>% 
          filter(equivalence == h) %>% 
          unnest(cols = data) %>% 
          rename(
            product_code = value,
            hs07_product_code = hs07
          ) %>% 
          ungroup() %>% 
          select(-equivalence) %>% 
          distinct()
        
        d1 <- d1 %>% 
          left_join(d2) %>% 
          ungroup() %>% 
          select(-nomen_code, -product_code) %>% 
          select(year, reporter_iso3, hs07_product_code, everything())
      } else {
        d1 <- d1 %>% 
          ungroup() %>% 
          select(-nomen_code) %>% 
          rename(commodity_code = product_code) %>% 
          select(year, reporter_iso3, commodity_code, everything())
      }
      
      return(d1 %>% distinct())
    }
  )
  
  tariffs <- tariffs %>% 
    arrange(year, reporter_iso3)
  
  # this is ok because of the n:1 map from hsxx -> hs07
  # duplications <- map_df(
  #   2002:2020,
  #   function(t) {
  #     message(t)
  #     tariffs %>%
  #       filter(year == t) %>% 
  #       select(year, reporter_iso3, hs07_product_code) %>%
  #       group_by(year, reporter_iso3, hs07_product_code) %>%
  #       count() %>%
  #       filter(n > 1)
  #     
  #   }
  # )
  
  # 41 products match to 1 product here
  # product_correlation %>% 
  #   filter(equivalence == "H0") %>% 
  #   unnest(data) %>% 
  #   filter(hs07 == "285200") %>% 
  #   distinct(value, .keep_all = T)
  
  tariffs <- tariffs %>%
    distinct()
  
  # write_parquet(tariffs, "mfn/mfn_applied_rates.parquet")
  # tariffs <- read_parquet("mfn/mfn_applied_rates.parquet")
  
  tariffs %>% 
    rename(reporter_iso = reporter_iso3) %>% 
    group_by(year, reporter_iso) %>% 
    write_dataset("mfn", partitioning = c("year", "reporter_iso"), 
                  hive_style = T)
  
  file_rem <- list.files("mfn", pattern = "CSV$|zip$|xml$", full.names = T)
  for (x in file_rem) file.remove(x)
}
