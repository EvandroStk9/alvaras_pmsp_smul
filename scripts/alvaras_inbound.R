library(rvest)
library(httr)
library(tidyverse)
library(here)
library(fs)

# 1. Importa --------------------------------------------------------------

#
url <- "https://www.prefeitura.sp.gov.br/cidade/secretarias/licenciamento/servicos/index.php?p=3334"

#
html <- read_html(url)
 
#  
urls <- html_attr(html_nodes(html, "a"), "href") %>%
  as_tibble() %>%
  filter(str_detect(value, ".xls$") &
           str_detect(value, regex("anual", ignore_case = TRUE))) %>%
  mutate(ano = map(value, ~str_extract(.x, "[-+]?[0-9]*\\.?[0-9]+")) %>% 
           as_vector() %>%
           if_else(is.na(.), format(Sys.Date(), "%Y"), .))

# 2. Exporta --------------------------------------------------------------

#
fs::dir_create(here("inputs", "1_inbound", "PMSPSMUL"))

#
map2(urls$value, urls$ano, ~download.file(.x,
                         file.path(fs::dir_create(here("inputs", "1_inbound", "PMSPSMUL", .y)),
                                   basename(.x))))

