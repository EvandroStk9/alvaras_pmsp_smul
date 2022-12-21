library(fs)
library(here)
library(sf)
library(tidyverse)
library(tidylog)
library(janitor)
library(tidygeocoder)
library(sfarrow)

# 1. Importa --------------------------------------------------------------

#
alvaras <- arrow::read_parquet(here("inputs", "3_trusted", "Alvaras",
                                      "alvaras.parquet"))

#
alvaras_por_lote <- arrow::read_parquet(here("inputs", "3_trusted", "Alvaras",
                                               "alvaras_por_lote.parquet")) %>%
  filter(ind_edificacao_nova == TRUE) %>%
  filter(ind_aprovacao == TRUE & ind_execucao == TRUE) %>%
  mutate(sql_ajust = case_when(nchar(sql_incra) == 14 ~ str_remove_all(sql_incra, "\\.") %>%
                                 str_remove_all("-") %>%
                                 str_sub(end = -2L),
                               nchar(sql_incra) <= 13 ~ str_remove_all(sql_incra, "\\.") %>%
                                 str_remove_all("-") %>%
                                 str_remove_all("x")))

# 55 sql_ajust aparecem duplicados
# todos contendo uma informação de 2004 que foi setada com "-x" no dígito verificador
alvaras_por_lote %>%
  get_dupes(sql_ajust) 

# Correspondência deve acontecer com alvarás agrupados por enderçeo
# java_alvaras_por_endereco <- st_read(here("inputs", "1_inbound", "Abrainc", "Alvaras",
#                                           "ArquivosOriginais", "Agrupados",
#                                           "20220530_pmsp_smul_licenciamento_alvaras_agrupamento.shp")) %>%
#   mutate(
#     cod_sql = case_when(str_detect(cod_sql, pattern = "\\+1") ~ NA_character_, # Mesmo tratamento feito em R
#                         nchar(cod_sql) == 17 ~ str_sub(cod_sql, start = 4L),
#                         nchar(cod_sql) == 16 ~ paste0(str_sub(cod_sql, start = 4L), "x"),
#                         nchar(cod_sql) == 14 ~ cod_sql,
#                         nchar(cod_sql) == 11 ~ cod_sql,
#                         TRUE ~ cod_sql),
#     ind_sql_incra_null = if_else(str_detect(cod_sql, "^(\\d)\\1{6,}"), 
#                                  TRUE, FALSE)) # se repete 6 vezes+ desde o início

# Correspondência também pode acontecer com alvarás não-agrupados
# java_alvaras <- st_read(here("inputs", "1_inbound", "Abrainc", "Alvaras",
#                              "ArquivosOriginais", "Individualizados",
#                              "20220530_pmsp_smul_licenciamento_alvaras.shp"))

# Uso da base de lotes da PMSP - via Geosampa
dir_tree(here("inputs", "1_inbound", "Lotes"))

#
CRS <- "+proj=utm +zone=23 +south +ellps=WGS84 +datum=WGS84 +units=m +no_defs"

#
list_lotes <- list.dirs(here("inputs", "1_inbound", "Lotes")) %>%
  set_names(map_chr(., ~ str_extract(.x, "[0-9].*"))) %>%
  map(~ list.files(.x, pattern = ".shp$", full.names = TRUE)) %>%
  compact() %>%
  map(st_read) %>%
  map_if(~ is.na(st_crs(.x)), ~ st_set_crs(.x, value = CRS)) %>%
  map(~ st_transform(.x, crs = 4674))

#
lotes <- bind_rows(list_lotes) %>%
  mutate(sql_ajust = paste0(lo_setor, lo_quadra, lo_lote))

#
glimpse(lotes)

# Para checagem da geocodificação
sf_geo_sp <- st_read(here("inputs", "1_inbound",
                          "ArqFuturo", "MapaBase", "Subprefeituras",
                          "SubprefectureMSP_SIRGAS.shp")) %>%
  st_set_crs(CRS) %>%
  st_transform(crs = 4674)

# 2. Geocodifica 1/3 -----------------------------------------------------------

# 1/3 - Geocode por herança de cadastro governamental oficial (GeoSampa)

# 4692 (33.5%) lotes encontrados na base de Lotes/SQL's oficial de São Paulo
join_lotes <- lotes %>%
  select(sql_ajust) %>%
  inner_join(alvaras_por_lote %>%
               select(sql_incra, sql_ajust, endereco_ultimo) %>%
               filter(!is.na(sql_ajust)), by = c("sql_ajust"))

# 94 alvaras com 2 ou 3 lat-long de lotes correspondentes
# Questão parece ser o dígito verificador e existência de mesmo código p/ diferentes subprefeituras
join_lotes %>%
  janitor::get_dupes(sql_ajust) %>%
  st_drop_geometry() %>%
  summarize(n = n(), n_distinct = n_distinct(sql_incra))

# 9304 lotes sem correspondência 
anti_lotes <- alvaras_por_lote %>%
  anti_join(lotes, by = c("sql_ajust"))

# 94 duplicatas
duplicatas_lotes <- join_lotes %>%
  get_dupes("sql_ajust") %>%
  st_drop_geometry() %>%
  select(sql_incra) %>%
  filter(!duplicated(sql_incra))

# 4692 lotes distintos (33.5%) e 4583 sem nenhuma duplicata (32.7%)
geo_alvaras_por_lote_1 <- join_lotes %>%
  filter(!sql_incra %in% c(duplicatas_lotes$sql_incra)) %>%
  st_filter(st_centroid(.), .) # tomando o centroide p/ transformar de polígono para ponto

# 3. Geocodifica 2/3 -----------------------------------------------------------

# 2/3 - Geocode por herança do tratamento Java/Abrainc

# Caso 1: correspondência direta entre o mesmo nível de agregação
# join_java_1 <- java_alvaras_por_endereco %>%
#   filter(ind_sql_incra_null == FALSE) %>% # excluído sql_incra inválido
#   transmute(sql_incra = cod_sql) %>%
#   inner_join(alvaras_por_lote %>% select(sql_incra, sql_ajust, endereco_ultimo), by = "sql_incra")

# Caso 2: correspondência direta entre diferentes níveis de agregação
# anti_java <- alvaras_por_lote %>%
#   select(sql_incra, sql_ajust, endereco_ultimo) %>%
#   anti_join(java_alvaras_por_endereco %>% # Estão na rotina R mas não estão na rotina Java
#               transmute(sql_incra = cod_sql),  by = "sql_incra")

# 
# join_java_2 <- java_alvaras %>%
#   transmute(sql_incra = case_when(str_detect(cod_sql, pattern = "\\+1") ~ NA_character_, # Mesmo tratamento feito em R
#                                   nchar(cod_sql) == 17 ~ str_sub(cod_sql, start = 4L),
#                                   nchar(cod_sql) == 16 ~ paste0(str_sub(cod_sql, start = 4L), "x"),
#                                   nchar(cod_sql) == 14 ~ cod_sql,
#                                   nchar(cod_sql) == 11 ~ cod_sql,
#                                   TRUE ~ cod_sql)) %>%
#   right_join(anti_java, by = "sql_incra") %>%
#   filter(duplicated(sql_incra)) %>%
#   filter(!is.na(sql_incra))

# Caso 3: correspondência entre sql_incra anulado e geocode por endereço
# join_java_3 <- java_alvaras_por_endereco %>%
#   filter(ind_sql_incra_null == TRUE) %>%
#   transmute(endereco = str_squish(endereco)) %>%
#   inner_join(alvaras_por_lote %>% 
#                select(sql_incra, sql_ajust, endereco_ultimo, endereco_registro),
#              by = c("endereco" = "endereco_registro")) %>%
#   select(-endereco)

#
# join_java <- bind_rows(
#   join_java_1,
#   join_java_2,
#   join_java_3
# )

#
# duplicatas_java <- join_java %>%
#   get_dupes("sql_incra") %>%
#   st_drop_geometry() %>%
#   select(sql_incra) %>%
#   filter(!duplicated(sql_incra))

# 6506 de 13996 (46.5%)
# geo_alvaras_por_lote_2 <- join_java %>%
#   filter(!sql_incra %in% c(geo_alvaras_por_lote_1$sql_incra)) %>%
#   filter(!sql_incra %in% c(duplicatas_java$sql_incra)) %>%
#   st_transform(crs = 4674) %>% # CRS original é diferente
#   st_filter(sf_geo_sp) # Alguns pontos estão fora de São Paulo

# 6506  
  
# 4. Geocodifica 3/3 -----------------------------------------------------------

# 3/3 - Geocode pela API do ArcGis

# 890 sem geocode por herança de outra entidade
alvaras_por_lote_no_geo <- alvaras_por_lote %>%
  filter(!sql_incra %in% c(geo_alvaras_por_lote_1$sql_incra)) #%>%
  # filter(!sql_incra %in% c(geo_alvaras_por_lote_2$sql_incra))


# API comunidade ArcGIS: https://developers.arcgis.com/rest/
# geocode_arcgis <- tidygeocoder::geocode(
#   alvaras_por_lote_no_geo %>%
#     mutate(endereco_key = if_else(!is.na(bairro),
#                                     paste0(endereco_ultimo, " ", bairro, " SAO PAULO SP"),
#                                     paste0(endereco_ultimo, " SAO PAULO SP")) %>%
#              str_remove_all(";|_|/") %>%
#              str_squish(),
#            city = "São Paulo",
#            state = "SP"),
#   address = "endereco_key",
#   method = "osm"
# )

# Retorno de 890 de 890 (100%)
# Todos dentro do perímetro de São Paulo 
# sf_geocode_arcgis <- geocode_arcgis %>%
#   filter(!is.na(lat) | !is.na(long)) %>%
#   st_as_sf(coords = c("long", "lat"),
#            crs = "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs") %>% # 4326 CRS
#   st_transform(crs = 4674) %>%
#   st_filter(sf_geo_sp)


# API HERE
geocode_here <- tidygeocoder::geocode(
  # alvaras_por_lote_no_geo %>%
  #   mutate(endereco_key = if_else(!is.na(bairro),
  #                                 paste0(endereco_ultimo, " ", bairro, " SAO PAULO SP"),
  #                                 paste0(endereco_ultimo, " SAO PAULO SP")) %>%
  #            str_remove_all(";|_|/") %>%
  #            str_squish()),
  alvaras_por_lote_no_geo %>%
    mutate(endereco_key = paste0(endereco_ultimo, " SAO PAULO SP") %>%
             str_remove_all(";|_|/") %>%
             str_squish()),
  address = "endereco_key",
  method = "here"
)

#
sf_geocode_here <- geocode_here %>%
  filter(!is.na(lat) | !is.na(long)) %>%
  st_as_sf(coords = c("long", "lat"),
           crs = "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs") %>% # 4326 CRS
  st_transform(crs = 4674) %>%
  st_filter(sf_geo_sp)

#
plot_map_here <- ggplot(data = sf_geocode_here) +
  geom_sf(data = sf_geo_sp) +
  geom_sf(aes(fill = fct_rev(categoria_de_uso_grupo), label = id, size = area_do_terreno)) +
  labs(fill = "Categoria de uso") +
  theme_bw()

#
plotly::ggplotly(plot_map_here)

#
geo_alvaras_por_lote_3 <- sf_geocode_here %>%
  select(sql_ajust, sql_incra, endereco_ultimo)

# 5. Unifica tabelas geocodificadas em uma -------------------------------------

#
all.equal(
  order(names(geo_alvaras_por_lote_1)),
  # order(names(geo_alvaras_por_lote_2)),
  order(names(geo_alvaras_por_lote_3))
)

# Tipos de geometria devem ser idênticos (pontos)
all.equal(
  all(st_geometry_type(geo_alvaras_por_lote_1) == "POINT"),
  # all(st_geometry_type(geo_alvaras_por_lote_2) == "POINT"),
  all(st_geometry_type(geo_alvaras_por_lote_3) == "POINT"),
)

# Sistemas CRS devem ser idênticos
all.equal(
  st_crs(geo_alvaras_por_lote_1),
  # st_crs(geo_alvaras_por_lote_2),
  st_crs(geo_alvaras_por_lote_3)
)

# Todos os pontos devem estar dentro do perímetro de São Paulo
all.equal(
  length(st_filter(geo_alvaras_por_lote_1, sf_geo_sp)) == length(geo_alvaras_por_lote_1),
  # length(st_filter(geo_alvaras_por_lote_2, sf_geo_sp)) == length(geo_alvaras_por_lote_2),
  length(st_filter(geo_alvaras_por_lote_3, sf_geo_sp)) == length(geo_alvaras_por_lote_3)
)

# Junta linhas
geo_alvaras_por_lote <- bind_rows(
  geo_alvaras_por_lote_1,
  # geo_alvaras_por_lote_2,
  geo_alvaras_por_lote_3
  ) %>%
  right_join(alvaras_por_lote, by = c("sql_incra", "sql_ajust", "endereco_ultimo")) %>%
  select(names(alvaras_por_lote)) %>%
  select(-sql_ajust)

#
ggplot(data = geo_alvaras_por_lote %>% filter(categoria_de_uso_grupo != "Outra")) +
  geom_sf(data = sf_geo_sp) +
  geom_sf(aes(color = fct_rev(categoria_de_uso_grupo)), show.legend = FALSE) +
  facet_wrap(~ano_execucao) +
  scale_color_manual(values = c("#009491", "#c4161c")) +
  theme_void()


# [Ajustar sql's duplicados de 2004]


# 6. Exporta --------------------------------------------------------------

#
fs::dir_create(here("inputs", "4_refined", "Alvaras"))

#
sfarrow::st_write_parquet(geo_alvaras_por_lote, 
                          here("inputs", "4_refined", "Alvaras", 
                               "geo_alvaras_por_lote.parquet"))

beepr::beep(8)