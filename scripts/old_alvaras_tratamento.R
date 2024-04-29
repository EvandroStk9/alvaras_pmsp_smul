library(here)
library(sf)
library(sfarrow)
library(arrow)
library(tidyverse)
library(tidylog)
library(readxl)
library(lubridate)
library(skimr)
library(DataExplorer)
library(stringr)
library(plotly)

# 0. Setup ----------------------------------------------------------------

# Definindo novo default de cores adequado à paleta de cores do Insper
options(ggplot2.discrete.colour = c("#009491", "#c4161c"),
        ggplot2.discrete.fill = c("#009491", "#c4161c"))

# Definindo default sem notação científica
options(scipen=999)

#
CRS <- "+proj=utm +zone=23 +south +ellps=WGS84 +datum=WGS84 +units=m +no_defs"

# 1. Importa -------------------------------------------------------------------

# Dado original, desagregado e sem georreferenciamento
# Necessário para atribuição da legislação
# Agregação: alvaras.parquet -> alvara_por_lote.parquet
alvaras <- arrow::read_parquet(here("inputs",
                                    "Alvaras", "ArquivosOriginais",
                                    "alvaras.parquet"))

# Dado original, agregado e georreferenciado feito em separado pela equipe
# Tratamento dos dados públicos, construindo série histórica e qualificando
# Processo está organizado em OneDrive/AlvarasPMSPSMUL e será publicizado
alvaras_por_lote <- sfarrow::st_read_parquet(here("inputs", 
                                                  "Alvaras", "ArquivosOriginais",
                                                  "geo_alvaras_por_lote.parquet"))

# Mesmo arquivo em formato shapefile em outputs/Alvaras/AlvarasPorLote.shp

# A - Geo

# Importa subprefeituras de São Paulo
geo_sp_subprefeituras <- st_read(here("inputs", 
                                      "Complementares", "Subprefeituras",
                                      "SubprefectureMSP_SIRGAS.shp"),
                                 crs = CRS) %>%
  st_transform(4674) %>% #
  # mutate(X = map_dbl(geometry, ~st_centroid(.x)[[1]]),
  #        Y = map_dbl(geometry, ~st_centroid(.x)[[2]]))
  transmute(subprefeitura = sp_nome)

# Importa distritos de São Paulo
geo_sp_distritos <- st_read(here("inputs", 
                                 "Complementares","Distritos",
                                 "DistrictsMSP_SIRGAS.shp"),
                            crs = CRS) %>%
  st_transform(4674) %>%
  transmute(distrito = ds_nome)

## Praça da Sé como CBD
geo_sp_cbd <- data.frame(long = -46.633959, lat= -23.550382) %>%
  st_as_sf(coords = c("long", "lat"),
           crs = 4674)

# Metro
geo_sp_estacao_metro <-st_read(here("inputs", "Complementares", 
                                    "Transporte", "EstacaoMetro",
                                    "SIRGAS_SHP_estacaometro_point.shp"),
                               crs = CRS)
# Ônibus
geo_sp_corredor_onibus <-st_read(here("inputs", "Complementares", 
                                      "Transporte","CorredorOnibus", 
                                      "SIRGAS_SHP_corredoronibus_line.shp"),
                                 crs = CRS)
# Trem
geo_sp_estacao_trem <- st_read(here("inputs", "Complementares", 
                                    "Transporte", "EstacaoTrem",
                                    "SIRGAS_SHP_estacaotrem_point.shp"),
                               crs = CRS) %>%
  st_transform(crs = CRS)


# B - Legislação

# Classificação da legislação
## SISACOE - Classificação da prefeitura + classificação Equipe Insper
# Script alvaras_tratamento_legislacao.R
alvaras_por_lote_legislacao <- arrow::read_parquet(
  here("inputs", "Alvaras", "ArquivosTratados", 
       "alvaras_por_lote_legislacao_tidy.parquet")) %>%
  select(id, legislacao, legislacao_origem)

# Macroareas
geo_sp_macroarea <- st_read(here("inputs", 
                                 "Complementares","Macroareas",
                                 "SIRGAS_SHP_MACROAREAS.shp"),
                            crs = CRS) %>%
  transmute(macroarea = as_factor(mc_sigla)) %>%
  st_transform(crs = st_crs(alvaras_por_lote))

# Setores da MEM
geo_sp_mem_setores <- st_read(here("inputs", 
                                   "Complementares","Macroareas",
                                   "MEMSetores",
                                   "sirgas_PDE_2A-Setores-MEM.shp"),
                              crs = CRS) %>%
  transmute(mem_setor = as_factor(str_to_upper(nm_perimet))) %>%
  st_transform(crs = st_crs(alvaras_por_lote)) %>%
  st_make_valid()

# PIUS
## Problema com double encoding!!!
geo_sp_pius <- st_read(here("inputs", 
                            "Complementares", "GestaoUrbana", "PIUS",
                            "PIUs_gestao_urbana.shp"),
                       crs = CRS) %>%
  mutate(across(where(is.character), 
                ~iconv(.x, to="latin1", from="utf-8") %>%
                  iconv(to="ASCII//TRANSLIT", from="utf-8") %>%
                  str_to_upper(locale = "br"))) %>%
  st_make_valid() %>%
  transmute(piu = Layer %>%
              str_remove("CONCESSAO DOS TERMINAIS - ")) %>%
  st_cast("MULTIPOLYGON") %>%
  st_transform(crs = st_crs(alvaras_por_lote)) %>%
  st_make_valid() 

# Zoneamento
## Importa tipologia de zoneamento e agrega informações por lote
## Avaliar colocar em outro script 
# geo_sp_zoneamento <- st_read(here("inputs", 
#                                   "Complementares", "Zoneamento", 
#                                   "ZonasLeiZoneamento", 
#                                   "ZoningSPL_Clean_Class.shp")) %>%
#   select(Zone) %>%
#   filter(!is.na(Zone)) %>%
#   st_transform(crs = st_crs(alvaras_por_lote)) %>%
#   st_make_valid(.) %>%
#   filter(st_is_valid(.) == TRUE) %>%
#   left_join(read_excel(here("inputs", 
#                             "Complementares", "Zoneamento", 
#                             "ZonasLeiZoneamento", 
#                             "Class_Join.xls")), # tipologia nossa
#             by = "Zone") %>%
#   transmute(
#     zoneamento_ajust = as.factor(Zone),
#     zoneamento_grupo = 
#       fct_relevel(ZoneGroup, 
#                   c("ZCs&ZMs", "EETU", "EETU Futuros", 
#                     "ZEIS Aglomerado", "ZEIS Vazio",  
#                     "ZPI&DE", "ZE&P", "ZE&PR", "ZCOR")) %>%
#       fct_other(drop = c("ZPI&DE", "ZE&P", "ZE&PR", "ZCOR"), 
#                 other_level = "Outros"),
#     solo_uso = as.factor(case_when(Use == "mixed use" ~ "misto",
#                                    Use == "unique use" ~ "único",
#                                    TRUE ~ NA_character_)),
#     ca_maximo = as.factor(FARmax))


# Zoneamento
geo_sp_zoneamento <- sfarrow::st_read_parquet(
  # here("outputs", "Zoneamento", "zoneamento.parquet")) %>%
  here("inputs", "Legislacao", 
       "zoneamento.parquet")) %>%
  st_transform(crs = st_crs(alvaras_por_lote)) %>%
  transmute(zoneamento_ajust = zoneamento, 
            zoneamento_grupo, solo_uso, ca_maximo)

# C - Indicadores
acessibilidade_empregos <-  st_read(here("inputs", "Complementares", 
                                         "AcessibilidadeEmpregos", "CleanedData",
                                         "AcessibilidadeEmpregos2019.shp")) %>%
  st_transform(crs = st_crs(alvaras_por_lote)) %>%
  transmute(acessibilidade_empregos = accssbl)


# 2. Seleciona ------------------------------------------------------------

# Selecionando variáveis de interesse
alvaras_por_lote_pre <- alvaras_por_lote %>%
  # filter(ind_aprovacao == 1 & ind_execucao == 1) %>%
  filter(categoria_de_uso_grupo != "Outra") %>% # Somente usos residenciais!
  filter(ano_execucao < 2022) %>%
  transmute(id, 
            ano_execucao,
            #diff_dias_aprovacao,
            categoria_de_uso_grupo,
            categoria_de_uso_registro,
            area_do_terreno,
            area_da_construcao,
            n_blocos,
            n_pavimentos,
            n_unidades,
            n_pavimentos_por_bloco,
            n_unidades_por_bloco,
            sql_incra,
            endereco_ultimo,
            bairro,
            zona_de_uso_registro,
            zona_de_uso_anterior,
            ind_uso_misto,
            # ind_r2v,
            # ind_r2h,
            ind_his,
            ind_hmp,
            ind_ezeis,
            # ind_zeis
  ) %>%
  left_join(alvaras_por_lote_legislacao, by = "id")

#
glimpse(alvaras_por_lote_pre)

#
skim(st_drop_geometry(alvaras_por_lote_pre))


# 3. Ajusta observações--------------------------------------------

#
st_drop_geometry(alvaras_por_lote_pre) %>%
  view()

# Analisa taxas de resposta 
alvaras_por_lote_pre %>% plot_missing()

# Observação 1: Taxa de resposta superestimada
## Visualizando o conjunto de dados podemos notar que as variáveis numéricas têm alguns registro 0
## Estes valores\ na verdade são não-respostas e, portanto, a taxa de resposta está superestimada
alvaras_por_lote_pre %>%
  st_drop_geometry() %>%
  filter(area_do_terreno == 0 & n_unidades == 0 |
           area_da_construcao == 0 & n_unidades == 0) %>%
  glimpse() 

## 2004 de 10699 (18.7%) | antes era 2437 de 9885 (24,7%)
## Áreas zero --> 712 (era 204)
## Unidades zero --> 1293 (era 2328)
## Corrigido na fonte pelo novo tratamento de dez-22
list(alvaras = alvaras_por_lote_pre,
     alvaras_zero = alvaras_por_lote_pre %>% 
       filter(area_do_terreno == 0 | area_da_construcao == 0 | n_unidades == 0),
     areas_zero = alvaras_por_lote_pre %>% 
       filter(area_do_terreno == 0 | area_da_construcao == 0),
     unidades_zero = alvaras_por_lote_pre %>% filter(n_unidades == 0)) %>%
  map(st_drop_geometry) %>%
  map_dfr(count, .id = "Var")

# 655 áreas de terreno zero, 683 áreas de construção zero, 1293 unidades zero
alvaras_por_lote_pre %>%
  st_drop_geometry() %>%
  summarize(across(starts_with(c("area_", "n_")), 
                   ~ sum(if_else(.x == 0, 1, 0), na.rm = TRUE)))

# Ajustando subnotificação de NA's
# Acrescenta informações de subprefeitura tal como definido na plaforma GeoSampa
# Garante somente observações circunscritas ao perímetro de São Paulo
alvaras_por_lote_cleaned <- alvaras_por_lote_pre %>%
  mutate(across(starts_with(c("area_", "n_")), ~ifelse(. == 0, NA, .))) #%>%
# st_intersection(geo_sp_subprefeituras %>% 
#                   transmute(subprefeitura = sp_nome))
#
alvaras_por_lote_pre %>% plot_missing()
alvaras_por_lote_cleaned %>% plot_missing()



# 4. Ajusta atributos ----------------------------------------------------

#
ind_eetu <- 
  bind_rows(
    # Metrô
    st_within(alvaras_por_lote_cleaned,
              st_buffer(geo_sp_estacao_metro %>%
                          st_transform(crs = 4674), 
                        dist = 600), 
              sparse = TRUE) %>%
      as.data.frame() %>% 
      transmute(id_row = row.id,
                id_Metro = col.id,
                Metro = 1) %>% # identificador binário para assinalar Tipo de transporte
      # Máximo de 600m e mínimo de 400m
      inner_join(st_intersects(alvaras_por_lote_cleaned, 
                               st_buffer(geo_sp_estacao_metro %>%
                                           st_transform(crs = 4674),
                                         dist = 400),
                               sparse = TRUE) %>%
                   as.data.frame() %>%
                   transmute(id_row = row.id,
                             id_Metro = col.id,
                             Metro = 1)),
    # Trem
    st_within(alvaras_por_lote_cleaned, 
              st_buffer(geo_sp_estacao_trem %>%
                          st_transform(crs = 4674), dist = 600), 
              sparse = TRUE) %>%
      as.data.frame() %>%
      transmute(id_row = row.id,
                id_Trem = col.id,
                Trem = 1) %>%
      inner_join(st_intersects(alvaras_por_lote_cleaned, 
                               st_buffer(geo_sp_estacao_trem %>%
                                           st_transform(crs = 4674), dist = 400),
                               sparse = TRUE) %>%
                   as.data.frame() %>%
                   transmute(id_row = row.id,
                             id_Trem = col.id,
                             Trem = 1)),
    # Ônibus
    st_within(alvaras_por_lote_cleaned,
              st_buffer(geo_sp_corredor_onibus %>%
                          st_transform(crs = 4674), dist = 300), 
              sparse = TRUE) %>%
      as.data.frame() %>%
      transmute(id_row = row.id,
                id_Corredor = col.id,
                Corredor = 1) %>%
      inner_join(st_intersects(alvaras_por_lote_cleaned, 
                               st_buffer(geo_sp_corredor_onibus %>%
                                           st_transform(crs = 4674), dist = 150),
                               sparse = TRUE) %>%
                   as.data.frame() %>%
                   transmute(id_row = row.id,
                             id_Corredor = col.id,
                             Corredor = 1))
  )


# CRS deve ser SIRGAS 2000 
st_crs(alvaras_por_lote_cleaned) == st_crs(geo_sp_subprefeituras)

# Criando variáveis para análise exploratória
alvaras_por_lote_tidy_com_outliers <- alvaras_por_lote_cleaned %>%
  # Agregando por geometria variáveis de zoneamento e macroarea
  st_join(geo_sp_distritos, join = st_intersects, largest = TRUE) %>%
  st_join(geo_sp_subprefeituras, join = st_intersects, largest = TRUE) %>%
  st_join(geo_sp_macroarea, join = st_intersects, largest = TRUE) %>%
  st_join(geo_sp_mem_setores, join = st_intersects, largest = TRUE) %>%
  st_join(geo_sp_pius, join = st_intersects, largest = TRUE) %>%
  st_join(geo_sp_zoneamento, join = st_nearest_feature) %>%
  # Criando variáveis para análise exploratória que dependem de informações GIS
  mutate(ca_total = area_da_construcao/area_do_terreno,
         area_da_unidade = area_da_construcao/n_unidades,
         cota_parte = area_do_terreno / n_unidades,
         distancia_cbd = round(as.numeric(st_distance(., geo_sp_cbd)) / 1000, 1),
         ind_miolo = case_when(macroarea != "MEM" & 
                                 zoneamento_grupo != "EETU" ~ TRUE, 
                               TRUE ~ FALSE),
         ind_eetu = if_else(row_number() %in% ind_eetu$id_row, TRUE, FALSE)) %>% 
  # Agregando por geometria indicador sintético por grid de 5km
  st_join(acessibilidade_empregos, join = st_nearest_feature) %>%
  mutate(
    # acessibilidade_empregos = rescale(acessibilidade_empregos, to = c(0, 1)),
    fx_acessibilidade_empregos = case_when(
      acessibilidade_empregos <= quantile(acessibilidade_empregos, 0.3) ~ "Muito baixa",
      between(acessibilidade_empregos, quantile(acessibilidade_empregos, 0.301),
              quantile(acessibilidade_empregos, 0.5)) ~ "Baixa",
      between(acessibilidade_empregos, quantile(acessibilidade_empregos, 0.501),
              quantile(acessibilidade_empregos, 0.7)) ~ "Média",
      between(acessibilidade_empregos, quantile(acessibilidade_empregos, 0.701),
              quantile(acessibilidade_empregos, 0.9)) ~ "Alta",
      acessibilidade_empregos > quantile(acessibilidade_empregos, 0.9) ~ "Muito alta"
    ) %>%
      fct_reorder(acessibilidade_empregos)) %>%
  select(everything(), geometry)


# 5. Trata outliers -------------------------------------------------------

# Função para ajustes de outliers com pequena correção p/ tratamento de NA's
scores_ajust <- function(x, ...) {
  not_na <- !is.na(x)
  scores <- rep(NA, length(x))
  scores[not_na] <- outliers::scores(na.omit(x), ...)
  scores
}

# Identifica outliers com teste Qui-quadrado + critério técnico
ind_outliers <- alvaras_por_lote_tidy_com_outliers %>%
  mutate(
    ind_outlier_area_unidade = scores_ajust(area_da_unidade,
                                            type = "chisq", prob = 0.9999),
    # ind_outlier_cota_parte = scores_ajust(cota_parte,
    #                                           type = "chisq", prob = 0.9999),
    ind_outlier_ca_total = scores_ajust(ca_total,
                                        type = "chisq", prob = 0.9999),
    ind_outlier_area_unidade_menos_15 = if_else(area_da_unidade < 15, 
                                                TRUE, FALSE)) %>%
  mutate(ind_outlier = case_when(
    area_da_unidade < 15 ~ TRUE, # Área precisa ser maior que 15 m²
    ind_outlier_area_unidade == TRUE | 
      # ind_outlier_cota_parte == TRUE |
      ca_total < 0.1 |
      ind_outlier_ca_total == TRUE ~ TRUE,
    TRUE ~ FALSE)) %>%
  # Atribui NA dos testes qui quadrado como FALSE/não-outlier
  mutate(across(starts_with("ind_outlier"),
                ~if_else(is.na(.x), FALSE, .x))) %>%
  relocate(starts_with("ind_"), .before = geometry)


# Outliers são 0.9% do total de empreendimentos e 18% do total de unidades
# 6.3% da área do terreno e 4.9% da área de construção
# Outliers eram 1.2% do total de empreendimentos e 18.1% do total de unidades
# 7% da área do terreno e 5% da área de construção
st_drop_geometry(ind_outliers) %>%
  mutate(ind_outlier = case_when(ind_outlier == TRUE ~ "outlier",
                                 ind_outlier == FALSE ~ "normal")) %>%
  group_by(ind_outlier) %>%
  summarize(n_empreendimentos = n(),
            n_unidades_total = sum(n_unidades, na.rm = TRUE),
            area_do_terreno_total = sum(area_do_terreno, na.rm = TRUE),
            area_da_construcao_total = sum(area_da_construcao, na.rm = TRUE)) %>%
  pivot_longer(-ind_outlier) %>%
  pivot_wider(names_from = ind_outlier, values_from = value) %>%
  mutate(prop = (outlier/(normal + outlier) * 100))

# Médias
st_drop_geometry(ind_outliers) %>%
  mutate(ind_outlier = case_when(ind_outlier == TRUE ~ "outlier",
                                 ind_outlier == FALSE ~ "normal")) %>%
  group_by(ind_outlier) %>%
  summarize(across(c(n_unidades, cota_parte, ca_total,
                     area_do_terreno, area_da_construcao,
                     n_pavimentos_por_bloco, distancia_cbd),
                   ~ mean(.x, na.rm = TRUE))) %>%
  pivot_longer(-ind_outlier) %>%
  pivot_wider(names_from = ind_outlier, values_from = value)

# Impacto de unidades é muito mais significativo na MEM do que na MQU e na MUC
left_join(
  ind_outliers %>%
    st_drop_geometry() %>%
    count(macroarea, wt = n_unidades),
  ind_outliers %>%
    st_drop_geometry() %>%
    filter(ind_outlier == TRUE) %>%
    count(macroarea, wt = n_unidades),
  by = "macroarea", suffix = c("_geral", "_outliers")) %>%
  mutate(prop = n_outliers/n_geral*100) 

# Anula número de unidades dos outliers
alvaras_por_lote_tidy <- ind_outliers %>%
  filter(ind_outlier == FALSE) %>%
  select(-starts_with("ind_outlier"))

# 6. Analisa padrões ----------------------------------------------------------

# OBS: Deslocar para relatório .Rmd ou excluir sessão

#
alvaras_por_lote_tidy %>%
  slice_sample(prop = 0.1) %>%
  view(title = "alvaras_por_lote_tidy_sample_0.1")

#
skim(st_drop_geometry(alvaras_por_lote_tidy))


#
cor(alvaras_por_lote_tidy$n_unidades, alvaras_por_lote_tidy$area_da_construcao, 
    use = "complete.obs")


# 296 de 1102 (21%) dos EETU's não se adequam ao indicador de EETU
alvaras_por_lote_tidy %>%
  filter(zoneamento_grupo == "EETU") %>%
  count(ind_eetu)

alvaras_por_lote_tidy %>%
  filter(macroarea == "MEM") %>%
  count(zoneamento_grupo,ind_eetu) %>%
  view()


#
alvaras_por_lote_tidy %>%
  mutate(ind_zona_eetu = if_else(zoneamento_grupo == "EETU", TRUE, FALSE)) %>%
  with(cor(ind_zona_eetu, ind_eetu, use = "complete.obs"))

#
st_drop_geometry(alvaras_por_lote_tidy) %>% 
  select(-starts_with("id")) %>%
  select(-c(macroarea, solo_uso, zoneamento_ajust)) %>%
  keep(is.numeric) %>%
  plot_correlation(cor_args = list(use = "pairwise.complete.obs"),
                   ggtheme = list(theme_minimal(base_family = "lato", 
                                                base_size = 12)),
                   theme_config = list(legend.position = "none",
                                       axis.title = element_blank(),
                                       axis.text.x = element_text(angle = 10)))

#
alvaras_por_lote_tidy %>% 
  select(-id) %>% 
  plot_histogram(ncol = 3)

#
alvaras_por_lote_tidy %>% 
  select(-id) %>% 
  plot_density(ncol = 3)

#
alvaras_por_lote_tidy %>% 
  select(area_da_construcao) %>% 
  filter(between(area_da_construcao, 0, 5000)) %>%
  plot_density()

#
alvaras_por_lote_tidy %>% 
  select(area_do_terreno) %>% 
  filter(between(area_do_terreno, 0, 4000)) %>%
  plot_density()

#
alvaras_por_lote_tidy %>% 
  select(n_unidades) %>% 
  filter(between(n_unidades, 0, 100)) %>%
  plot_density()

#
alvaras_por_lote_tidy %>% 
  select(subprefeitura) %>%
  plot_bar()

#
alvaras_por_lote_tidy %>% 
  select(categoria_de_uso_grupo) %>%
  plot_bar()

#
alvaras_por_lote_tidy %>% 
  select(subprefeitura, area_da_construcao) %>%
  plot_bar(with = "area_da_construcao")

#
alvaras_por_lote_tidy %>% 
  select(categoria_de_uso_grupo, area_da_construcao) %>%
  plot_bar(with = "area_da_construcao")

#
alvaras_por_lote_tidy %>% 
  select(subprefeitura, n_unidades) %>%
  plot_bar(with = "n_unidades")

#
alvaras_por_lote_tidy %>% 
  select(categoria_de_uso_grupo, n_unidades) %>%
  plot_bar(with = "n_unidades")

#
alvaras_por_lote_tidy %>% 
  select(-id) %>%
  plot_bar(maxcat = 2)

#
alvaras_por_lote_tidy %>% 
  select(-id) %>%
  filter(categoria_de_uso_grupo %in% c("ERM", "ERP")) %>%
  plot_boxplot(by = "categoria_de_uso_grupo", ncol = 2)

#
alvaras_por_lote_tidy %>% 
  ggplot(aes(ano_execucao)) +
  geom_histogram(aes(y = ..density..), bins = 18) +
  geom_density() +
  facet_wrap(~ categoria_de_uso_grupo) +
  scale_x_continuous(breaks = seq(2004, 2022, 1))

#
alvaras_por_lote_tidy %>% 
  ggplot(aes(ano_execucao)) +
  geom_histogram(bins = 18) +
  facet_wrap(~ categoria_de_uso_grupo) +
  scale_x_continuous(breaks = seq(2004, 2022, 1))

#
plot_n_unidades <- alvaras_por_lote_tidy %>% 
  ggplot(aes(ano_execucao, n_unidades, color = categoria_de_uso_grupo,
             label = endereco_ultimo)) +
  geom_point() +
  scale_x_continuous(breaks = seq(2004, 2022, 1))

#
plotly::ggplotly(plot_n_unidades)

#
plot_area_do_terreno <- alvaras_por_lote_tidy %>% 
  ggplot(aes(ano_execucao, area_do_terreno, color = categoria_de_uso_grupo)) + 
  geom_point() +
  scale_x_continuous(breaks = seq(2004, 2021, 1)) +
  coord_flip()

#
plotly::ggplotly(plot_area_do_terreno)

#
plot_cota_parte <- alvaras_por_lote_tidy %>% 
  ggplot(aes(ano_execucao, cota_parte, color = categoria_de_uso_grupo,
             label = endereco_ultimo)) +
  geom_point() +
  scale_x_continuous(breaks = seq(2004, 2022, 1)) +
  coord_flip()

#
plotly::ggplotly(plot_cota_parte)


# Nenhum alvará deve ter distância superior a 50 km da praça da Sé
st_drop_geometry(alvaras_por_lote_tidy) %>% 
  select(distancia_cbd, subprefeitura) %>%
  filter(distancia_cbd > 50) %>% count()

#
st_drop_geometry(alvaras_por_lote_tidy) %>% 
  select(distancia_cbd, subprefeitura) %>%
  filter(distancia_cbd < 50) %>%
  group_by(subprefeitura) %>%
  summarize(media_distancia_cbd = mean(distancia_cbd, na.rm=TRUE)) %>%
  plot_bar(with = "media_distancia_cbd")

#
alvaras_por_lote_tidy %>%
  filter(cota_parte < 100) %>%
  ggplot(aes(n_unidades, cota_parte, color = categoria_de_uso_grupo, 
             size = cota_parte)) +
  geom_point(alpha = .5) +
  facet_wrap(~categoria_de_uso_grupo, scales = "free")


# 7. Exporta ---------------------------------------------------------

# Exporta inputs
# Futuramente: queries em DW

# Dados originais desagregados em formato .xlsx
writexl::write_xlsx(alvaras, here("inputs", "Alvaras", "ArquivosOriginais", 
                                  "alvaras.xlsx"))

# Dados originais em formato .shp
st_write(alvaras_por_lote, here("inputs", "Alvaras", "ArquivosOriginais"),
         layer="alvaras_por_lote", delete_layer = TRUE, driver="ESRI Shapefile")

## Adiciona labels da Base Tratada
write_csv(data.frame(labels = names(alvaras_por_lote)), 
          here("inputs",  "Alvaras", "ArquivosOriginais",
               "alvaras_por_lote_labels.csv"))

# Dados tratados em formato .parquet
sfarrow::st_write_parquet(alvaras_por_lote_tidy, 
                          here("inputs", 
                               "Alvaras", "ArquivosTratados",
                               "alvaras_por_lote_tidy.parquet"))

# Dados tratados em formato .shp
st_write(alvaras_por_lote_tidy,
         here("inputs", "Alvaras", "ArquivosTratados"),
         layer="alvaras_por_lote_tidy", delete_layer = TRUE, 
         driver="ESRI Shapefile")

## Adiciona labels
write_csv(data.frame(labels = names(alvaras_por_lote_tidy)), 
          here("inputs", "Alvaras", "ArquivosTratados",
               "alvaras_por_lote_tidy_labels.csv"))

# Dados tratados em formato .xlsx
writexl::write_xlsx(alvaras_por_lote_tidy,
                    here("inputs", "Alvaras", "ArquivosTratados",
                         "alvaras_por_lote_tidy.xlsx"))
