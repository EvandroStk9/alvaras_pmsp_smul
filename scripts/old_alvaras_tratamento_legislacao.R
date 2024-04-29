library(here)
library(sf)
library(tidyverse)
library(tidylog)
library(readxl)
library(sfarrow)
library(arrow)
library(waldo)
library(writexl)

# 1. Importa --------------------------------------------------------------

#
alvaras <- arrow::read_parquet(here("inputs", 
                                    "Alvaras", "ArquivosOriginais",
                                    "alvaras.parquet"))

#
alvaras_por_lote <- sfarrow::st_read_parquet(here("inputs", 
                                                  "Alvaras", "ArquivosOriginais",
                                                  "geo_alvaras_por_lote.parquet"))

# Classificação da legislação
## SISACOE - Classificação da prefeitura
#
sisacoe <- read_csv2(here("inputs", 
                          "Complementares","GestaoUrbana", "SISACOE",
                          "msp_empreendimentos_sisacoe.csv")) %>%
  transmute(alvara_data_emissao = lubridate::dmy(alvara_data_emissao),
            alvara_numero, sql,
            endereco = str_squish(endereco),
            legislacao_pde = case_when(
              str_detect(legislacao_pde, "13.430/02 - 16.050/14") ~ "PDE2002/PDE2014",
              str_detect(legislacao_pde, "16.050/14|16,05") ~ "PDE2014",
              str_detect(legislacao_pde, "13.430/02|13.340/02|13,43") ~ "PDE2002",
              str_detect(legislacao_lpuos, "16.402/16|Decreto 57.377/16|16,402") ~ "PDE2014",
              TRUE ~ NA_character_),
            legislacao_lpuos = case_when(
              str_detect(legislacao_lpuos, "7.805/72") ~ "LPUOS1972",
              str_detect(legislacao_lpuos, "13.885/04|13,885") ~ "LPUOS2004",
              str_detect(legislacao_lpuos, "16.402/16|Decreto 57.377/16|16,402") ~ "LPUOS2016",
              TRUE ~ NA_character_))

## Chave relacional
alvaras_key_sisacoe <- alvaras %>%
  left_join(sisacoe %>% 
              select(alvara_numero, alvara_data_emissao, starts_with("legislacao")), 
            by = c("alvara" = "alvara_numero")) %>%
  left_join(sisacoe %>%
              anti_join(alvaras, by = c("alvara_numero" = "alvara")) %>% 
              select(sql, starts_with("legislacao")), 
            by = c("sql_incra" = "sql", 
                   "legislacao_pde", "legislacao_lpuos")) %>%
  left_join(sisacoe %>%
              anti_join(alvaras, by = c("alvara_numero" = "alvara")) %>%
              anti_join(alvaras, by = c("sql" = "sql_incra")) %>% 
              select(endereco, starts_with("legislacao")), 
            by = c("endereco_raw" = "endereco",
                   "legislacao_pde", "legislacao_lpuos")) %>%
  filter(!duplicated(id)) %>%
  arrange(desc(alvara_data_emissao)) %>%
  select(sql_incra, legislacao_pde, legislacao_lpuos) %>%
  filter(sql_incra %in% alvaras_por_lote$sql_incra) %>%
  group_by(sql_incra) %>% 
  summarize(across(c(legislacao_pde, legislacao_lpuos), 
                   ~first(na.omit(.x))))

## Classificação qualitativa Equipe Insper
alvaras_por_lote_legislacao <- read_xlsx(here("inputs", 
                                              "Alvaras", "ArquivosTratados",
                                              "alvaras_por_lote_tidy_legislacao.xlsx")) %>%
  transmute(sql_incra, legislacao = case_when(
    legislacao %in% c("LPUOS1972", "LPUOS2004") | 
      legislacao == "PDE2002eLPUOS2004" ~ "PDE2002eLPUOS2004",
    legislacao %in% c("LPUOS2016", "PDE2014") ~ "PDE2014eLPUOS2016",
    TRUE ~ legislacao)) %>%
  left_join(alvaras_key_sisacoe %>%
              mutate(legislacao_origem = if_else(
                !is.na(legislacao_pde) & !is.na(legislacao_lpuos),
                "SISACOE", NA_character_)), by = "sql_incra") %>%
  mutate(legislacao = case_when(
    legislacao_pde == "PDE2014" & legislacao_lpuos == "LPUOS2004" ~ "PDE2014eLPUOS2004",
    legislacao_pde == "PDE2014" & legislacao_lpuos == "LPUOS2016" ~ "PDE2014eLPUOS2016",
    legislacao_pde == "PDE2002" & legislacao_lpuos == "LPUOS2004" ~ "PDE2002eLPUOS2004",
    legislacao_pde == "PDE2002" & legislacao_lpuos == "LPUOS2016" ~ "PDE2002eLPUOS2004",
    is.na(legislacao_pde) |  is.na(legislacao_lpuos) ~ legislacao
  )) %>%
  transmute(sql_incra, legislacao, 
            legislacao_origem = if_else(!is.na(legislacao_origem),
                                        legislacao_origem, "Insper")
  ) %>%
  filter(!is.na(sql_incra))

#
alvaras_por_lote_pre <- alvaras_por_lote %>%
  # filter(ind_aprovacao == 1 & ind_execucao == 1) %>%
  filter(categoria_de_uso_grupo != "Outra") %>% # Somente usos residenciais!
  filter(ano_execucao < 2022) %>%
  transmute(id, 
            sql_incra,
            ano_aprovacao,
            ano_execucao,
            data_autuacao_projeto,
            categoria_de_uso_registro,
            zona_de_uso_registro,
            zona_de_uso_anterior) %>%
  left_join(alvaras_por_lote_legislacao, by = "sql_incra")


# 2. Modela ---------------------------------------------------------------


# Dados para classificação
alvaras_por_lote_sem_legislacao <- alvaras_por_lote_pre %>%
  filter(is.na(legislacao)) 

# A) Zoneamento - tipologia
tipologia_zona_de_uso <- readxl::read_excel(here("inputs",
                                                 "Alvaras", "ArquivosTratados",
                                                 "tipologia_legislacao.xlsx"),
                                            sheet = "zoneamento_atual") %>%
  janitor::clean_names() %>%
  arrange(hierarquia) %>%
  transmute(zona_de_uso = str_squish(zona_de_uso) %>% 
              str_replace_all("​", "") %>%
              str_replace_all("X", ".") %>%
              str_replace_all(" ", "[[:space:]]"),
            lpuos_zona_de_uso = str_squish(lpuos_zona_de_uso),
            pde_zona_de_uso = str_squish(pde_zona_de_uso))

# Zoneamento - Modelo de classificação
modelo_zona_de_uso <- tipologia_zona_de_uso %>%
  group_by(pde_zona_de_uso, lpuos_zona_de_uso) %>%
  summarize(zona_de_uso = map_chr(list(unique(na.omit(zona_de_uso))),
                                  ~ paste0(.x, collapse = "|"))) %>%
  ungroup() %>%
  rowwise() %>%
  mutate(legislacao_zona_de_uso = str_c(pde_zona_de_uso, lpuos_zona_de_uso, sep = "e"),
         legislacao_zona_de_uso = case_when(
           is.na(legislacao_zona_de_uso) & is.na(pde_zona_de_uso) ~ lpuos_zona_de_uso,
           is.na(legislacao_zona_de_uso) & is.na(lpuos_zona_de_uso) ~ pde_zona_de_uso,
           TRUE ~ legislacao_zona_de_uso)) %>%
  ungroup() %>%
  arrange(desc(lpuos_zona_de_uso), desc(pde_zona_de_uso))

# B) Zoneamento anterior - tipologia
tipologia_zona_de_uso_anterior <- readxl::read_excel(here("inputs",
                                                          "Alvaras", "ArquivosTratados",
                                                          "tipologia_legislacao.xlsx"),
                                                     sheet = "zoneamento_anterior") %>%
  janitor::clean_names() %>%
  arrange(hierarquia) %>%
  transmute(zona_de_uso_anterior = str_squish(zona_de_uso_anterior) %>%
              str_replace_all("​", "") %>%
              str_replace_all("0", "[:digit:]") %>%
              str_replace_all(" ", "[[:space:]]"),
            lpuos_anterior = str_squish(lpuos_anterior),
            pde_zona_de_uso_anterior = str_squish(pde_zona_de_uso_anterior),
            lpuos_zona_de_uso_anterior = str_squish(lpuos_zona_de_uso_anterior))

# Zoneamento anterior - Modelo de classificação
modelo_zona_de_uso_anterior <- tipologia_zona_de_uso_anterior %>%
  group_by(lpuos_anterior, pde_zona_de_uso_anterior, lpuos_zona_de_uso_anterior) %>%
  summarize(zona_de_uso_anterior = map_chr(list(unique(na.omit(zona_de_uso_anterior))),
                                           ~ paste0(.x, collapse = "|"))) %>%
  ungroup() %>%
  rowwise() %>%
  mutate(legislacao_zona_de_uso_anterior = str_c(pde_zona_de_uso_anterior, 
                                                 lpuos_zona_de_uso_anterior, sep = "e"),
         legislacao_zona_de_uso_anterior = case_when(
           is.na(legislacao_zona_de_uso_anterior) & 
             is.na(pde_zona_de_uso_anterior) ~ lpuos_zona_de_uso_anterior,
           is.na(legislacao_zona_de_uso_anterior) & 
             is.na(lpuos_zona_de_uso_anterior) ~ pde_zona_de_uso_anterior,
           TRUE ~ legislacao_zona_de_uso_anterior)) %>%
  ungroup() %>%
  arrange(desc(lpuos_zona_de_uso_anterior), desc(pde_zona_de_uso_anterior),
          desc(lpuos_anterior))


# C) Zoneamento atual vs LPUOS anterior
tipologia_zoneamentos <- readxl::read_excel(here("inputs",
                                                 "Alvaras", "ArquivosTratados",
                                                 "tipologia_legislacao.xlsx"),
                                            sheet = "zoneamento_atual_vs_anterior") %>%
  janitor::clean_names() %>%
  arrange(hierarquia) %>%
  transmute(zona_de_uso = str_squish(zona_de_uso) %>%
              str_replace_all("​", "") %>%
              str_replace_all(" ", "[[:space:]]"),
            lpuos_anterior = str_squish(lpuos_anterior),
            pde_zoneamentos = str_squish(pde_zoneamentos),
            lpuos_zoneamentos = str_squish(lpuos_zoneamentos))

#
modelo_zoneamentos <- tipologia_zoneamentos %>%
  group_by(lpuos_anterior, pde_zoneamentos, lpuos_zoneamentos) %>%
  summarize(zona_de_uso = map_chr(list(unique(na.omit(zona_de_uso))),
                                  ~ paste0(.x, collapse = "|"))) %>%
  ungroup() %>%
  rowwise() %>%
  mutate(legislacao_zoneamentos = str_c(pde_zoneamentos, lpuos_zoneamentos, sep = "e"),
         legislacao_zoneamentos = case_when(
           is.na(legislacao_zoneamentos) & is.na(pde_zoneamentos) ~ lpuos_zoneamentos,
           is.na(legislacao_zoneamentos) & is.na(lpuos_zoneamentos) ~ pde_zoneamentos,
           TRUE ~ legislacao_zoneamentos)) %>%
  ungroup() %>%
  arrange(desc(lpuos_zoneamentos), desc(pde_zoneamentos),
          desc(pde_zoneamentos))


# D) Data de autuação 
ind_data_autuacao <- alvaras_por_lote_pre %>%
  st_drop_geometry() %>%
  transmute(id,
            pde_autuacao = if_else(data_autuacao_projeto < "2014-07-31",
                                   "PDE2002", "PDE2014"),
            lpuos_autuacao = case_when(data_autuacao_projeto >= "2016-03-23" ~ "LPUOS2016",
                                       data_autuacao_projeto < "2014-07-31" ~ "LPUOS2004", 
                                       TRUE ~ NA_character_),
            legislacao_autuacao = str_c(pde_autuacao, lpuos_autuacao, sep = "e"),
            legislacao_autuacao = case_when(
              is.na(legislacao_autuacao) & is.na(pde_autuacao) ~ lpuos_autuacao,
              is.na(legislacao_autuacao) & is.na(lpuos_autuacao) ~ pde_autuacao,
              TRUE ~ legislacao_autuacao))

# D - Categoria de uso +
ind_categoria_de_uso <- alvaras_por_lote_pre %>%
  st_drop_geometry() %>%
  transmute(id,
            pde_categoria_de_uso = if_else(str_detect(categoria_de_uso_registro, 
                                                      "HIS[[:space:]]1|HIS[[:space:]]2|EZEIS"),
                                           "PDE2014", NA_character_),
            lpuos_categoria_de_uso = if_else(str_detect(categoria_de_uso_registro, "EZEIS"),
                                             "LPUOS2016", NA_character_),
            legislacao_categoria_de_uso = str_c(pde_categoria_de_uso, lpuos_categoria_de_uso, sep = "e"),
            legislacao_categoria_de_uso = case_when(
              is.na(legislacao_categoria_de_uso) & is.na(pde_categoria_de_uso) ~ lpuos_categoria_de_uso,
              is.na(legislacao_categoria_de_uso) & is.na(lpuos_categoria_de_uso) ~ pde_categoria_de_uso,
              TRUE ~ legislacao_categoria_de_uso))


# 3. Classifica -----------------------------------------------------------


#
arvore <- alvaras_por_lote_pre %>%
  st_drop_geometry() %>%
  # Data de autuação
  left_join(ind_data_autuacao, by = "id") %>%
  # Categoria de uso
  left_join(ind_categoria_de_uso, by = "id") %>%
  mutate(
    # Zoneamento Anterior
    lpuos_anterior = case_when(
      str_detect(zona_de_uso_anterior,
                 modelo_zona_de_uso_anterior$zona_de_uso_anterior[1]) ~ modelo_zona_de_uso_anterior$lpuos_anterior[1],
      str_detect(zona_de_uso_anterior,
                 modelo_zona_de_uso_anterior$zona_de_uso_anterior[2]) ~ modelo_zona_de_uso_anterior$lpuos_anterior[2],
      str_detect(zona_de_uso_anterior,
                 modelo_zona_de_uso_anterior$zona_de_uso_anterior[3]) ~ modelo_zona_de_uso_anterior$lpuos_anterior[3],
      str_detect(zona_de_uso_anterior,
                 modelo_zona_de_uso_anterior$zona_de_uso_anterior[4]) ~ modelo_zona_de_uso_anterior$lpuos_anterior[4]      
    ),
    # Legislacao - Zoneamento
    legislacao_zona_de_uso = case_when(
      str_detect(zona_de_uso_registro,
                 modelo_zona_de_uso$zona_de_uso[1]) ~ modelo_zona_de_uso$legislacao_zona_de_uso[1],
      str_detect(zona_de_uso_registro,
                 modelo_zona_de_uso$zona_de_uso[2]) ~ modelo_zona_de_uso$legislacao_zona_de_uso[2],
      str_detect(zona_de_uso_registro,
                 modelo_zona_de_uso$zona_de_uso[3]) ~ modelo_zona_de_uso$legislacao_zona_de_uso[3],
      str_detect(zona_de_uso_registro,
                 modelo_zona_de_uso$zona_de_uso[4]) ~ modelo_zona_de_uso$legislacao_zona_de_uso[4],
    ),
    # Legislacao - Zoneamento Anterior
    legislacao_zona_anterior = case_when(
      str_detect(zona_de_uso_anterior,
                 modelo_zona_de_uso_anterior$zona_de_uso_anterior[1]) ~ modelo_zona_de_uso_anterior$legislacao_zona_de_uso_anterior[1],
      str_detect(zona_de_uso_anterior,
                 modelo_zona_de_uso_anterior$zona_de_uso_anterior[2]) ~ modelo_zona_de_uso_anterior$legislacao_zona_de_uso_anterior[2],
      str_detect(zona_de_uso_anterior,
                 modelo_zona_de_uso_anterior$zona_de_uso_anterior[3]) ~ modelo_zona_de_uso_anterior$legislacao_zona_de_uso_anterior[3]),
    # Legislacao - Zoneamentos
    legislacao_zoneamentos = case_when(
      str_detect(zona_de_uso_registro,
                 modelo_zoneamentos$zona_de_uso[1]) & 
        str_detect(lpuos_anterior, modelo_zoneamentos$lpuos_anterior[1]) ~ modelo_zoneamentos$legislacao_zoneamentos[1],
      str_detect(zona_de_uso_registro,
                 modelo_zoneamentos$zona_de_uso[2]) & 
        str_detect(lpuos_anterior, modelo_zoneamentos$lpuos_anterior[2]) ~ modelo_zoneamentos$legislacao_zoneamentos[2]),
    # Legislacao - Multicritério
    legislacao_arvore = case_when(
      # Aplica critérios
      !is.na(legislacao_zona_de_uso) &
        str_detect(legislacao_autuacao, "PDE2014eLPUOS2016") ~ legislacao_autuacao,
      !is.na(legislacao_zona_de_uso) & 
        str_detect(legislacao_zona_de_uso, "LPUOS") ~ legislacao_zona_de_uso,
      !is.na(legislacao_categoria_de_uso) &
        str_detect(legislacao_autuacao, "PDE2014eLPUOS2016") ~ legislacao_autuacao,
      !is.na(legislacao_categoria_de_uso) & 
        str_detect(legislacao_zoneamentos, "LPUOS") ~ legislacao_zoneamentos,
      !is.na(lpuos_anterior) & 
        str_detect(legislacao_categoria_de_uso, "LPUOS") ~ legislacao_categoria_de_uso,
      # !is.na(legislacao_zona_anterior) & 
      #   str_detect(legislacao_zona_anterior, "LPUOS") ~ legislacao_zona_anterior,
      !is.na(lpuos_anterior) & 
        str_detect(legislacao_autuacao, "LPUOS") ~ legislacao_autuacao,
      !is.na(legislacao_zona_anterior) ~ legislacao_zona_anterior,
      str_detect(legislacao_autuacao, "LPUOS", negate = TRUE) ~ legislacao_zona_de_uso,
      # Esgota critérios
      # is.na(legislacao_zona_de_uso) ~ legislacao_zoneamentos,
      # is.na(legislacao_zona_de_uso) & is.na(legislacao_zoneamentos) ~ legislacao_categoria_de_uso,
      # is.na(legislacao_zona_de_uso) & is.na(legislacao_zoneamentos) &
      #   is.na(legislacao_categoria_de_uso) ~ legislacao_zona_anterior,
      # is.na(legislacao_zona_de_uso) & is.na(legislacao_zoneamentos) &
      #   is.na(legislacao_categoria_de_uso) & is.na(legislacao_zona_anterior) ~ legislacao_autuacao,
      # is.na(legislacao_zona_anterior) ~ legislacao_autuacao,
      TRUE ~ legislacao_autuacao
    )
  ) %>%
  transmute(id, sql_incra, ano_aprovacao, ano_execucao, data_autuacao_projeto, categoria_de_uso_registro,
            zona_de_uso_registro, zona_de_uso_anterior,
            pde_categoria_de_uso, lpuos_categoria_de_uso,
            pde_autuacao, lpuos_autuacao, lpuos_anterior, 
            legislacao, legislacao_origem,
            legislacao_arvore = case_when(str_detect(legislacao_arvore, "PDE2002eLPUOS2004") ~ "PDE2002eLPUOS2004",
                                          str_detect(legislacao_arvore, "PDE2014eLPUOS2016") ~ "PDE2014eLPUOS2016",
                                          str_detect(legislacao_arvore, "PDE2014|PDE2014eLPUOS2004") ~ "PDE2014eLPUOS2004"),
            legislacao_zona_de_uso, legislacao_zoneamentos, 
            legislacao_categoria_de_uso, legislacao_zona_anterior, legislacao_autuacao)


# 4. Avalia ---------------------------------------------------------------


#
arvore %>%
  filter(is.na(legislacao)) %>%
  count(legislacao_arvore) %>%
  arrange(desc(n))

#
arvore %>%
  filter(!is.na(legislacao)) %>%
  mutate(#ind_na_train = if_else(!is.na(legislacao_train), 1, 0),
    ind_na = if_else(!is.na(legislacao_arvore), 1, 0)) %>%
  summarize(#cobertura_manual = mean(ind_na_train, na.rm = TRUE),
    cobertura_arvore = mean(ind_na, na.rm = TRUE))

#
nao_classificados <- arvore %>%
  filter(!is.na(legislacao)) %>%
  filter(is.na(legislacao_arvore)) %>%
  select(id:zona_de_uso_anterior, lpuos_anterior, legislacao)

#
nao_classificados %>%
  count(legislacao)

#
ggplot(nao_classificados) +
  geom_histogram(aes(ano_execucao), color = "black", bins = 23) +
  scale_x_continuous(breaks = seq(2004, 2022))

#
nao_classificados %>%
  mutate(aprovacao = if_else(ano_aprovacao < 2014, "Antes do PDE", "Depois do PDE")) %>%
  count(aprovacao)

#
nao_classificados %>%
  mutate(execucao = if_else(ano_execucao >= 2013, "Pós 2013", "Antes de 2013")) %>%
  count(execucao)

#
nao_classificados %>%
  filter(ano_aprovacao >= 2014) %>%
  view()

#
arvore %>%
  filter(is.na(legislacao)) %>%
  filter(is.na(legislacao_arvore)) %>%
  mutate(execucao = if_else(ano_execucao >= 2013, "Pós 2013", "Antes de 2013")) %>%
  count(execucao)

#
nao_classificados_alvo <- arvore %>%
  filter(ano_execucao >=2013) %>%
  filter(is.na(legislacao)) %>%
  filter(is.na(legislacao_arvore)) %>%
  select(id:zona_de_uso_anterior, lpuos_anterior, legislacao)

#
arvore %>%
  filter(ano_execucao >= 2013) %>%
  mutate(ind_match = if_else(legislacao_arvore == legislacao, 1, 0)) %>%
  summarize(acertos = length(which(ind_match == 1)),
            erros = length(which(ind_match == 0)),
            precisao = mean(ind_match, na.rm = TRUE))

#
divergentes <- arvore %>%
  filter(ano_execucao >= 2013 & legislacao != legislacao_arvore & !is.na(legislacao_arvore))

#
arvore %>%
  mutate(
    legislacao_categoria_de_uso = str_c(pde_categoria_de_uso, lpuos_categoria_de_uso, sep = "e"),
    legislacao_categoria_de_uso = case_when(is.na(legislacao_categoria_de_uso) & is.na(pde_categoria_de_uso) ~ lpuos_categoria_de_uso,
                                            is.na(legislacao_categoria_de_uso) & is.na(lpuos_categoria_de_uso) ~ pde_categoria_de_uso,
                                            TRUE ~ legislacao_categoria_de_uso)) %>%
  count(legislacao_categoria_de_uso, legislacao_zona_de_uso) %>%
  arrange(desc(n))

#
arvore %>%
  count(legislacao_autuacao, legislacao_zona_de_uso) %>%
  arrange(desc(n)) %>%
  na.omit()

#
arvore %>%
  count(lpuos_anterior, legislacao_zona_de_uso) %>%
  na.omit()



# 5. Incorpora complemento manual -----------------------------------------

# Após restarem 120 casos, conjunto foi enviado para nova análise manual dos casos restantes
alvaras_por_lote_legislacao_2 <- read_xlsx(here("inputs", 
                                                "Alvaras", "ArquivosTratados",
                                                "alvaras_por_lote_sem_legislacao_ajust.xlsx")) %>%
  transmute(id, sql_incra, 
            legislacao = case_when(
                legislacao == "PDE2002LPUOS2004" ~ "PDE2002eLPUOS2004",
              TRUE ~ legislacao),
            legislacao_origem = "Insper") %>%
  left_join(arvore %>% select(id, sql_incra, ano_execucao))

#
alvaras_por_lote_legislacao_2 %>%
  count(is.na(legislacao))

alvaras_por_lote_legislacao_2 %>%
  count(legislacao)


# 5. Exporta --------------------------------------------------------------

#
alvaras_por_lote_legislacao_tidy <- arvore %>%
  filter(!id %in% nao_classificados_alvo$id) %>%
  full_join(alvaras_por_lote_legislacao_2) %>%
  transmute(id,
            ano_execucao,
            legislacao = if_else(is.na(legislacao), legislacao_arvore, 
                                 legislacao),
            legislacao_origem = if_else(!is.na(legislacao) & is.na(legislacao_origem),
                                        "Insper", legislacao_origem),
            legislacao = case_when(str_detect(legislacao, "PDE2002eLPUOS2004") ~ "PDE2002eLPUOS2004",
                                          str_detect(legislacao, "PDE2014eLPUOS2016") ~ "PDE2014eLPUOS2016",
                                          str_detect(legislacao, "PDE2014|PDE2014eLPUOS2004") ~ "PDE2014eLPUOS2004"),
  )
  

#
alvaras_por_lote_legislacao_tidy %>%
  filter(ano_execucao >= 2013) %>%
  count(is.na(legislacao))

alvaras_por_lote_legislacao_tidy %>%
  filter(ano_execucao >= 2013) %>%
  count(legislacao)


#
arrow::write_parquet(alvaras_por_lote_legislacao_tidy,
                     here("inputs", "Alvaras", "ArquivosTratados", 
                          "alvaras_por_lote_legislacao_tidy.parquet"))

#
writexl::write_xlsx(list("sem_classificacao" = nao_classificados_alvo, 
                         "divergentes" = divergentes), 
                    here("inputs", "Alvaras", "ArquivosTratados", 
                         "alvaras_por_lote_sem_legislacao.xlsx"))

