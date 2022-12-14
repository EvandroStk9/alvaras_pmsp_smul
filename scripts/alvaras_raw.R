library(here)
library(tidyverse)
library(tidylog)
library(XLConnect)
library(janitor)
library(arrow)

# 1. Importa planilhas ---------------------------------------------------------

#
options(scipen = 9999)

#
pmsp_names <- list.dirs(here("inputs", "1_inbound", "PMSPSMUL")) %>%
  map_chr(~ word(.x, -1, sep = "/"))

#
pmsp_files <- list.dirs(here("inputs", "1_inbound", "PMSPSMUL")) %>%
  set_names(pmsp_names) %>%
  map(~ list.files(.x, pattern = ".xls$", full.names = TRUE)) %>%
  compact()


# 2. Verifica padrões anômalos -------------------------------------------------

# Verifica abas/planilhas com conteúdo ativo
pmsp_files_indexes <- pmsp_files %>%
  map_df(~ loadWorkbook(.x) %>%
        XLConnect::getActiveSheetIndex())

# Em 2012, excecpionalmente, a planilha ativa é a da posição 2
pmsp_files_indexes %>%
  transpose()

# Verifica primeira linha de todas as planilhas 
pmsp_sheets_headers <- map2(pmsp_files, pmsp_files_indexes, 
                  ~ loadWorkbook(.x) %>%
                    readWorksheet(.y, startRow = 1, endRow = 2)) 

# De 2000 a 2012 as planilhas contém um cabeçalho na linha 1, a exceção de 2003
pmsp_sheets_headers 

# Identificador das planilhas com cabeçalho
pmsp_files_headers <- as.character(c(2000:2002, 2004:2012))

# Em 2021 não há nome para blocos-pavimentos-unidades sendo a variável setada como "Col22"
pmsp_files["2021"] %>%
  map_df(~ loadWorkbook(.x) %>%
        readWorksheet(1)) %>%
  filter(!is.na(Col22)) %>%
  glimpse()

# Em 2020 há uma coluna sem nome (setada como "Col23") que duplica informação de blocos_pavimentos_unidades
# Apenas em poucas observações há acréscimo de informação (onde há NA em blocos_pavimentos_unidades)
pmsp_files["2020"] %>%
  map_df(~ loadWorkbook(.x) %>%
           readWorksheet(1)) %>%
  filter(!is.na(Col23)) %>%
  glimpse()

# Não há informação de blocos-pavimentos-unidades em 2003!!
pmsp_files["2003"] %>%
  map_df(~ loadWorkbook(.x) %>%
           readWorksheet(1)) %>%
  glimpse()

# 3. Lista planilhas -----------------------------------------------------------

#
pmsp_sheets <- map2(pmsp_files, pmsp_files_indexes, 
                 ~ loadWorkbook(.x) %>%
                   readWorksheet(.y, startRow = 1))
#
warnings()

#
pmsp_sheets_cleaned <-
  map_at(pmsp_sheets, pmsp_files_headers, 
         ~janitor::row_to_names(.x, row_number = 1)) %>%
  map(janitor::clean_names) %>%
  map_at("2021", ~rename(.x, x_b_locos_p_avimentos_u_nidades = col22)) %>%
  map2(names(.), ~mutate(.x, ano = .y) %>% select(ano, everything()))


# 4. Identifica padrões --------------------------------------------------------


# Sem diferenças em nome de atributo: padrao 1 = {2000, 2001}, padrao 2 = {2002, 2004-2010}, 
# padrao 3 = {2014, 2015}, padrao 4 = {2016, 2018, 2019}
##
waldo::compare(names(pmsp_sheets_cleaned[["2000"]]), 
               names(pmsp_sheets_cleaned[["2001"]])) 

##
map(as.character(2004:2010), 
    ~ waldo::compare(names(pmsp_sheets_cleaned[["2002"]]),
                     names(pmsp_sheets_cleaned[[.x]]))) 

##
waldo::compare(names(pmsp_sheets_cleaned[["2014"]]), 
               names(pmsp_sheets_cleaned[["2015"]])) 

##
map(as.character(2017:2019), 
    ~ waldo::compare(names(pmsp_sheets_cleaned[["2016"]]),
                     names(pmsp_sheets_cleaned[[.x]]))) %>%
  set_names(2017:2019)

# Com diferença marginal em nome: {2011, 2012, 2013, padrao 3}, {2017, padrao 4}, {2020, 2021, 2022}
##
waldo::compare(names(pmsp_sheets_cleaned[["2020"]]), 
               names(pmsp_sheets_cleaned[["2022"]])) 

##
waldo::compare(names(pmsp_sheets_cleaned[["2012"]]), 
               names(pmsp_sheets_cleaned[["2014"]])) 

##
waldo::compare(names(pmsp_sheets_cleaned[["2016"]]), 
               names(pmsp_sheets_cleaned[["2017"]])) 

# Com diferenças absolutas em nome: {2003}
##
map(as.character(c(2000, 2002, 2014, 2016, 2020)), 
    ~ waldo::compare(names(pmsp_sheets_cleaned[["2003"]]),
                     names(pmsp_sheets_cleaned[[.x]]))) %>%
  set_names(as.character(c(2000, 2002, 2014, 2016, 2020)))

# Hipótese: padrao 1 = {2000, 2001}, padrao 2 = {2002, 2004-2010}, 
# padrao 3 = {2011-2015}, padrao 4 = {2016-2019}, padrao 5 = {2020-2022}, 
# outlier = {2003} [4 categorias de uso, 5 áreas adicionais, sequencia, ident, espaco....]
pmsp_sheets_cleaned[["2003"]] %>%
  glimpse()

# Possivelmente, pode se ajustar 2003 ao padrão 2, mas não há informação de bloco-pavimento-unidade

# Ajustes ao padrão 3 no df de 2011
pmsp_sheets_cleaned["2011"] <- pmsp_sheets_cleaned["2011"] %>%
  map(~rename(.x, area_da_construcao_m = area_da_construcao_m2,
              area_do_terreno_m = area_do_terreno_m2,
              responsavel_pela_empresa_1 = responsavel_pela_empresa_2,
              x_b_locos_p_avimentos_u_nidades = b_locos_p_avimentos_u_nidades) %>%
        mutate(processo = NA_character_) %>%
        select(ano:alvara, processo, everything()))

# Ajustes ao padrão 3 no df de 2012
pmsp_sheets_cleaned["2012"] <- pmsp_sheets_cleaned["2012"] %>%
  map(~rename(.x, area_da_construcao_m = area_da_construcao_m2,
              area_do_terreno_m = area_do_terreno_m2,
              responsavel_pela_empresa_1 = responsavel_pela_empresa_2,
              x_b_locos_p_avimentos_u_nidades = b_locos_p_avimentos_u_nidades))

# Ajustes ao padrão 3 no df de 2013
pmsp_sheets_cleaned["2013"] <- pmsp_sheets_cleaned["2013"] %>%
  map(~rename(.x, administracao_regional = subprefeitura))

# Ajustes ao padrão 4 no df de 2017
pmsp_sheets_cleaned["2017"] <- pmsp_sheets_cleaned["2017"] %>%
  map(~rename(.x, subprefeitura = prefeitura_regional))

# Ajustes ao padrão 5 no df de 2020
pmsp_sheets_cleaned["2020"] <- pmsp_sheets_cleaned["2020"] %>%
  map(~select(.x, -col23))


#
padrao_1 <- map_df(as.character(2000:2001), ~ pmsp_sheets_cleaned[.x] %>% 
                   bind_rows())

#
padrao_2 <- map_df(as.character(c(2002, 2004:2010)), ~ pmsp_sheets_cleaned[.x] %>% 
                  bind_rows())

#
padrao_3 <- map_df(as.character(2011:2015), ~ pmsp_sheets_cleaned[.x] %>% 
                  bind_rows())

#
padrao_4 <- map_df(as.character(2016:2019), ~  pmsp_sheets_cleaned[.x] %>% 
                   bind_rows()) 

#
padrao_5 <- map_df(as.character(2020:2022), ~ pmsp_sheets_cleaned[.x] %>%
                     map(~mutate(.x, mes = as.character(mes))) %>%
                     bind_rows()) 

##
map2(list(padrao_1, padrao_2, padrao_3, padrao_4, padrao_5),
     list(padrao_2, padrao_3, padrao_4, padrao_5, padrao_1),
     ~ waldo::compare(names(.x),
                      names(.y)))
#
waldo::compare(names(padrao_1), 
               names(padrao_2)) 

# 5. Aplica Processamento Natural de Linguagem ---------------------------------

#
pmsp_sheets_names <- map(pmsp_sheets_cleaned, 
                         ~ names(.x) %>%
                           tibble() %>% 
                           set_names("name")) %>%
  map(bind_cols)

# Nome-padrão dos atributos/variáveis da "classe" alvaras
pmsp_alvaras_names <- tribble(
  ~name,
  # Em todos os alvarás
  "alvara",
  "descricao",
  "aprovacao",
  "mes",
  "unidade",
  "categoria_de_uso",
  "blocos_pavimentos_unidades",
  "sql_incra",
  "endereco",
  "bairro",
  "administracao_regional",
  "proprietario",
  "processo",
  "zona_de_uso",
  # "zona_de_uso_atual",
  "zona_de_uso_anterior",
  "area_da_construcao",
  "area_do_terreno",
  "dirigente_tecnico",
  # "dirigente_responsavel", Questão dos responsáveis precisa de análise caso a caso
  "projeto_autor",
  # "projeto_responsavel"
)

# Aplica modelo de processamento natural de linguagem para identificar o nome similar mais provável
model_pmsp_sheets_names <-
  map2(pmsp_sheets_names, names(pmsp_sheets_names),
       ~ fuzzyjoin::stringdist_join(pmsp_alvaras_names,
                                    .x,
                                    by = "name",
                                    mode = "left",
                                    method = "jw",
                                    max_dist = 4,
                                    distance_col = "jw_dist") %>% # Jaro-Winkler
         mutate(name.y = if_else(jw_dist > 0.3, NA_character_, name.y)) %>%
         group_by(name.x) %>% # argupa os vários resultados
         top_n(1, -jw_dist) %>% # toma somente o resultado mais provável (top 1)
         rename("name" = name.x,
                !!paste0("name_", .y) := name.y,
                !!paste0("jw_dist_", .y) := jw_dist) %>%
         select(-starts_with("jw_dist"))) %>%
  reduce(left_join, by = "name") %>% # unifica em um único conjunto de dados
  pivot_longer(cols = -name, 
               names_to = "ano", values_to = "att") %>%
  mutate(ano = str_sub(ano, -4L)) %>%
  pivot_wider(names_from = c("name"), values_from = "att")

#
model_pmsp_sheets_names %>%
  view("nomes_provaveis")

#
map(model_pmsp_sheets_names, unique)

# Lista os padrões identificados
list_padroes <- map(list(padrao_1, padrao_2, padrao_3, padrao_4, padrao_5),
         ~ names(.x) %>% as_tibble()) %>%
  flatten() %>%
  set_names(paste0("padrao_", 1:5))

# Analisa similitude dos padrões
analise_padroes <- tibble(padrao = names(list_padroes), att = list_padroes) %>%
  unnest(cols = c(att)) %>% 
  group_by(padrao, att) %>% 
  mutate(n = length(att)) %>%
  ungroup() %>%
  complete(padrao, att) %>%
  pivot_wider(names_from = "padrao", values_from = c("n")) 

#
analise_padroes %>%
  view()

# 6. Unifica padrões -----------------------------------------------------------

#
padrao_5_ajust <- padrao_5 %>%
  rename(data_aprovacao = aprovacao,
         n_blocos_pavimentos_unidades = x_b_locos_p_avimentos_u_nidades,
         subprefeitura = administracao_regional,
         area_da_construcao = area_da_construcao_m, 
         area_do_terreno = area_do_terreno_m,
         dirigente_responsavel = responsavel_pela_empresa,
         projeto_autor = autor_do_projeto,
         projeto_responsavel = responsavel_pela_empresa_1)

#
padrao_4_ajust <- padrao_4 %>%
  rename(data_aprovacao = aprovacao,
         n_blocos_pavimentos_unidades = x_b_locos_p_avimentos_u_nidades,
         area_da_construcao = area_da_construcao_m, 
         area_do_terreno = area_do_terreno_m,
         dirigente_responsavel = responsavel_pela_empresa,
         projeto_autor = autor_do_projeto,
         projeto_responsavel = responsavel_pela_empresa_1)

#
padrao_3_ajust <- padrao_3 %>%
  rename(data_aprovacao = aprovacao,
         n_blocos_pavimentos_unidades = x_b_locos_p_avimentos_u_nidades,
         area_da_construcao = area_da_construcao_m, 
         subprefeitura = administracao_regional,
         area_do_terreno = area_do_terreno_m,
         dirigente_responsavel = responsavel_pela_empresa,
         projeto_autor = autor_do_projeto,
         projeto_responsavel = responsavel_pela_empresa_1) %>%
  mutate(data_autuacao = NA_character_)

#
padrao_2_ajust <- padrao_2 %>%
  rename(data_aprovacao = aprovacao,
         n_blocos = numero_de_blocos,
         n_pavimentos_por_bloco = numero_de_pavimentos,
         n_unidades_por_bloco = numero_de_unidades,
         zona_de_uso_atual = zona_de_uso,
         subprefeitura = administracao_regional,
         dirigente_tecnico = firma_dirigente_tecnico,
         dirigente_responsavel = responsavel_tecnico,
         projeto_autor = autor_projeto,
         projeto_responsavel = responsavel_da_firma) %>%
  mutate(n_blocos_pavimentos_unidades = NA_character_,
         zona_de_uso_anterior = NA_character_,
         processo = NA_character_,
         data_autuacao = NA_character_) %>%
  select(-c(na, na_2, tipo_da_construcao)) 

#
alvaras_raw <- list(padrao_5_ajust, padrao_4_ajust, padrao_3_ajust, padrao_2_ajust) %>%
  reduce(full_join, by = names(padrao_5_ajust)) %>%
  transmute(data_aprovacao, mes, ano, alvara, descricao, unidade_pmsp = unidade, 
            processo, data_autuacao, categoria_de_uso, area_do_terreno, area_da_construcao,
            n_blocos_pavimentos_unidades, n_blocos, n_pavimentos_por_bloco, n_unidades_por_bloco,
            proprietario, dirigente_tecnico, dirigente_responsavel, projeto_autor, projeto_responsavel,
            sql_incra, endereco, bairro, subprefeitura, 
            zona_de_uso_registro = zona_de_uso_atual, zona_de_uso_anterior)

# 7. Exporta --------------------------------------------------------------

#
fs::dir_create(here("inputs", "2_raw", "Alvaras"))

#
arrow::write_parquet(alvaras_raw, here("inputs", "2_raw", "Alvaras", 
                                                      "alvaras_raw.parquet"))

