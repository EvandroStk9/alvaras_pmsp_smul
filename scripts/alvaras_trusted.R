library(here)
library(arrow)
library(tidyverse)
library(tidylog)
library(lubridate)
library(tidytext)
library(readxl)
library(writexl)

# 1. Importa -------------------------------------------------------------------

#
alvaras_raw <- arrow::read_parquet(here("inputs", "2_raw", "Alvaras", 
                                        "alvaras_raw.parquet"))

# 2. Checa atributos -----------------------------------------------------------

# Todos dados vem da camada raw como string/character
alvaras_raw %>%
  glimpse()

# character
  # "ano",
  # "mes",
  # "alvara",
  # "descricao",
  # "unidade_pmsp",
  # "categoria_de_uso",
  # "processo",
  # "sql_incra",
  # "endereco",
  # "bairro",
  # "administracao_regional",
  # "proprietario",
  # "dirigente_tecnico",
  # "dirigente_responsavel",
  # "projeto_autor",
  # "projeto_responsavel",
  # "n_blocos_pavimentos_unidades",
  # "zona_de_uso_registro",
  # "zona_de_uso_anterior",
# date
  # "data_aprovacao",
  # "data_autuacao",
# double

  # "n_blocos",
  # "n_pavimentos_por_bloco",
  # "n_unidades_por_bloco",
  # "area_da_construcao",
  # "area_do_terreno"

#
alvaras_trimed <- map_df(alvaras_raw, ~ str_trim(.x) %>% 
                           str_squish()) %>%
  filter(!is.na(alvara)) %>% # 2 linhas de NA's importadas 
  filter(ano != "2002") %>% # Excluídos os anos de 2002 e 2003 por inconsistência
  mutate(id = row_number()) %>%
  select(id, everything())

# Número de observações únicas
map(alvaras_trimed, ~unique(.x) %>% length())

# Número de caracteres de atributos padronizados/padronizáveis
map(alvaras_trimed[c("data_aprovacao", "alvara", "processo", "data_autuacao", "sql_incra")], 
    ~nchar(.x) %>% unique())

# Chave relacional excluindo variáveis alvo do tratamento
alvaras_key <- alvaras_trimed %>%
  select(-c(# datas
            "data_aprovacao", "data_autuacao", 
            # numericas
            "area_do_terreno", "area_da_construcao", "n_blocos_pavimentos_unidades", 
            "n_blocos", "n_pavimentos_por_bloco", "n_unidades_por_bloco",
            # categoricas
            "ano", "mes", "descricao", "categoria_de_uso", "sql_incra", "endereco"))

# 3. Ajusta datas ---------------------------------------------------------

# Variáveis de data
alvaras_trimed %>%
  select(starts_with("data_"))

# Diferentes padrões de data
padroes_data_aprovacao <- alvaras_trimed %>%
  mutate(nchar_data_aprovacao = nchar(data_aprovacao)) %>%
  group_split(nchar_data_aprovacao)

#
map(padroes_data_aprovacao, ~ slice_sample(.x, prop = 0.1))
# 1 - 09/04/04 [D/M/y]
# 2 - 09/09/2020 [D/M/Y]
# 3 - 2020-01-09 00:00:00 [Y-M-D time]


# Padrões para a variável "data_autuacao" são os mesmos

# Cria tabela de atributos atualizados com chave relacional
atts_datas <- alvaras_trimed %>%
  select(id, starts_with("data_")) %>%
  mutate(across(starts_with("data_"), 
                ~ case_when(nchar(.x) == 8 ~ str_replace_all(.x, "$02", "2002") %>%
                            str_replace_all("/", "-") %>%
                            dmy(),
                            nchar(.x) == 10 ~ str_replace_all(.x, "/", "-") %>%
                              dmy(),
                            nchar(.x) == 19 ~ as_date(.x),
                            TRUE ~ NA_Date_)
  ))

# waarning message: problem wile computing | À primeira vista, porém, parece ok
atts_datas[c(9886, 39028, 2235, 24956), ] %>%
  pull(id)

# 4. Ajusta variáveis numéricas -------------------------------------------

#
alvaras_trimed %>%
  select(starts_with("n_"), starts_with("area_")) %>%
  glimpse()

# A - Áreas

# Ajusta area_construida e area_terreno
atts_areas <- alvaras_trimed %>%
  transmute(id, area_do_terreno_raw = area_do_terreno, 
            area_da_construcao_raw = area_da_construcao,
            area_do_terreno = if_else(str_detect(str_sub(area_do_terreno, -3L), "\\."),
                                      area_do_terreno,
                                      str_remove_all(area_do_terreno, "\\.")) %>%
              str_replace(",", ".") %>%
              as.numeric(),
            area_da_construcao = if_else(str_detect(str_sub(area_da_construcao, -3L), "\\."),
                                      area_da_construcao,
                                      str_remove_all(area_da_construcao, "\\.")) %>%
              str_replace(",", ".") %>%
              as.numeric())
  

# B - Blocos-Pavimentos-Unidades agregados

# Segmentação geral: anos que vieram com info agregada não tem info desagregada
map(alvaras_trimed %>% select(starts_with("n_")), 
    ~ filter(alvaras_trimed, !is.na(n_blocos_pavimentos_unidades) & !is.na(.x))) # 0 observações

# Info desagregada tem 3 vetores que devem interagir mas que nem sempre tem mesmo comprimento!
alvaras_trimed %>%
  filter(!is.na(n_blocos) | !is.na(n_pavimentos_por_bloco) | !is.na(n_unidades_por_bloco)) %>%
  select(id, starts_with("n_")) %>%
  slice_sample(prop = 0.1) %>%
  rowwise() %>%
  mutate(across(-id, ~str_count(.x, pattern = " - "))) %>%
  filter(n_blocos != n_unidades_por_bloco) %>%
  view()

# Segmentação info de blocos desagregada em 2002: começam com B ou começam com número
# alvaras_trimed %>%
#   filter(str_starts(n_blocos, "B:")) %>%
#   view()

# Aplica Processamento Natural de Linguagem a n_blocos_pavimentos_unidades no padrão "B: 2 P: 2 U: 20"
padroes_n_blocos_pavimentos_unidades <- alvaras_trimed %>%
  transmute(id, n_blocos_pavimentos_unidades_raw = n_blocos_pavimentos_unidades,
            n_blocos_pavimentos_unidades) %>%
  nest(data = n_blocos_pavimentos_unidades) %>%
  mutate(data = map(data, ~unnest_tokens(.x, tokens, colnames(.x), token = "words"))) %>%
  unnest(data) %>%
  mutate(var = str_extract(tokens, "[a-z]+"), # extrai alfabéticas
         n = as.integer(lead(str_extract(tokens, "[-+]?[0-9]*\\.?[0-9]+"))) # extrai numéricas e tipa a variável
         ) %>% 
  na.omit() %>%
  select(-tokens)

# Id's duplicados e letras e números separados como observações
padroes_n_blocos_pavimentos_unidades

# Podemos agora desagregar a informação, porém não necessariamente com 1 obsevação para cada id (forma tidy)
padroes_n_blocos_pavimentos_unidades %>%
  pivot_wider(names_from = var, values_from = n) %>%
  rename(list_n_blocos = b, list_n_pavimentos_por_bloco = p, list_n_unidades_por_bloco = u) %>%
  view()

# Para chegarmos a um número único podemos fazer a soma/multiplicação dos vetores
padroes_n_blocos_pavimentos_unidades %>%
  pivot_wider(names_from = var, values_from = n) %>%
  rename(list_n_blocos = b, list_n_pavimentos_por_bloco = p, list_n_unidades_por_bloco = u) %>%
  mutate(list_n_pavimentos = map2(list_n_blocos, list_n_pavimentos_por_bloco, ~.x * .y),
         list_n_unidades = map2(list_n_blocos, list_n_unidades_por_bloco, ~.x * .y)) %>%
  rowwise() %>%
  mutate(n_blocos = reduce(list_n_blocos, sum),
         n_pavimentos_por_bloco = mean(list_n_pavimentos_por_bloco),
         n_unidades_por_bloco = mean(list_n_unidades_por_bloco),
         n_pavimentos = reduce(list_n_pavimentos, sum),
         n_unidades = reduce(list_n_unidades, sum)) %>%
  view()

# Assinala resultado a objeto que será posteriormente unificado aos dados que vem desagregados
att_n_blocos_pavimentos_unidades_1 <- padroes_n_blocos_pavimentos_unidades %>%
  pivot_wider(names_from = var, values_from = n) %>%
  rename(list_n_blocos = b, list_n_pavimentos_por_bloco = p, list_n_unidades_por_bloco = u) %>%
  transmute(id, n_blocos_pavimentos_unidades_raw, list_n_blocos,
            list_n_pavimentos = map2(list_n_blocos, list_n_pavimentos_por_bloco, ~.x * .y),
            list_n_unidades = map2(list_n_blocos, list_n_unidades_por_bloco, ~.x * .y)) %>%
  rowwise() %>%
  mutate(n_blocos = reduce(list_n_blocos, sum),
         # n_pavimentos_por_bloco = mean(list_n_pavimentos_por_bloco),
         # n_unidades_por_bloco = mean(list_n_unidades_por_bloco),
         n_pavimentos = reduce(list_n_pavimentos, sum),
         n_unidades = reduce(list_n_unidades, sum)) %>%
  discard(is.list)

# Número de id's distintos deve ser igual a número de observações
n_distinct(att_n_blocos_pavimentos_unidades_1$id) == nrow(att_n_blocos_pavimentos_unidades_1)

# C - Número de blocos

# Tratamento aplicável somente a dados de 2002 e de 2004 a 2010

# Para tratar demais info desagregadas de padrão numérico, necessário separar em "palavras"/numerais
map(alvaras_trimed %>%
      select(starts_with("n_")) %>%
      filter(!is.na(n_blocos) | !is.na(n_pavimentos_por_bloco) | !is.na(n_unidades_por_bloco)),
    ~ str_extract(.x, "[-+]?[0-9]*\\.?[0-9]+") %>%
      word())

# Exemplo de vetores de diferentes comprimentos:
# SQL 129.023.0069-6
# SQL 140.260.0152-1
# SQL 188.003.0019-6
# Padrão 

# Cria tabela ajustada com atributo n_blocos e chave relacional
att_n_blocos <- alvaras_trimed %>%
  select(id, n_blocos) %>%
  mutate(n_blocos_raw = n_blocos,
         ) %>%
  nest(data = n_blocos) %>%
  mutate(data = map(data, ~unnest_tokens(.x, tokens, colnames(.x), token = "words"))) %>%
  unnest(data) %>%
  mutate(var = "list_n_blocos",
         n_blocos = as.integer(tokens)) %>%
  na.omit() %>%
  select(-tokens) %>%
  pivot_wider(names_from = var, values_from = n_blocos)

# D - Número de pavimentos por bloco
att_n_pavimentos_por_bloco <- alvaras_trimed %>%
  select(id, n_pavimentos_por_bloco) %>%
  mutate(n_pavimentos_por_bloco_raw = n_pavimentos_por_bloco) %>%
  nest(data = n_pavimentos_por_bloco) %>%
  mutate(data = map(data, ~unnest_tokens(.x, tokens, colnames(.x), token = "words"))) %>%
  unnest(data) %>%
  mutate(var = "list_n_pavimentos_por_bloco",
         n_pavimentos_por_bloco = as.integer(tokens)) %>%
  na.omit() %>%
  select(-tokens) %>%
  pivot_wider(names_from = var, values_from = n_pavimentos_por_bloco)

# E - Número de unidades por bloco
att_n_unidades_por_bloco <- alvaras_trimed %>%
  select(id, n_unidades_por_bloco) %>%
  mutate(n_unidades_por_bloco_raw = n_unidades_por_bloco) %>%
  nest(data = n_unidades_por_bloco) %>%
  mutate(data = map(data, ~unnest_tokens(.x, tokens, colnames(.x), token = "words"))) %>%
  unnest(data) %>%
  mutate(var = "list_n_unidades_por_bloco",
         n_unidades_por_bloco = as.integer(tokens)) %>%
  na.omit() %>%
  select(-tokens) %>%
  pivot_wider(names_from = var, values_from = n_unidades_por_bloco)

# F - Blocos-Pavimentos-Unidades desagregados

# Agora podemos formar um conjunto de atributos no mesmo formato de att_n_blocos_pavimentos_unidades_1
reduce(list(att_n_blocos, att_n_pavimentos_por_bloco, att_n_unidades_por_bloco),
       full_join, by = "id") %>%
  select(-ends_with("_raw")) %>%
  view()

# Obs: Quando vetores tem algum NA, retorna NULL (pavimentos) e integer(0) (unidades)
list(att_n_blocos, att_n_pavimentos_por_bloco, att_n_unidades_por_bloco) %>%
  reduce(full_join, by = "id") %>% # obs: 4 informações NULL em bloco
  mutate(list_n_pavimentos = map2(list_n_blocos, list_n_pavimentos_por_bloco, ~.x * .y),
         list_n_unidades = map2(list_n_blocos, list_n_unidades_por_bloco, ~.x * .y)) %>% 
  filter(!complete.cases(n_blocos_raw) | !complete.cases(n_pavimentos_por_bloco_raw) | 
         !complete.cases(n_unidades_por_bloco_raw)) %>%
  view()

# Obs: Quando vetores tem diferentes comprimentos multiplicação vetorial simples falha
list(att_n_blocos, att_n_pavimentos_por_bloco, att_n_unidades_por_bloco) %>%
  reduce(full_join, by = "id") %>%
  mutate(list_n_unidades = map2(list_n_blocos, list_n_unidades_por_bloco, ~.x * .y)) %>% 
  rowwise() %>%
  filter(length(list_n_blocos) != length(list_n_unidades_por_bloco)) %>%
  view()

# Funções replace_null e get_produto para corrigir
##
replace_null <- function(x) {  
  x <- purrr::map(x, ~ replace(.x, is.null(.x), NA_real_))
  purrr::map(x, ~ if(is.list(.x)) {replace_null(.x)} else .x)
}

##
get_produto <- function(x, y) {
  length(x) <- length(y)
  x * y
}

#
att_n_blocos_pavimentos_unidades_2 <- list(att_n_blocos, att_n_pavimentos_por_bloco, att_n_unidades_por_bloco) %>%
  reduce(full_join, by = "id") %>% # obs: 4 informações NULL em bloco
  mutate(across(where(is.list), replace_null)) %>%
  rowwise() %>%
  mutate(list_n_pavimentos = list(get_produto(x = list_n_blocos, y = list_n_pavimentos_por_bloco)),
         list_n_unidades = list(get_produto(x = list_n_blocos, y = list_n_unidades_por_bloco))) %>%
  mutate(n_blocos = reduce(list_n_blocos, sum),
         # n_pavimentos_por_bloco = mean(list_n_pavimentos_por_bloco),
         # n_unidades_por_bloco = mean(list_n_unidades_por_bloco),
         n_pavimentos = reduce(list_n_pavimentos, sum),
         n_unidades = reduce(list_n_unidades, sum)) %>%
  discard(is.list)


# Nomes devem ser idênticos exceto variáveis "raw"
names(att_n_blocos_pavimentos_unidades_1 %>% select(-ends_with("_raw"))) == 
  names(att_n_blocos_pavimentos_unidades_2 %>% select(-ends_with("_raw")))

# F - Unifica atributos de Blocos-Pavimentos-Unidades
att_n_blocos_pavimentos_unidades <- full_join(att_n_blocos_pavimentos_unidades_1, 
                  att_n_blocos_pavimentos_unidades_2, 
                  by = names(att_n_blocos_pavimentos_unidades_1 %>% 
                               select(-ends_with("_raw")))) %>%
  mutate(n_pavimentos_por_bloco = n_pavimentos/n_blocos,
         n_unidades_por_bloco = n_unidades/n_blocos)

# 22989 id's distintos (24,4% do universo)
n_distinct(att_n_blocos_pavimentos_unidades$id) 

# {n_blocos_pavimentos_unidades} U {n_blocos} U {n_pavimentos_por_bloco} U {n_unidades_por_bloco}
alvaras_trimed %>%
  filter(!is.na(n_blocos_pavimentos_unidades) | !is.na(n_blocos) | 
           !is.na(n_pavimentos_por_bloco) | !is.na(n_unidades_por_bloco)) %>%
  summarize(n = n_distinct(id))

# G - Cria tabela de atributos atualizados com chave relacional
atts_numericos <- left_join(atts_areas, att_n_blocos_pavimentos_unidades, by = "id")

#
atts_numericos %>%
  view()

# 5. Ajusta variáveis categóricas ----------------------------------------------

#
alvaras_trimed %>%
  select(-c(starts_with("data_"), starts_with("n_"), starts_with("area_"))) %>%
  names()

# A - Mês
att_mes <- alvaras_trimed %>%
  select(-starts_with("data_")) %>%
  left_join(atts_datas, by = "id") %>%
  transmute(id, mes_raw = mes, 
            mes = as_factor(str_to_upper(month(data_aprovacao, 
                                               label = TRUE, abbr = FALSE))))
# B - Ano
att_ano <- alvaras_trimed %>%
  select(-starts_with("data_")) %>%
  left_join(atts_datas, by = "id") %>%
  transmute(id, ano_raw = ano, 
            ano = year(data_aprovacao))

# C - Descrição
padroes_descricao <- alvaras_trimed %>%
  mutate(nchar_sql_incra = nchar(sql_incra)) %>%
  group_split(nchar_sql_incra)

#
padroes_descricao %>%
  map(~select(.x, id, ano, descricao))

# Definição de alvará relevante trazida do primeiro tratamento no script Java
att_descricao <- alvaras_trimed %>%
  transmute(id, descricao,
            ind_aprovacao = if_else(str_detect(descricao, "APROVACAO"), TRUE, FALSE),
            ind_execucao = if_else(str_detect(descricao, "EXECUCAO"), TRUE, FALSE),
            ind_edificacao_nova = if_else(str_detect(descricao, "EDIFICACAO NOVA"), TRUE, FALSE),
            ind_relevante = if_else(
              str_detect(descricao, "APROVACAO|EXECUCAO") & str_detect(descricao, "EDIFICACAO NOVA") &
                negate(str_detect)(descricao, "APOSTILAMENTO|REFORMA|DESPACHO|DEMOLICAO"),
              TRUE, FALSE))

#
warnings()

# 32839 alvaras de interesse
att_descricao %>%
  count(ind_relevante)


# 19670 alvaras de aprovação e execução
att_descricao %>%
  count(ind_execucao == TRUE & ind_aprovacao == TRUE)

# D - Unidade_PMSP
padroes_unidade_pmsp <- alvaras_trimed %>%
  mutate(nchar_unidade_pmsp = nchar(unidade_pmsp)) %>%
  group_split(nchar_unidade_pmsp)

# E - Processo
padroes_processo <- alvaras_trimed %>%
  mutate(nchar_processo = nchar(processo)) %>%
  group_split(nchar_processo)

#
map(padroes_processo, ~ .x %>%
      select(processo#, everything()
             ))

# 1 - 2003-1.026.714-1 | 2014-0.109.953-6 [29,004 observações]
# 2 - 0000-2003-1.035.848-1 | 0000-2018-0.067.765-7 [15,370 observações]

# Nota-se que os primeiros caracteres do padrão 2 poderiam ser suprimidos para ajuste ao padrão 1
# Porém, necessário avaliar se informação tem relevência antes de suprimir


# F - Categoria de uso
att_categoria_de_uso <- alvaras_trimed %>%
  transmute(id, categoria_de_uso,
            ind_r2v = if_else(str_detect(categoria_de_uso, "R2V"), TRUE, FALSE),
            ind_r2h = if_else(str_detect(categoria_de_uso, "R2H"), TRUE, FALSE),
            ind_his = if_else(str_detect(categoria_de_uso, "HIS|H.I.S"), TRUE, FALSE),
            ind_hmp = if_else(str_detect(categoria_de_uso, "HMP|H.M.P"), TRUE, FALSE),
            ind_zeis = if_else(str_detect(categoria_de_uso, "ZEIS"), TRUE, FALSE))
#
att_categoria_de_uso %>%
  count(ind_his)


# G - SQL_INCRA
padroes_sql_incra <- alvaras_trimed %>%
  mutate(nchar_sql_incra = nchar(sql_incra)) %>%
  group_split(nchar_sql_incra)

#
map(padroes_sql_incra, ~ .x %>%
      select(sql_incra, nchar_sql_incra, everything()))

# 1 - 1E+13 (1 observação)
# 2 - 19999999 (5 observações)
# 3 - 515750622 (59 observações)
# 4 - 9612702616 | 6,3601E+12 [108 observações]
# 5 - 14700100019 | 6,38358E+12 [299 observações] -> numerais = 11
# 6 - 400069999999 | 638358015040 [123 observações]
# 7 - 7777777777777 | 9501656876427 [1611 observações]
# 8 - 040.083.0032-5 | 193.006.0028-0 [46870 observações] -> numerais = 11
# 9 - 00.097.035.0013- | 00.001.049.0029- [302 observações] 
# 10 - 00.001.003.0089-8 | 00.168.024.0077-3 [13152 observações] -> numerais = 00 + 11

# Ano de 2004 sem dígito SQL
padroes_sql_incra[[9]] %>%
  view()

# 123 observações com problema de leitura no excel gerando notação científica (tipos mistos na mesma coluna)
alvaras_trimed %>% 
  filter(str_detect(sql_incra, pattern = "\\+1")) %>% 
  count(ano)

# Para correção é necessário voltar aos dados na camada inbound
# Esta correção deveria ser feita na camada raw! Necessário alterar depois

#
alvaras_sqls_nulos <- alvaras_raw %>% 
  filter(str_detect(sql_incra, pattern = "\\+1")) %>% 
  select(ano, alvara)

#
unique(alvaras_sqls_nulos$ano)

#
pmsp_files <- list.dirs(here("inputs", "1_inbound", "PMSPSMUL")) %>%
  set_names(~map_chr(.x, ~ word(.x, -1, sep = "/"))) %>%
  map(~ list.files(.x, pattern = ".xls$", full.names = TRUE)) %>%
  compact() %>%
  keep(names(.) %in% unique(alvaras_sqls_nulos$ano))

# Ano de 2012 começa na planilha 2
pmsp_files_indexes <- c(1, 2, 1, 1)

# Anos 2011 e 2012 possuem cabeçalho
pmsp_files_headers <- c("2011", "2012")

#
pmsp_sheets <- map2(pmsp_files, pmsp_files_indexes, 
                    ~ readxl::read_xls(.x, sheet = .y)) %>%
  map_at(pmsp_files_headers, 
         ~janitor::row_to_names(.x, row_number = 1)) %>%
  map(janitor::clean_names) %>%
  map2(names(.), ~mutate(.x, ano = .y) %>% select(ano, alvara, sql_incra))

#
alvaras_sqls_ajust <- pmsp_sheets %>%
  map(~filter(.x, alvara %in% alvaras_sqls_nulos$alvara)) %>%
  reduce(full_join, by = c("ano", "alvara", "sql_incra")) %>%
  full_join(alvaras_trimed %>% transmute(id, alvara, sql_incra),
                                         by = "alvara", suffix = c("_new", "_old")) %>%
  transmute(id,
            sql_incra = if_else(str_detect(sql_incra_old, pattern = "\\+1"), 
                                sql_incra_new, sql_incra_old))

#
alvaras_sqls_ajust %>% 
  filter(str_detect(sql_incra, pattern = "\\+1")) %>% 
  count()

#
att_sql_incra <- alvaras_sqls_ajust %>%
  transmute(id, sql_incra_raw = sql_incra, #nchar_sql_incra = nchar(sql_incra), 
            sql_incra = case_when(str_detect(sql_incra, pattern = "\\+1") ~ NA_character_,
                            nchar(sql_incra) == 17 ~ str_sub(sql_incra, start = 4L),
                            nchar(sql_incra) == 16 ~ paste0(str_sub(sql_incra, start = 4L), "x"),
                            nchar(sql_incra) == 14 ~ sql_incra,
                            nchar(sql_incra) == 11 ~ sql_incra,
                            TRUE ~ sql_incra),
            # ind_sql = if_else(nchar(sql_incra) >= 11, TRUE, FALSE),
            ind_sql_incra_null = if_else(str_detect(sql_incra, "(\\d)\\1{6,}"), TRUE, FALSE) # se repete 6 vezes ou mais
            )

# G - Endereco

# Processamento em Java faz um tratamento do endereço para fazer match entre endereços
# Pode ser interessante tomar o endereço produzido pelo geoloc como "endereco_ajust"

#
att_endereco <- alvaras_trimed %>%
  transmute(id, endereco_raw = endereco,
            endereco_ajust = str_remove_all(endereco_raw, ",SN|S/N"),
            logradouro = case_when(
              str_detect(endereco_ajust, "[0-9]\\'") ~ endereco_raw,
              TRUE ~ str_split(endereco_ajust, "(?<=[a-zA-Z])\\s*(?=[0-9])") %>%
                map_chr(~.x[1])),
            numero = case_when(
              str_detect(endereco_ajust, "\\sKM\\s") ~ str_extract(endereco_ajust, "KM.*") %>%
                str_replace_all(",", ".") %>%
                str_extract(pattern = "[-+]?[0-9]*\\.?[0-9]+") %>%
                str_replace_all("\\s", "") %>%
                as.numeric() %>% as.character(),
              str_detect(endereco_ajust, "[0-9]\\'") ~ str_extract(endereco_raw, "\\'.*") %>%
                str_extract(pattern = "[-+]?[0-9]*\\.?[0-9]+") %>%
                as.numeric() %>% as.character(),
              TRUE ~ str_extract(endereco_ajust, pattern = ".[-+]?[0-9]*\\.?[0-9]+") %>%
                as.numeric() %>% as.character()),
            endereco = case_when(
              str_detect(endereco_ajust, "\\sKM\\s") ~ paste0(logradouro, " KM ", numero),
              str_detect(endereco_ajust, "[0-9]\\'") ~ endereco_ajust, 
              numero>=0 & !is.na(numero) ~ paste0(logradouro, " ", numero),
              TRUE ~ logradouro) %>%
              str_replace_all(",", " "))

## NPL para tratar endereços
# passo 1: limpeza, paso 2: Separar logradouro e número, passo 3: aplicar similaridade ao mais recente
# teste1 <- slice_head(alvaras_trusted, n = 8000)
# teste2 <- teste1 %>% 
#   group_by(sql_incra) %>%
#   summarize(endereco_list = list(unique(endereco))) %>%
#   mutate(sim = map(endereco_list, ~ stringdist::stringsim(.x[1], .x, method = "jw"))) %>%
#   rowwise() %>%
#   mutate(n_enderecos = length(endereco_list),
#          n_sim = length(sim),
#          n_enderecos_distintos = length(sim[which(sim < 0.75)]))
# 
# teste_df <- tibble(unlist(teste2$endereco_list),
#              unlist(teste2$sim)) 

# Cria tabela de atributos atualizados com chave relacional
att_categoricos <- list(att_ano, att_mes, att_descricao, att_categoria_de_uso, att_sql_incra, 
                        att_endereco) %>%
  reduce(left_join, by = "id")

# 6. Cria conjunto de dados ----------------------------------------------------

#
alvaras_trusted_not_unique <- list(alvaras_key, atts_datas, atts_numericos, att_categoricos) %>%
  reduce(left_join, by = "id") %>%
  select(id, data_aprovacao, mes, ano, 
         alvara, descricao, unidade_pmsp, processo, data_autuacao, categoria_de_uso, 
         area_do_terreno_raw, area_do_terreno, area_da_construcao_raw, area_da_construcao,
         n_blocos_pavimentos_unidades_raw, n_blocos_raw, n_blocos, 
         n_pavimentos_por_bloco_raw, n_pavimentos_por_bloco, n_unidades_por_bloco_raw, 
         n_unidades_por_bloco, n_pavimentos, n_unidades,
         proprietario, dirigente_tecnico, dirigente_responsavel, projeto_autor, projeto_responsavel,
         sql_incra_raw, sql_incra, endereco_raw, endereco, bairro, subprefeitura, 
         zona_de_uso_registro, zona_de_uso_anterior, starts_with("ind_"))

# 1174 alvarás com número de alvará duplicado sendo 440 deles relevantes
alvaras_trusted_not_unique %>%
  janitor::get_dupes(alvara) %>%
  count(ind_relevante)

# Número duplicado, entretanto, não indica necessariamente mesmo alvará!
# Para correção, mantém-se tratamento dado no script Java
# Considerada duplicata Somente se idênticos: {unidade_pmsp, subprefeitura, alvara, 
# data_aprovacao, data_autuacao, descricao, endereco}
# 
alvaras_duplicatas <- alvaras_trusted_not_unique %>%
  janitor::get_dupes(unidade_pmsp, subprefeitura, alvara, data_aprovacao, data_autuacao, 
                     descricao, endereco_raw) 

#
alvaras_trusted <- alvaras_trusted_not_unique %>%
  distinct(unidade_pmsp, subprefeitura, alvara, data_aprovacao, data_autuacao, 
           descricao, endereco_raw, .keep_all = TRUE)

# 32681 alvarás relevantes 
alvaras_trusted %>%
  count(ind_relevante)

# 0 alvarás relevantes com codigo SQL inválido
alvaras_trusted %>%
  count(is.na(sql_incra) & ind_relevante == TRUE)

# 7. Cria conjunto de dados por agrupamento/empreendimento ---------------------


# Até 56 observações para grupos de sql + endereco em alvaras relevantes!
alvaras_trusted %>%
  filter(ind_relevante == TRUE) %>%
  group_by(sql_incra, endereco_raw) %>%
    count(sort = TRUE)

# Até 9 enderecos distintos para mesmo sql em alvaras relevantes!
alvaras_trusted %>%
  filter(ind_relevante == TRUE) %>%
  group_by(sql_incra) %>%
  summarize(n = n_distinct(endereco_raw)) %>%
  arrange(desc(n)) %>%
  view()

# Até 9 enderecos distintos para mesmo sql em alvaras relevantes!
# Obs: NA's devem ser tratados diferente
alvaras_trusted %>%
  filter(ind_relevante == TRUE) %>%
  group_by(sql_incra) %>%
  summarize(n = n_distinct(endereco_raw)) %>%
  arrange(desc(n))

#
alvaras_trusted %>%
  filter(ind_relevante == TRUE) %>%
  group_by(sql_incra) %>%
  summarize(n = n_distinct(endereco)) %>%
  arrange(desc(n))

# Solução mínima: tomar código SQL ou Incra como chave relacional para clusterização
# Ou seja id_empreendimento = id_sql

# Melhor solução para identificar empreendimento:
# Clusterização de enderecos similares (Distância Levenshtein) + sql_key

# Identificados os id's de empreendimento, necessário sumarizar informações numéricas
# E tomar primeiras e últimas datas
# Como envolve engenharia de atributos propriamente dita, desejável tratar em outro script

# Quando há indicativo de sql_incra nulo (6 digitos repetidos) não deve haver endereço nulo
alvaras_trusted %>%
  filter(ind_sql_incra_null == TRUE & is.na(endereco)) %>%
  count()

# Caso sql_incra seja inválido, unidade de observação é endereço
alvaras_trusted_por_endereco <- alvaras_trusted %>%
  filter(ind_sql_incra_null == TRUE) %>%
  group_by(endereco) %>%
  arrange(desc(data_aprovacao)) %>%
  summarize(
    ano_execucao = year(first(data_aprovacao[which(ind_execucao == TRUE)])),
    n_alvaras = n(),
    n_alvaras_relevantes = length(which(ind_relevante == TRUE)),
    data_aprovacao_projeto = first(data_aprovacao[which(ind_aprovacao == TRUE)]),
    data_aprovacao_execucao = first(data_aprovacao[which(ind_execucao == TRUE)]),
    diff_dias_aprovacao = interval(data_aprovacao_projeto, 
                                   data_aprovacao_execucao) / days(1),
    data_autuacao_projeto = first(data_autuacao[which(ind_aprovacao == TRUE)]),
    data_autuacao_execucao = first(data_autuacao[which(ind_execucao == TRUE)]),
    unidade_pmsp = map_chr(list(unique(unidade_pmsp)), ~paste0(.x, collapse = "; ")),
    # categoria_de_uso_classe = as.factor(case_when(
    #   any(ind_his == TRUE) | any(ind_hmp == TRUE) ~ "ERP",
    #   any(ind_his == FALSE) & any(ind_hmp == FALSE) ~ "ERM")),
    categoria_de_uso_grupo = as.factor(case_when(
      # any(ind_his == TRUE) ~ "HIS",
      # any(ind_hmp == TRUE) ~ "HMP",
      any(ind_his == TRUE) | any(ind_hmp == TRUE | any(ind_zeis == TRUE)) ~ "ERP",
      # any(str_detect(categoria_de_uso,"R2V")) ~ "R2V",
      any(str_detect(categoria_de_uso,"R2V|R202|R302|R2H|R301|R302|R303|(?<!N)R1|(?<!N)R2")) ~ "ERM",
      # any(str_detect(categoria_de_uso,"NR")) ~ "NR",
      TRUE ~ "Outra"
      )),
    # categoria_de_uso_registro = unique(unlist(str_split(categoria_de_uso, "; | - "))) %>% 
    #   paste0(collapse = "; "),
    categoria_de_uso_registro = map_chr(list(unique(na.omit(categoria_de_uso))), 
                                         ~ paste0(.x, collapse = "; ")),
    area_do_terreno = first(na.omit(
      area_do_terreno[which(ind_aprovacao == TRUE | ind_execucao == TRUE)])),
    area_da_construcao = first(na.omit(
      area_da_construcao[which(ind_aprovacao == TRUE | ind_execucao == TRUE)])), 
    n_blocos = first(na.omit(
      n_blocos[which(ind_aprovacao == TRUE | ind_execucao == TRUE)])),
    n_blocos_raw = map_chr(list(c(unique(na.omit(n_blocos_pavimentos_unidades_raw)),
                                unique(na.omit(n_blocos_raw)))),
                           ~ paste0(.x, collapse = "; ")),
    n_pavimentos = first(na.omit(
      n_pavimentos[which(ind_aprovacao == TRUE | ind_execucao == TRUE)])),
    n_unidades = first(na.omit(
      n_unidades[which(ind_aprovacao == TRUE | ind_execucao == TRUE)])),
    n_pavimentos_por_bloco = n_pavimentos/n_blocos,
    n_pavimentos_por_bloco_raw = map_chr(list(unique(na.omit(n_pavimentos_por_bloco_raw))), 
                                         ~ paste0(.x, collapse = "; ")),
    n_unidades_por_bloco = n_unidades/n_blocos,
    n_unidades_por_bloco_raw = map_chr(list(unique(na.omit(n_unidades_por_bloco_raw))), 
                                         ~ paste0(.x, collapse = "; ")), 
    proprietario = map_chr(list(unique(na.omit(proprietario))), 
                           ~ paste0(.x, collapse = "; ")),
    dirigente_tecnico = map_chr(list(unique(na.omit((dirigente_tecnico)))),
                                ~ paste0(.x, collapse = "; ")),
    dirigente_responsavel = map_chr(list(unique(na.omit(dirigente_responsavel))), 
                                    ~ paste0(.x, collapse = "; ")),
    projeto_autor = map_chr(list(unique(na.omit(projeto_autor))), 
                            ~ paste0(.x, collapse = "; ")),
    projeto_responsavel = map_chr(list(unique(na.omit(projeto_responsavel))), 
                                  ~ paste0(.x, collapse = "; ")),
    sql_incra = NA_character_,
    endereco_ultimo = first(endereco),
    n_enderecos = 1,
    endereco_registro = map_chr(list(unique(na.omit(endereco_raw))), 
                                ~ paste0(.x, collapse = "; ")),
    bairro = first(bairro),
    subprefeitura = map_chr(list(unique(na.omit(subprefeitura))), 
                            ~ paste0(.x, collapse = "; ")),
    zona_de_uso_registro = map_chr(list(unique(na.omit(zona_de_uso_registro))), 
                                   ~ paste0(.x, collapse = "; ")),
    zona_de_uso_anterior = map_chr(list(unique(na.omit(zona_de_uso_anterior))), 
                                   ~ paste0(.x, collapse = "; ")),
    ind_relevante = if_else(any(ind_relevante == TRUE), TRUE, FALSE),
    ind_aprovacao = if_else(any(ind_aprovacao == TRUE), TRUE, FALSE),
    ind_execucao = if_else(any(ind_execucao == TRUE), TRUE, FALSE),
    ind_his = if_else(any(ind_his == TRUE), TRUE, FALSE),
    ind_hmp = if_else(any(ind_hmp == TRUE), TRUE, FALSE),
    ind_zeis = if_else(any(ind_zeis == TRUE), TRUE, FALSE),
    ind_uso_misto = if_else(str_detect(categoria_de_uso_registro, 
                                       "NR|C1|C2|C3|S1|S2|S3|E1|E2|E3|E4") & 
                              categoria_de_uso_grupo != "Outra", TRUE, FALSE)
  ) %>%
  ungroup()

# Caso sql_incra seja válido, unidade de observação é sql_incra
alvaras_trusted_por_sql_incra <- alvaras_trusted %>%
  filter(ind_sql_incra_null == FALSE) %>%
  group_by(sql_incra) %>%
  arrange(desc(data_aprovacao)) %>% # importante para inferir que posição 1 é sempre data mais atual
  summarize(
    ano_execucao = year(first(data_aprovacao[which(ind_execucao == TRUE)])),
    n_alvaras = n(),
    n_alvaras_relevantes = length(which(ind_relevante == TRUE)),
    data_aprovacao_projeto = first(data_aprovacao[which(ind_aprovacao == TRUE)]),
    data_aprovacao_execucao = first(data_aprovacao[which(ind_execucao == TRUE)]),
    diff_dias_aprovacao = interval(data_aprovacao_projeto, 
                                   data_aprovacao_execucao) / days(1),
    data_autuacao_projeto = first(data_autuacao[which(ind_aprovacao == TRUE)]),
    data_autuacao_execucao = first(data_autuacao[which(ind_execucao == TRUE)]),
    unidade_pmsp = map_chr(list(unique(unidade_pmsp)), ~paste0(.x, collapse = "; ")),
    # categoria_de_uso_classe = as.factor(case_when(
    #   any(ind_his == TRUE) | any(ind_hmp == TRUE) ~ "ERP",
    #   any(ind_his == FALSE) & any(ind_hmp == FALSE) ~ "ERM")),
    categoria_de_uso_grupo = as.factor(case_when(
      # any(ind_his == TRUE) ~ "HIS",
      # any(ind_hmp == TRUE) ~ "HMP",
      any(ind_his == TRUE) | any(ind_hmp == TRUE | any(ind_zeis == TRUE)) ~ "ERP",
      # any(str_detect(categoria_de_uso,"R2V")) ~ "R2V",
      any(str_detect(categoria_de_uso,"R2V|R202|R302|R2H|R301|R302|R303|(?<!N)R1|(?<!N)R2")) ~ "ERM",
      # any(str_detect(categoria_de_uso,"NR")) ~ "NR",
      TRUE ~ "Outra"
      )),
    categoria_de_uso_registro = map_chr(list(unique(na.omit(categoria_de_uso))), 
                                        ~ paste0(.x, collapse = "; ")),
    area_do_terreno = first(na.omit(
      area_do_terreno[which(ind_aprovacao == TRUE | ind_execucao == TRUE)])),
    area_da_construcao = first(na.omit(
      area_da_construcao[which(ind_aprovacao == TRUE | ind_execucao == TRUE)])), 
    n_blocos = first(na.omit(
      n_blocos[which(ind_aprovacao == TRUE | ind_execucao == TRUE)])),
    n_blocos_raw = map_chr(list(c(unique(na.omit(n_blocos_pavimentos_unidades_raw)),
                                  unique(na.omit(n_blocos_raw)))),
                           ~ paste0(.x, collapse = "; ")),
    n_pavimentos = first(na.omit(
      n_pavimentos[which(ind_aprovacao == TRUE | ind_execucao == TRUE)])),
    n_unidades = first(na.omit(
      n_unidades[which(ind_aprovacao == TRUE | ind_execucao == TRUE)])),
    n_pavimentos_por_bloco = n_pavimentos/n_blocos,
    n_pavimentos_por_bloco_raw = map_chr(list(unique(na.omit(n_pavimentos_por_bloco_raw))), 
                                         ~ paste0(.x, collapse = "; ")),
    n_unidades_por_bloco = n_unidades/n_blocos,
    n_unidades_por_bloco_raw = map_chr(list(unique(na.omit(n_unidades_por_bloco_raw))), 
                                       ~ paste0(.x, collapse = "; ")), 
    proprietario = map_chr(list(unique(na.omit(proprietario))), 
                           ~ paste0(.x, collapse = "; ")),
    dirigente_tecnico = map_chr(list(unique(na.omit((dirigente_tecnico)))),
                                ~ paste0(.x, collapse = "; ")),
    dirigente_responsavel = map_chr(list(unique(na.omit(dirigente_responsavel))), 
                                    ~ paste0(.x, collapse = "; ")),
    projeto_autor = map_chr(list(unique(na.omit(projeto_autor))), 
                            ~ paste0(.x, collapse = "; ")),
    projeto_responsavel = map_chr(list(unique(na.omit(projeto_responsavel))), 
                                  ~ paste0(.x, collapse = "; ")),
    sql_incra = first(sql_incra),
    endereco_ultimo = first(na.omit(endereco)),
    n_enderecos = n_distinct(endereco),
    endereco_registro = map_chr(list(unique(na.omit(endereco_raw))), 
                                ~ paste0(.x, collapse = "; ")),
    bairro = first(bairro),
    subprefeitura = map_chr(list(unique(na.omit(subprefeitura))), 
                            ~ paste0(.x, collapse = "; ")),
    zona_de_uso_registro = map_chr(list(unique(na.omit(zona_de_uso_registro))), 
                                   ~ paste0(.x, collapse = "; ")),
    zona_de_uso_anterior = map_chr(list(unique(na.omit(zona_de_uso_anterior))), 
                                   ~ paste0(.x, collapse = "; ")),
    ind_relevante = if_else(any(ind_relevante == TRUE), TRUE, FALSE),
    ind_aprovacao = if_else(any(ind_aprovacao == TRUE), TRUE, FALSE),
    ind_execucao = if_else(any(ind_execucao == TRUE), TRUE, FALSE),
    ind_r2v = if_else(any(ind_r2v == TRUE), TRUE, FALSE),
    ind_r2h = if_else(any(ind_r2h == TRUE), TRUE, FALSE),
    ind_his = if_else(any(ind_his == TRUE), TRUE, FALSE),
    ind_hmp = if_else(any(ind_hmp == TRUE), TRUE, FALSE),
    ind_zeis = if_else(any(ind_zeis == TRUE), TRUE, FALSE),
    ind_uso_misto = if_else(str_detect(categoria_de_uso_registro, 
                                       "NR|C1|C2|C3|S1|S2|S3|E1|E2|E3|E4") & 
                              categoria_de_uso_grupo != "Outra", TRUE, FALSE)
    ) %>%
  ungroup()

# Sem ajuste de identificador do lote, teríamos 30432 observações
alvaras_trusted %>%
  summarize(n_distinct(sql_incra))

# Cria alvaras por lote 
# Com ajuste temos 30484 observações
alvaras_trusted_por_lote <- alvaras_trusted_por_endereco %>%
  select(-endereco) %>%
  full_join(alvaras_trusted_por_sql_incra, by = names(.)) %>%
  mutate(id = row_number()) %>%
  select(id, everything())

# 203 códigos SQL/Incra setados como NA
alvaras_trusted_por_lote %>%
  count(is.na(sql_incra))

# SQL's sem alvara de execução não podem ter data de aprovação de execução
alvaras_trusted_por_lote %>% 
  filter(ind_relevante == TRUE) %>%
  filter(is.na(data_aprovacao_execucao)) %>%
  count(ind_execucao)

# 11552 alvarás relevantes
alvaras_trusted_por_lote %>% 
  filter(ind_relevante == TRUE) %>%
  count()

# 1912 de 11552 (16.6%) com mais de um endereço válido
alvaras_trusted_por_lote %>% 
  filter(ind_relevante == TRUE) %>%
  filter(n_enderecos > 1) %>%
  count()
  
# 8. Exporta -------------------------------------------------------------------

#
fs::dir_create(here("inputs", "3_trusted", "Alvaras"))

#
arrow::write_parquet(alvaras_trusted, here("inputs", "3_trusted", "Alvaras", 
                                       "alvaras.parquet"))
#
arrow::write_parquet(alvaras_trusted_por_lote, here("inputs", "3_trusted", "Alvaras", 
                                           "alvaras_por_lote.parquet"))

#
beepr::beep(8)
