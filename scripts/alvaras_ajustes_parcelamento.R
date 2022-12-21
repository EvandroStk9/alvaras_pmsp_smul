library(beepr)
library(here)
library(arrow)
library(tidyverse)
library(lubridate)
library(DataExplorer)

#
options(error = beep)

# 0. Importa --------------------------------------------------------------
#
alvaras_trusted <- arrow::read_parquet(here("inputs", "3_trusted", "Alvaras", 
                                            "alvaras_old.parquet"))

# 1. Ajusta observações ---------------------------------------------------

# Subnotificações de NA -> Taxa de resposta superestimada
## Visualizando o conjunto de dados podemos notar que as variáveis numéricas têm alguns registro 0
## Estes valores\ na verdade são não-respostas e, portanto, a taxa de resposta está superestimada
alvaras_trusted %>%
  select(-ends_with("_raw")) %>%
  summarize(across(starts_with(c("area_", "n_")), 
                   ~ sum(if_else(.x == 0, 1, 0), na.rm = TRUE))) %>%
  pivot_longer(cols = everything())

#
alvaras_trusted %>%
  group_by(sql_incra) %>%
  filter(any(ind_relevante == TRUE)) %>%
  select(-ends_with("_raw")) %>%
  select(starts_with(c("area_", "n_"))) %>%
  plot_missing()

#
alvaras_trusted_ajust <- alvaras_trusted %>%
  # Ajusta subnotificação de NA's
  mutate(across(starts_with(c("area_", "n_")), ~ifelse(. == 0, NA, .)))

# 2. Reorganiza atributos  ---------------------------------------------

#
alvaras_trusted_ajust %>%
  mutate(
    endereco_ajust = str_remove(endereco,
                                "R |AV |ES |EST |VIA |AL |PC |PG |LV |TV |PV |LG |VD |RV |PQ |VP "),
         ind_diff_endereco = if_else(endereco != endereco_ajust, TRUE, FALSE)) %>%
  select(endereco_raw, endereco, endereco_ajust, ind_diff_endereco) %>%
  view()

# Indicadores de loteamento/parcelamento
alvaras_trusted_ajust %>%
  filter(str_detect(descricao, 
                    "DESMEMBRAMENTO|LOTEAMENTO|DESDOBRO|REMEMBRAMENTO")) %>%
  count(descricao) #%>%
  #view()

# Alvarás de conclusão
# Podem indicar provável parcelamento
alvaras_trusted_ajust %>%
  filter(str_detect(descricao, "CONCLUSAO")) %>%
  count(descricao)

# Alvarás determinados como relevantes
alvaras_trusted_ajust %>%
  filter(ind_relevante == TRUE) %>%
  count(descricao)

# Alvarás de Reconsideração de Despacho de Alvara de Aprovação e/ou de Execução
# Devem ser tipificados como relevantes
# Podem indicar provável parcelamento
alvaras_trusted_ajust %>%
  filter(str_detect(descricao, "RECONSIDERACAO") &
           str_detect(descricao, "EDIFICACAO") &
           ind_aprovacao == TRUE |
           str_detect(descricao, "RECONSIDERACAO") &
           str_detect(descricao, "EDIFICACAO") &
           ind_execucao == TRUE) %>%
  count(descricao)


# Tipologias de descrições/tipos de alvarás e indicativos de relevância
alvaras_trusted_ajust_2 <- alvaras_trusted_ajust %>%
  mutate(
    endereco = str_remove(endereco,
                                "R |AV |ES |EST |VIA |AL |PC |PG |LV |TV |PV |LG |VD |RV |PQ |VP "),
    ind_edificacao_nova = if_else(
      str_detect(descricao,"EDIFICACAONOVA|EDI-FICACAO NOVA"),
      TRUE, ind_edificacao_nova),
    ind_loteamento = if_else(
      str_detect(descricao,"DESMEMBRAMENTO|LOTEAMENTO|DESDOBRO|REMEMBRAMENTO|TERMO DE VERIF"),
      TRUE, FALSE),
    ind_execucao = if_else(str_detect(descricao, "TERMO DE VERIF"),
                           FALSE, ind_execucao),
    ind_conclusao = if_else(str_detect(descricao, "CONCLUSAO"),
                            TRUE, FALSE),
    ind_aprovacao_execucao = if_else(ind_aprovacao == TRUE & ind_execucao == TRUE,
                                     TRUE, FALSE),
    ind_correcao = if_else(
      str_detect(descricao,"APOSTILAMENTO|PROJETO MODIFICATIVO"),
      TRUE, FALSE),
    ind_relevante_ajust = if_else(
      ind_loteamento == TRUE | ind_conclusao == TRUE |
        # (str_detect(descricao, "RECONSIDERACAO") &
        #    str_detect(descricao, "EDIFICACAO") &
        #    ind_aprovacao == TRUE |
        #    str_detect(descricao, "RECONSIDERACAO") &
        #    str_detect(descricao, "EDIFICACAO") &
        #    ind_execucao == TRUE),
        ind_edificacao_nova == TRUE,
      TRUE, FALSE),
    setor = str_sub(sql_incra, start = 0L, end = 3L),
    quadra = str_sub(sql_incra, start = 5L, end = 7L), 
    descricao_tipo = case_when(
      ind_loteamento == TRUE ~ "LOTEAMENTO",
      ind_aprovacao_execucao == TRUE ~ "APROVACAO E EXECUCAO",
      ind_aprovacao == TRUE ~ "APROVACAO",
      ind_execucao == TRUE ~ "EXECUCAO",
      ind_conclusao == TRUE ~ "CONCLUSAO",
      TRUE ~ "OUTRO"
    )
  )

# Atributos necessários:
# descricao_tipo, ind_edificacao_nova, ind_correcao


# Alvarás relevantes para contabilidade:
# Aprovação, Execução ou Aprovação e Execução de Edificação Nova
# Loteamento e Conclusão (quando lote está atribuido à edificaçao nova)
# Dados Bloco-Pavimentos-Unidades: Aprovação, Aprovaçao e Execuçao
# Casos excepcionais podem ter dados em Execuçao

#
tipologia_descricao <- alvaras_trusted_ajust_2 %>%
  group_by(descricao) %>%
  summarize(
    descricao_tipo = first(descricao_tipo),
    ind_relevante = first(ind_relevante),
    ind_relevante_ajust = first(ind_relevante_ajust),
    ind_edificacao_nova = first(ind_edificacao_nova),
    ind_loteamento = first(ind_loteamento),
    ind_conclusao = first(ind_conclusao),
    ind_aprovacao = first(ind_aprovacao),
    ind_execucao = first(ind_execucao),
    ind_aprovacao_execucao = first(ind_aprovacao_execucao),
    ind_correcao = first(ind_correcao)
  )

# 3. Tipifica alvarás ---------------------------------------------------------

# A - Parcelamentos
atts_ind_parcelamento <- alvaras_trusted_ajust_2 %>%
  # Somente sql's válidos
  filter(ind_sql_incra_null == FALSE) %>%
  # Contabiliza aprovações e execuções em alvará relevante para o sql/lote
  group_by(sql_incra) %>%
  mutate(
    n_aprovacao = length(which(ind_aprovacao == TRUE &
                                 ind_correcao == FALSE &
                                 ind_edificacao_nova == TRUE)),
    n_execucao = length(which(ind_execucao == TRUE &
                                ind_correcao == FALSE &
                                ind_edificacao_nova == TRUE))
  ) %>%
  ungroup() %>%
  # Tipifica sql's com áreas de terreno diferentes em alvarás relevantes de tipo igual
  group_by(sql_incra, descricao_tipo) %>%
  mutate(    
    n_areas_terreno = length(unique(area_do_terreno[
      !is.na(area_do_terreno) & ind_relevante_ajust == TRUE]))
    ) %>%
  ungroup() %>%
  # Somente sql's com algum alvará relevante
  group_by(sql_incra) %>%
  filter(any(ind_edificacao_nova == TRUE)) %>%
  #1 Sql possui alvará do tipo loteamento
  ## Descricao := {Desmembramento, loteamento, desdobro ou remembramento}
  group_by(sql_incra) %>%
  mutate(
    n_loteamento = length(which(ind_loteamento == TRUE) & 
                            ind_correcao == FALSE),
    ind_loteamento = if_else(n_loteamento >= 1, TRUE, FALSE),
    ind_caso_1 = if_else(n_loteamento >= 1 &
                           n_areas_terreno > 1 &
                           n_aprovacao > 1 |
                           n_loteamento >= 1 &
                           n_areas_terreno > 1 &
                           n_execucao > 1, TRUE, FALSE)
  ) %>%
  #2 Sql possui mais de um alvará de tipo conclusão
  group_by(sql_incra) %>%
  mutate(
    n_caso_2 = length(which(ind_conclusao == TRUE &
                              ind_correcao == FALSE)),
    ind_caso_2 = if_else(n_caso_2 > 1 & n_areas_terreno > 1,
                         TRUE, FALSE)
  ) %>%
  #3 Sql com diferentes endereços em alvarás de tipo igual 
  group_by(sql_incra, descricao_tipo) %>%
  mutate(
    n_caso_3 = length(unique(endereco[ind_relevante_ajust == TRUE &
                                        ind_correcao == FALSE])),
    ind_caso_3 = if_else(n_caso_3 > 1 & n_areas_terreno > 1, 
                         TRUE, FALSE)
  ) %>%
  #4 Endereço possui alvará do tipo loteamento
group_by(endereco, setor, quadra) %>%
  mutate(
    n_loteamento = length(which(ind_loteamento == TRUE) & 
                            ind_correcao == FALSE),
    ind_loteamento = if_else(n_loteamento >= 1, TRUE, FALSE),
    ind_caso_4 = if_else(n_loteamento >= 1 &
                           n_areas_terreno > 1 &
                           n_aprovacao > 1 |
                           n_loteamento >= 1 &
                           n_areas_terreno > 1 &
                           n_execucao > 1, TRUE, FALSE)
  ) %>%
  #5 Endereço possui mais de um alvará de tipo conclusão
  group_by(endereco, setor, quadra) %>%
  mutate(
    n_caso_5 = length(which(ind_conclusao == TRUE &
                              ind_correcao == FALSE)),
    ind_caso_5 = if_else(n_caso_5 > 1 & n_areas_terreno > 1,
                         TRUE, FALSE)
  ) %>%
  #6 Endereço com diferentes sql's em alvarás de tipo igual 
  group_by(endereco, setor, quadra, descricao_tipo) %>%
  mutate(
    n_caso_6 = length(unique(sql_incra[ind_relevante_ajust == TRUE &
                                        ind_correcao == FALSE])),
    ind_caso_6 = if_else(n_caso_6 > 1 & n_areas_terreno > 1, 
                         TRUE, FALSE)
  ) %>%
  ungroup() %>%
  group_by(sql_incra) %>%
  mutate(ind_parcelamento = if_else(
    any(ind_relevante_ajust == TRUE) & 
      any(ind_edificacao_nova == TRUE) &
      (any(ind_caso_1 == TRUE) | any(ind_caso_2 == TRUE) |
         any(ind_caso_3 == TRUE)),
    TRUE, FALSE))


# C - Alvarás ajustados
alvaras_trusted_ajust_3 <- alvaras_trusted_ajust_2 %>%
  # Agrega atributos de indicativo de parcelamento
  select(-c(ind_loteamento)) %>%
  left_join(atts_ind_parcelamento) %>%
  select(-starts_with("n_caso_"))


# 4. Agrupa por lote -----------------------------------------------------------


# Caso sql_incra seja inválido, unidade de observação é endereço
alvaras_trusted_por_endereco <- alvaras_trusted_ajust_3 %>%
  filter(ind_sql_incra_null == TRUE) %>%
  group_by(endereco) %>%
  arrange(desc(data_aprovacao)) %>%
  summarize(
    ano_execucao = year(first(data_aprovacao[which(ind_execucao == TRUE)])),
    n_alvaras = n(),
    n_alvaras_relevantes = length(which(ind_relevante_ajust == TRUE)),
    n_aprovacao = length(which(ind_aprovacao == TRUE &
                                 ind_correcao == FALSE &
                                 ind_relevante_ajust == TRUE)),
    n_execucao = length(which(ind_execucao == TRUE &
                                ind_correcao == FALSE &
                                ind_relevante_ajust == TRUE)),
    n_areas_terreno = max(n_areas_terreno),
    data_aprovacao_projeto = first(data_aprovacao[which(ind_aprovacao == TRUE)]),
    data_aprovacao_execucao = first(data_aprovacao[which(ind_execucao == TRUE)]),
    diff_dias_aprovacao = interval(data_aprovacao_projeto, 
                                   data_aprovacao_execucao) / days(1),
    data_autuacao_projeto = first(data_autuacao[which(ind_aprovacao == TRUE)]),
    data_autuacao_execucao = first(data_autuacao[which(ind_execucao == TRUE)]),
    unidade_pmsp = map_chr(list(unique(unidade_pmsp)), ~paste0(.x, collapse = "; ")),
    categoria_de_uso_grupo = as.factor(case_when(
      any(ind_his == TRUE) | any(ind_hmp == TRUE | any(ind_zeis == TRUE)) ~ "ERP",
      any(str_detect(categoria_de_uso,
                     "R2V|R202|R302|R2H|R301|R302|R303|(?<!N)R1|(?<!N)R2")) ~ "ERM",
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
    sql_incra = NA_character_,
    setor = NA_character_,
    quadra = NA_character_,
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
                              categoria_de_uso_grupo != "Outra", TRUE, FALSE),
    ind_parcelamento = if_else(any(ind_parcelamento == TRUE), TRUE, FALSE),
    ind_caso_1 = if_else(any(ind_caso_1 == TRUE), TRUE, FALSE),
    ind_loteamento = if_else(any(ind_loteamento == TRUE), 
                             TRUE, FALSE),
    # n_aprovacao = length(which(ind_aprovacao == TRUE)),
    # n_execucao = length(which(ind_execucao == TRUE)),
    ind_caso_2 = if_else(any(ind_caso_2 == TRUE), TRUE, FALSE),
    ind_caso_3 = if_else(any(ind_caso_3 == TRUE & 
                               ind_relevante == TRUE), TRUE, FALSE),
    ind_caso_4 = if_else(any(ind_caso_4 == TRUE &
                               ind_relevante == TRUE), TRUE, FALSE),
    ind_caso_5 = if_else(any(ind_caso_5 == TRUE &
                               ind_relevante == TRUE), TRUE, FALSE),
    ind_caso_6 = if_else(any(ind_caso_6 == TRUE &
                               ind_relevante == TRUE), TRUE, FALSE),
    ind_conclusao = if_else(any(ind_conclusao == TRUE), TRUE, FALSE)
  ) %>%
  ungroup()

# Caso sql_incra seja válido, unidade de observação é sql_incra
alvaras_trusted_por_sql_incra <- alvaras_trusted_ajust_3 %>%
  filter(ind_sql_incra_null == FALSE) %>%
  group_by(sql_incra) %>%
  arrange(desc(data_aprovacao)) %>% # importante para inferir que posição 1 é sempre data mais atual
  summarize(
    ano_execucao = year(first(data_aprovacao[which(ind_execucao == TRUE)])),
    n_alvaras = n(),
    n_alvaras_relevantes = length(which(ind_relevante_ajust == TRUE)),
    n_aprovacao = length(which(ind_aprovacao == TRUE &
                                 ind_correcao == FALSE &
                                 ind_relevante_ajust == TRUE)),
    n_execucao = length(which(ind_execucao == TRUE &
                                ind_correcao == FALSE &
                                ind_relevante_ajust == TRUE)),
    n_areas_terreno = max(n_areas_terreno),
    data_aprovacao_projeto = first(data_aprovacao[which(ind_aprovacao == TRUE)]),
    data_aprovacao_execucao = first(data_aprovacao[which(ind_execucao == TRUE)]),
    diff_dias_aprovacao = interval(data_aprovacao_projeto, 
                                   data_aprovacao_execucao) / days(1),
    data_autuacao_projeto = first(data_autuacao[which(ind_aprovacao == TRUE)]),
    data_autuacao_execucao = first(data_autuacao[which(ind_execucao == TRUE)]),
    unidade_pmsp = map_chr(list(unique(unidade_pmsp)), ~paste0(.x, collapse = "; ")),
    categoria_de_uso_grupo = as.factor(case_when(
      any(ind_his == TRUE) | any(ind_hmp == TRUE | any(ind_zeis == TRUE)) ~ "ERP",
      any(str_detect(categoria_de_uso,
                     "R2V|R202|R302|R2H|R301|R302|R303|(?<!N)R1|(?<!N)R2")) ~ "ERM",
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
    setor = first(setor),
    quadra = first(quadra),
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
                              categoria_de_uso_grupo != "Outra", TRUE, FALSE),
    ind_parcelamento = if_else(any(ind_parcelamento == TRUE), TRUE, FALSE),
    ind_caso_1 = if_else(any(ind_caso_1 == TRUE), TRUE, FALSE),
    ind_loteamento = if_else(any(ind_loteamento == TRUE), 
                             TRUE, FALSE),
    # n_aprovacao = length(which(ind_aprovacao == TRUE)),
    # n_execucao = length(which(ind_execucao == TRUE)),
    ind_caso_2 = if_else(any(ind_caso_2 == TRUE), TRUE, FALSE),
    ind_caso_3 = if_else(any(ind_caso_3 == TRUE & 
                               ind_relevante == TRUE), TRUE, FALSE),
    ind_caso_4 = if_else(any(ind_caso_4 == TRUE &
                               ind_relevante == TRUE), TRUE, FALSE),
    ind_caso_5 = if_else(any(ind_caso_5 == TRUE &
                               ind_relevante == TRUE), TRUE, FALSE),
    ind_caso_6 = if_else(any(ind_caso_6 == TRUE &
                               ind_relevante == TRUE), TRUE, FALSE),
    ind_conclusao = if_else(any(ind_conclusao == TRUE), TRUE, FALSE)
  ) %>%
  ungroup()

# Cria alvaras por lote 
# Com ajuste temos 30484 observações
alvaras_trusted_por_lote <- alvaras_trusted_por_endereco %>%
  select(-endereco) %>%
  full_join(alvaras_trusted_por_sql_incra, by = names(.)) %>%
  mutate(id = row_number()) %>%
  select(id, everything())

#
# att_ind_endereco_null %>%
#   filter(ind_endereco_null == TRUE) %>%
#   view()

# 5. Analisa --------------------------------------------------------------

# Selecionando variáveis de interesse
alvaras_por_lote <- alvaras_trusted_por_lote %>%
  # filter(ind_aprovacao == TRUE & ind_execucao == TRUE) %>%
  # filter(categoria_de_uso_grupo != "Outra") %>% # Somente usos residenciais!
  # filter(ano_execucao < 2022) %>%
  transmute(id, 
            ano_execucao,
            n_alvaras_relevantes,
            n_aprovacao,
            n_execucao,
            n_areas_terreno,
            diff_dias_aprovacao,
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
            setor,
            quadra,
            endereco_ultimo,
            bairro,
            zona_de_uso_registro,
            zona_de_uso_anterior,
            ind_aprovacao,
            ind_execucao,
            ind_uso_misto,
            ind_r2v,
            ind_r2h,
            ind_his,
            ind_hmp,
            ind_zeis,
            ind_parcelamento,
            ind_caso_1,
            ind_caso_2,
            ind_caso_3,
            ind_caso_4,
            ind_caso_5,
            ind_caso_6,
            ind_conclusao,
            ind_loteamento
            )

# A - Indicativo de parcelamento -> 37 (0.3%)


# Caso 1 - Desmembramento, loteamento, desdobro ou remembramento -> 37 (0.3%)
# Antes - Desmembramento, loteamento, desdobro ou remembramento -> 61 (0.5%)
alvaras_por_lote %>%
  count(ind_caso_1) %>% 
  mutate(freq = (n / sum(n)) * 100)

## Caso 1.1 - Desmembramento, loteamento, desdobro ou remembramento -> 61 (0.5%)
alvaras_por_lote %>%
  count(ind_loteamento) %>% 
  mutate(freq = (n / sum(n)) * 100)

## Caso 1.2 - Mais de um alvará de aprovação -> 472 [3617 (30.5%)]
alvaras_por_lote %>%
  count(n_aprovacao > 1) %>%
  mutate(freq = (n / sum(n)) * 100)

## Caso 1.3 - Mais de um alvará de execução -> 371 [3262 (27.5%)]
alvaras_por_lote %>%
  count(n_execucao > 1) %>%
  mutate(freq = (n / sum(n)) * 100)

## Caso 1.4 - Mais de uma área de terreno -> 1057 (8.9%)
alvaras_por_lote %>%
  count(n_areas_terreno > 1) %>%
  mutate(freq = (n / sum(n)) * 100)

# B - Indicativo de parcelamento provável -> 136 (1.2%)

# Caso 2 - Mais de uma conclusão -> 12 (0.1%)
## Antes - Mais de uma conclusão -> 141 (1.2%)
alvaras_por_lote %>%
  count(ind_caso_2) %>% 
  mutate(freq = (n / sum(n)) * 100)

# Caso 3 - Endereço diferente, SQL igual + descricao igual -> 107 (0.9%)
alvaras_por_lote %>%
  count(ind_caso_3) %>% 
  mutate(freq = (n / sum(n)) * 100)

# A + B
alvaras_por_lote %>%
  count(ind_parcelamento) %>% 
  mutate(freq = (n / sum(n)) * 100)

# Por endereço

# Caso 4 
alvaras_por_lote %>%
  count(ind_caso_4) %>%
  mutate(freq = (n / sum(n)) * 100)

## Caso 5
alvaras_por_lote %>%
  count(ind_caso_5) %>%
  mutate(freq = (n / sum(n)) * 100)

## Caso 6 
alvaras_por_lote %>%
  count(ind_caso_6) %>%
  mutate(freq = (n / sum(n)) * 100)


# F - Indicativo de conclusao -> 298 (2.5%)
# Antes - Indicativo de conclusao -> 452 (3.8%)
alvaras_por_lote %>%
  count(ind_conclusao) %>%
  mutate(freq = (n / sum(n)) * 100)

# 0 somente aprovação, 333 somente execução e 11860 com aprovação e execução
alvaras_trusted_por_lote %>%
  filter(categoria_de_uso_grupo != "Outra") %>%
  filter(ano_execucao < 2022) %>%
  count(ind_aprovacao, ind_execucao)

# Sem filtrar ano de 2022, temos 945 casos com aprovação mas sem execução 
alvaras_trusted_por_lote %>%
  # filter(ano_execucao < 2022) %>%
  filter(categoria_de_uso_grupo != "Outra") %>% # Somente usos residenciais!
  count(ind_aprovacao, ind_execucao)


# 6. Estuda casos --------------------------------------------------------


# Caso 1 - Alvará de loteamento + Mais de 1 alvará aprovação/execução + 
# Diferentes Áreas de Terreno

#
alvaras_por_lote %>% 
  filter(ind_caso_1 == TRUE) %>%
  # pull(unique(sql_incra))
  view()

#
## R2V - CYRELA CONSTRUTORA LTDA - AV GIOVANNI GRONCH - VILA ANDRADE
view(alvaras_trusted_ajust_3[alvaras_trusted_ajust_2$sql_incra=="301.054.0059-6",])
## R2H - R JUCARAL - ITAQUERA
view(alvaras_trusted_ajust_3[alvaras_trusted_ajust_2$sql_incra=="138.044.0002-6",])
# HIS - AV OSVALDO VALLE CORDEIRO 0 - PRQ SAVOY CITY
view(alvaras_trusted_ajust_3[alvaras_trusted_ajust_2$sql_incra=="47.327.0054-2",])

# Caso 2 - Mais de uma conclusão
#
alvaras_por_lote %>%
  filter(ind_caso_2 == TRUE) %>% 
  view()

# HIS - CDHU - R DO BUCOLISMO/R SAMPAIO MOREIRA
view(alvaras_trusted_ajust_2[alvaras_trusted_ajust_2$sql_incra=="002.049.0045-1",])
# R2V/NR2 - R MICHIGAN/ R NOVA YORK - ITAIM BIBI/ BROOKLIN PAULISTA
view(alvaras_trusted_ajust_2[alvaras_trusted_ajust_2$sql_incra=="085.124.0001-3",])
# HIS - CONSTRUTORA E INCORPORADORA FALEIROS LTDA - R CHUBEI TAKAGASHI - JOSÉ BONIFACIO
view(alvaras_trusted_ajust_2[alvaras_trusted_ajust_2$sql_incra=="6383580031313",])


#Ind Caso 5 - Mesmo endereço, diferente SQL 

# 
alvaras_por_lote %>% 
  filter(ind_caso_5 == TRUE) %>%
  # pull(unique(sql_incra))
  view()

# ALBERT BARTHOLOME
# AL CASA BRANCA 939
# AV CANGAIBA | 110.102.0013-4

# View(alvaras_ajust[alvaras_ajust$endereco=="AV RIO BRANCO 125",])
# View(alvaras_ajust[alvaras_ajust$endereco=="AV RANGEL PESTANA 1016",])
# View(alvaras_ajust[alvaras_ajust$sql_incra=="003.011.0165-5",])
# View(alvaras_ajust[alvaras_ajust$endereco=="AV RANGEL PESTANA 0",])
# View(alvaras_ajust[alvaras_ajust$endereco=="R CESARIO RAMALHO 237",])
# View(alvaras_ajust[alvaras_ajust$endereco=="R CAPITAO MESSIAS 35",])
# View(alvaras_ajust[alvaras_ajust$endereco=="R QUIXADA 80",])

# Caso chave passasse a ser endereço reduziriamos de 773 para 541
alvaras_por_lote %>% 
  filter(ind_caso_6 == TRUE) %>%
  count(n_distinct(endereco_ultimo))

#
caso_6 <- alvaras_por_lote %>% 
  filter(ind_caso_6 == TRUE) %>%
  pull(unique(endereco_ultimo))


# 31659 sendo 541 casos 5
alvaras_trusted_ajust_3 %>%
  filter(ind_sql_incra_null == FALSE) %>%
  mutate(caso_6 = if_else(endereco %in% caso_6, TRUE, FALSE)) %>%
  group_by(endereco, caso_6) %>%
  summarize(n = 1) %>%
  ungroup() %>%
  summarize(sum(n),
            sum(n[caso_6 == TRUE]))

# 30374 sendo 953 casos 5
alvaras_trusted_ajust_3 %>%
  filter(ind_sql_incra_null == FALSE) %>%
  mutate(caso_6 = if_else(endereco %in% caso_6, TRUE, FALSE)) %>%
  group_by(sql_incra, caso_6) %>%
  summarize(n = 1) %>%
  ungroup() %>%
  summarize(sum(n),
            sum(n[caso_6 == TRUE]))
  
# 7. Estuda tratamento --------------------------------------------------------

#
alvaras_trusted_ajust_parcelamentos <- alvaras_trusted_ajust_3 %>%
  # Somente casos com SQL válido
  filter(ind_sql_incra_null == FALSE) %>%
  # Somente casos relevantes e de interesse 
  group_by(sql_incra) %>%
  filter(any(ind_relevante_ajust == TRUE) & 
           any(ind_edificacao_nova == TRUE) &
           (any(ind_caso_1 == TRUE) | any(ind_caso_2 == TRUE) |
              any(ind_caso_3 == TRUE))) %>%
  ungroup() %>%
  # Ordena mais recente para mais antigo
  arrange(desc(data_aprovacao)) %>%
  # Toma valor antigo para comparação
  group_by(sql_incra) %>%
  mutate(
    ano_execucao = year(first(data_aprovacao[which(ind_execucao == TRUE)])),
    area_do_terreno_1 =  first(na.omit(
      area_do_terreno[which(ind_aprovacao == TRUE | ind_execucao == TRUE)])),
    n_unidades_1 = first(na.omit(
      n_unidades[which(ind_aprovacao == TRUE | ind_execucao == TRUE)]))
  ) %>%
  # Agrupa por Área de terreno e pega informação do mais recente por grupo
  ## Grupo = mesmo sql, mesmo tipo alvará, mesma área de terreno
  group_by(sql_incra, descricao_tipo, area_do_terreno) %>%
  summarize(
    ano_execucao = first(ano_execucao),
    endereco = first(endereco),
    ind_caso_1 = first(ind_caso_1),
    ind_caso_2 = first(ind_caso_2),
    ind_caso_3 = first(ind_caso_3),
    area_do_terreno_antes = first(area_do_terreno_1), 
    area_do_terreno_2 = first(na.omit(
      area_do_terreno[which(
        (ind_aprovacao == TRUE))])),
    n_unidades_antes = first(n_unidades_1),
    n_unidades_2 = first(na.omit(
      n_unidades[which(
        (ind_aprovacao == TRUE | ind_execucao == TRUE) &
          !is.na(area_do_terreno))])),
    n = length(which(!is.na(area_do_terreno) &
                       (ind_aprovacao == TRUE | ind_execucao == TRUE) &
                       ind_correcao == FALSE &
                       !is.na(n_unidades))),
    n_aprovacao = first(n_aprovacao),
    n_execucao = first(n_execucao)
  ) %>%
  # Somas os valores dos agrupamentos 
  group_by(sql_incra) %>%
  summarize(
    ano_execucao = first(ano_execucao),
    endereco = first(endereco),
    across(c(ind_caso_1, ind_caso_2, ind_caso_3),
           ~if_else(any(.x == TRUE), TRUE, FALSE)),
    area_do_terreno_antes = first(area_do_terreno_antes), 
    area_do_terreno_depois = sum(area_do_terreno_2, na.rm = TRUE),
    n_unidades_antes = first(n_unidades_antes), 
    n_unidades_depois = sum(n_unidades_2, na.rm = TRUE),
    diff_area_do_terreno = area_do_terreno_depois - area_do_terreno_antes,
    diff_unidades = n_unidades_depois - n_unidades_antes,
    ## Identifica quantas informações de unidade foram somadas
    ## Identifica quantos alvará de Execução disponível
    n = sum(n),
    n_aprovacao = max(n_aprovacao),
    diff_n = n_aprovacao - n,
    n_execucao = max(n_execucao),
    diff_alvaras = n_execucao - n_aprovacao,
  ) %>%
  arrange(desc(diff_unidades))

# #
# alvaras_trusted_ajust_3 %>%
#   group_by(sql_incra) %>%
#   filter(any(ind_relevante_ajust == TRUE) & 
#            (any(ind_caso_1 == TRUE) | any(ind_caso_2 == TRUE) |
#               any(ind_caso_3 == TRUE))
#          ) %>%
#   arrange(desc(data_aprovacao)) %>%
#   group_by(setor, quadra, area_do_terreno) %>%
#   mutate(sql_incra_original = case_when(
#     n_aprovacao > n_execucao ~ last(sql_incra),
#     TRUE ~ NA_character_
#     ),
#     diff_sql = if_else(sql_incra != sql_incra_original, TRUE, FALSE)) %>%
#   select(sql_incra, sql_incra_original, diff_sql, endereco, everything()) %>%
#   view()


# Hipóteses a verificar:
#   - N < Aprovação deve-se principalmente a dados faltantes
# - Não há N > Aprovação, conforme esperado
# - Aprovação > Execução é porque não foi executado mesmo
# - Aprovação < Execução são dois casos que parecem erros de registro 


# Caso 1
# 066.122.0001-4 (aqui parece funcionar)
# 157.229.0050-7 (neste caso o que parece estar subestimado é área de terreno!)
# 197.034.0024-7 (ok!)
# 098.030.0017-9 (ok!)
# 124.157.0095-8 (caso com maior diferença de unidades (3147!), mas parece funcionar)
# 077.001.0001-2 (antes 0, pois mais recente = 0; registro dúbio parece duplicar unidades)

# Caso 2
# 085.124.0001-3 (conclusão de reforma, mas área do terreno parece funcionar)
# 154.231.0001-0 (ok!)
# 6383580031313 (ok!)

# Caso 3
# 133.278.0003-1/ 133.278.0004-8
# 087.315.0001-3/ 234.035.0002-9
# 028.046.0132-3 / 028.046.0322-9 / 028.046.0321-0
# 197.034.0024-7/ 197.061.0001-5 / 197.060.0001-0 [Jardim das Perdizes]

#
alvaras_trusted_ajust_parcelamentos_2 <- alvaras_trusted_ajust_3 %>%
  # Somente casos com SQL válido
  filter(ind_sql_incra_null == FALSE) %>%
  # Somente casos relevantes e de interesse 
  group_by(endereco) %>%
  filter(any(ind_relevante_ajust == TRUE) & 
           (any(ind_caso_4 == TRUE) | any(ind_caso_5 == TRUE) |
              any(ind_caso_6 == TRUE))) %>%
  ungroup() %>%
  # Ordena mais recente para mais antigo
  arrange(desc(data_aprovacao)) %>%
  # Toma valor antigo para comparação
  group_by(endereco) %>%
  mutate(
    area_do_terreno_1 =  first(na.omit(
      area_do_terreno[which(ind_aprovacao == TRUE | ind_execucao == TRUE)])),
    n_unidades_1 = first(na.omit(
      n_unidades[which(ind_aprovacao == TRUE | ind_execucao == TRUE)]))
  ) %>%
  # Agrupa por Área de terreno e pega informação do mais recente por grupo
  ## Grupo = mesmo sql, mesmo tipo alvará, mesma área de terreno
  group_by(endereco, descricao_tipo, area_do_terreno) %>%
  summarize(
    ind_caso_4 = first(ind_caso_4),
    ind_caso_5 = first(ind_caso_5),
    ind_caso_6 = first(ind_caso_6),
    area_do_terreno_antes = first(area_do_terreno_1), 
    area_do_terreno_2 = first(na.omit(
      area_do_terreno[which(
        (ind_aprovacao == TRUE))])),
    n_unidades_antes = first(n_unidades_1),
    n_unidades_2 = first(na.omit(
      n_unidades[which(
        (ind_aprovacao == TRUE | ind_execucao == TRUE) &
          !is.na(area_do_terreno))])),
    n = length(which(!is.na(area_do_terreno) &
                       (ind_aprovacao == TRUE | ind_execucao == TRUE) &
                       ind_correcao == FALSE &
                       !is.na(n_unidades))),
    n_aprovacao = first(n_aprovacao),
    n_execucao = first(n_execucao)
  ) %>%
  # Somas os valores dos agrupamentos 
  group_by(endereco) %>%
  summarize(
    across(c(ind_caso_4, ind_caso_5, ind_caso_6),
           ~if_else(any(.x == TRUE), TRUE, FALSE)),
    area_do_terreno_antes = first(area_do_terreno_antes), 
    area_do_terreno_depois = sum(area_do_terreno_2, na.rm = TRUE),
    n_unidades_antes = first(n_unidades_antes), 
    n_unidades_depois = sum(n_unidades_2, na.rm = TRUE),
    diff_area_do_terreno = area_do_terreno_depois - area_do_terreno_antes,
    diff_unidades = n_unidades_depois - n_unidades_antes,
    ## Identifica quantas informações de unidade foram somadas
    ## Identifica quantos alvará de Execução disponível
    n = sum(n),
    n_aprovacao = max(n_aprovacao),
    diff_n = n_aprovacao - n,
    n_execucao = max(n_execucao),
    diff_alvaras = n_execucao - n_aprovacao,
  ) %>%
  arrange(desc(diff_unidades))


# Alguns endereços não constam no dado por endereço
alvaras_trusted_ajust_parcelamentos %>%
  anti_join(alvaras_trusted_ajust_parcelamentos_2, by = "endereco") %>%
  view()

# Alguns endereços não constam no dado por sql
alvaras_trusted_ajust_parcelamentos_2 %>%
  anti_join(alvaras_trusted_ajust_parcelamentos, by = "endereco") %>%
  view()



# 8. Trata casos ----------------------------------------------------------

# Identifica e trata casos de parcelamento
# Validado em uma série de reuniões Insper/Abrainc entre nov-dez de 22
atts_ind_parcelamento <- alvaras_trusted_ajust_2 %>%
  
  # A - Tipifica parcelamento
  left_join(
    # Unifica com atributo novo - Indicador de parcelamento
    alvaras_trusted_ajust_2 %>%
      # Somente sql's válidos
      filter(ind_sql_incra_null == FALSE) %>%
      # Contabiliza aprovações e execuções em alvará relevante para o sql/lote
      group_by(sql_incra) %>%
      mutate(
        n_aprovacao = length(which(ind_aprovacao == TRUE &
                                     ind_correcao == FALSE &
                                     ind_edificacao_nova == TRUE)),
        n_execucao = length(which(ind_execucao == TRUE &
                                    ind_correcao == FALSE &
                                    ind_edificacao_nova == TRUE))
      ) %>%
      ungroup() %>%
      # Tipifica sql's com áreas de terreno diferentes em alvarás relevantes de tipo igual
      group_by(sql_incra, descricao_tipo) %>%
      mutate(    
        n_areas_terreno = length(unique(area_do_terreno[
          !is.na(area_do_terreno) & ind_relevante_ajust == TRUE]))
      ) %>%
      ungroup() %>%
      # Somente sql's com algum alvará relevante
      group_by(sql_incra) %>%
      filter(any(ind_edificacao_nova == TRUE)) %>%
      #1 Sql possui alvará do tipo loteamento
      ## Descricao := {Desmembramento, loteamento, desdobro ou remembramento}
      group_by(sql_incra) %>%
      mutate(
        n_loteamento = length(which(descricao_tipo == "LOTEAMENTO") & 
                                ind_correcao == FALSE),
        ind_loteamento = if_else(n_loteamento >= 1, TRUE, FALSE),
        ind_caso_1 = if_else(n_loteamento >= 1 &
                               n_areas_terreno > 1 &
                               n_aprovacao > 1 |
                               n_loteamento >= 1 &
                               n_areas_terreno > 1 &
                               n_execucao > 1, TRUE, FALSE)
      ) %>%
      #2 Sql possui mais de um alvará de tipo conclusão
      group_by(sql_incra) %>%
      mutate(
        n_caso_2 = length(which(ind_conclusao == TRUE &
                                  ind_correcao == FALSE)),
        ind_caso_2 = if_else(n_caso_2 > 1 & n_areas_terreno > 1,
                             TRUE, FALSE)
      ) %>%
      #3 Sql com diferentes endereços em alvarás de tipo igual 
      group_by(sql_incra, descricao_tipo) %>%
      mutate(
        n_caso_3 = length(unique(endereco[ind_relevante_ajust == TRUE &
                                            ind_correcao == FALSE])),
        ind_caso_3 = if_else(n_caso_3 > 1 & n_areas_terreno > 1, 
                             TRUE, FALSE)
      ) %>%
      ungroup() %>%
      # Cria Indicador de parcelamento
      group_by(sql_incra) %>%
      mutate(ind_parcelamento = if_else(
        any(ind_edificacao_nova == TRUE) &
          (any(ind_caso_1 == TRUE) | any(ind_caso_2 == TRUE) |
             any(ind_caso_3 == TRUE)),
        TRUE, FALSE)) %>%
      # Exclui atributos desnecessários
      select(-starts_with("n_caso"), - starts_with("ind_caso_"))
  ) %>%
  
  # B - Trata em separado atributos numéricos em casos de parcelamento
  # Somente parcelamentos
  group_by(sql_incra) %>%
  filter(any(ind_parcelamento == TRUE)) %>%
  ungroup() %>%
  # Ordena mais recente para mais antigo
  arrange(desc(data_aprovacao)) %>%
  # Agrupa por Área de terreno e pega informação do mais recente por grupo
  ## Grupo = mesmo sql, mesmo tipo alvará, mesma área de terreno
  group_by(sql_incra, descricao_tipo, area_do_terreno) %>%
  summarize(
    area_do_terreno = first(na.omit(
      area_do_terreno[which(ind_aprovacao == TRUE | ind_execucao == TRUE |
                              ind_conclusao == TRUE)])),
    area_da_construcao = first(na.omit(
      area_da_construcao[which(ind_aprovacao == TRUE | ind_execucao == TRUE |
                                 ind_conclusao == TRUE)])),  
    n_blocos = first(na.omit(
      n_blocos[which(ind_aprovacao == TRUE | ind_execucao == TRUE)])),
    n_pavimentos = first(na.omit(
      n_pavimentos[which(ind_aprovacao == TRUE | ind_execucao == TRUE)])),
    n_unidades = first(na.omit(
      n_unidades[which(ind_aprovacao == TRUE | ind_execucao == TRUE)])),
  ) %>%
  # Somas os valores dos agrupamentos 
  group_by(sql_incra) %>%
  summarize(
    across(where(is.numeric), ~ sum(.x, na.rm = TRUE)),
    n_pavimentos_por_bloco = n_pavimentos/n_blocos,
    n_unidades_por_bloco = n_unidades/n_blocos
  ) %>%
  ungroup()


# Atualiza atributos numéricos em casos de parcelamento
alvaras_trusted_por_sql_incra_ajust <- 
  # Unifica por observação/ linhas
  bind_rows(
    # Não-parcelamentos
    alvaras_trusted_por_sql_incra %>%
      filter(!sql_incra %in% atts_ind_parcelamento$sql_incra),
    # Parcelamentos
    alvaras_trusted_por_sql_incra %>%
      filter(sql_incra %in% atts_ind_parcelamento$sql_incra) %>%
      select(-c(
        area_do_terreno, area_da_construcao, n_blocos,n_pavimentos,
        n_unidades, n_pavimentos_por_bloco, n_unidades_por_bloco )) %>%
      left_join(atts_ind_parcelamento)
  )

#
list(alvaras_trusted_por_sql_incra["n_unidades"], 
     alvaras_trusted_por_sql_incra_ajust["n_unidades"]) %>%
  set_names(c("antes", "depois")) %>%
  map_df(~sum(.x, na.rm = TRUE)) %>%
  mutate(diff = depois - antes)

# 9. Exporta --------------------------------------------------------------

# Dados tratados em formato .xlsx
writexl::write_xlsx(alvaras_por_lote %>%
                      filter(ind_parcelamento == TRUE),
                    here("inputs", "4_refined", "Alvaras", 
                         "alvaras_por_lote_parcelamentos.xlsx"))

# Dados tratados em formato .xlsx
writexl::write_xlsx(tipologia_descricao,
                    here("inputs", "4_refined", "Alvaras", 
                         "tipologia_descricao.xlsx"))

#
beep(8)
