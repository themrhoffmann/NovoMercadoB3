# ==============================================================================
# Script para Análise de Risco Sistemático (Beta) por Segmento de Governança B3
# Autor: Marcos Rafael Goffmann
# Data Modificação: 2025-04-04
#
# Descrição:
# Este script lê uma lista de tickers e seus segmentos de listagem de um arquivo
# CSV, baixa dados históricos de preços (Yahoo Finance), calcula o Beta médio
# de cada segmento em relação a um benchmark equi-ponderado do segmento
# Tradicional, e gera tabelas e gráficos com os resultados.
# ==============================================================================

# --- 0. Configuração do Ambiente ---

# Descomente a linha abaixo para instalar os pacotes se for a primeira vez
# install.packages(c("BatchGetSymbols", "dplyr", "lubridate", "PerformanceAnalytics", "tidyr", "ggplot2", "xts", "readr", "stringr", "here"))

# Carregar Pacotes Necessários
library(BatchGetSymbols)
library(dplyr)
library(lubridate)
library(PerformanceAnalytics)
library(tidyr)
library(ggplot2)
library(xts)
library(readr)
library(stringr)
library(here) # Para caminhos relativos robustos

# Para Reprodutibilidade
set.seed(123) # Define uma semente para processos aleatórios (boa prática)

# Recomendações para Gerenciamento de Ambiente (Execute no Console se necessário):
# 1. Usar 'renv' para isolar dependências do projeto:
#    # renv::init() # Inicializa o renv (cria pasta 'renv' e arquivo 'renv.lock')
#    # renv::snapshot() # Salva o estado atual dos pacotes no renv.lock
# 2. Ou, pelo menos, registrar a sessão para referência futura:
#    # sessionInfo()
#    # writeLines(capture.output(sessionInfo()), here("results", "sessionInfo.txt")) # Salva info da sessão

# --- 1. Parâmetros da Análise ---

data_inicio <- as.Date("2000-01-01")
# !! AJUSTE NECESSÁRIO: Defina a data final EXATA da sua análise !!
data_fim_analise <- as.Date("2024-12-31") # Exemplo: fim de 2024. SUBSTITUA PELA SUA DATA.
print(paste("Período de análise definido:", format(data_inicio, "%Y-%m-%d"), "a", format(data_fim_analise, "%Y-%m-%d")))

# Caminho relativo para o arquivo CSV (espera-se que esteja em uma subpasta 'data')
# !! CERTIFIQUE-SE que a pasta 'data' existe e contém 'Tickers.csv' !!
caminho_csv_relativo <- here("data", "Tickers.csv")

# Diretório para salvar os resultados
dir_resultados <- here("results")
if (!dir.exists(dir_resultados)) {
  dir.create(dir_resultados, recursive = TRUE)
  print(paste("Pasta de resultados criada em:", dir_resultados))
}

# --- 2. Leitura e Processamento dos Tickers e Segmentos ---

print(paste("Tentando ler arquivo CSV de:", caminho_csv_relativo))

if (!file.exists(caminho_csv_relativo)) {
  stop(paste("ERRO: Arquivo CSV não encontrado em:", caminho_csv_relativo,
             "\nCertifique-se que a pasta 'data' existe no diretório do projeto e contém 'Tickers.csv'."))
}

# Tentar ler o CSV com tratamento de erro e encoding
tryCatch({
  df_tickers_segmentos_csv <- NULL
  tryCatch({
    df_tickers_segmentos_csv <- read_delim(caminho_csv_relativo, delim = ";", locale = locale(encoding = "Latin1"), show_col_types = FALSE)
    print("CSV lido com sucesso usando Latin1 encoding.")
  }, error = function(e_latin1){
    warning("Falha ao ler CSV com Latin1, tentando UTF-8...")
    tryCatch({
      df_tickers_segmentos_csv <<- read_delim(caminho_csv_relativo, delim = ";", locale = locale(encoding = "UTF-8"), show_col_types = FALSE)
      print("CSV lido com sucesso usando UTF-8 encoding.")
    }, error = function(e_utf8){
      stop(paste("Erro ao ler CSV com Latin1 e UTF-8. Verifique o arquivo, o caminho e a codificação. Erro Original:", e_utf8$message))
    })
  })
  
  # Verificar colunas e renomear
  expected_cols <- c("Tickers", "Segmentos")
  actual_cols <- colnames(df_tickers_segmentos_csv)
  if (!all(tolower(expected_cols) %in% tolower(actual_cols))) {
    stop(paste("ERRO: Colunas 'Tickers' e/ou 'Segmentos' não encontradas no CSV. Colunas encontradas:", paste(actual_cols, collapse=", ")))
  }
  colnames(df_tickers_segmentos_csv)[tolower(colnames(df_tickers_segmentos_csv)) == "tickers"] <- "Ticker"
  colnames(df_tickers_segmentos_csv)[tolower(colnames(df_tickers_segmentos_csv)) == "segmentos"] <- "Segmento_Original"
  
}, error = function(e) {
  stop(paste("Erro ao ler ou processar o arquivo CSV:", e$message))
})

print(paste("Arquivo CSV lido. Total de linhas:", nrow(df_tickers_segmentos_csv)))

# Limpar e Mapear Segmentos, Formatar Tickers
df_processado <- df_tickers_segmentos_csv %>%
  select(Ticker, Segmento_Original) %>%
  filter(!is.na(Ticker), !is.na(Segmento_Original), Ticker != "", Segmento_Original != "") %>%
  mutate(
    Ticker = str_trim(Ticker),
    Segmento_Original = str_trim(Segmento_Original),
    # Mapeamento dos nomes de segmentos para categorias padronizadas
    Segmento_Limpo = case_when(
      str_detect(Segmento_Original, regex("NOVO MERCADO", ignore_case = TRUE)) ~ "NovoMercado",
      str_detect(Segmento_Original, regex("NIVEL 2", ignore_case = TRUE)) ~ "Nivel2",
      str_detect(Segmento_Original, regex("NIVEL 1", ignore_case = TRUE)) ~ "Nivel1",
      str_detect(Segmento_Original, regex("M2", ignore_case = TRUE)) ~ "BovespaMaisN2", # Considerar se agrupa com N2 ou Mais
      str_detect(Segmento_Original, regex("BOVESPA MAIS", ignore_case = TRUE)) ~ "BovespaMais",
      str_detect(Segmento_Original, regex("BOLSA", ignore_case = TRUE)) ~ "Tradicional", # Assumindo que "BOLSA" significa Tradicional
      str_detect(Segmento_Original, regex("MERCADO DE BALCÃO", ignore_case = TRUE)) | str_detect(Segmento_Original, regex("SOMA", ignore_case = TRUE)) ~ "OTC", # Agrupando Balcão
      TRUE ~ paste0("NaoMapeado_", Segmento_Original) # Marcar claramente não mapeados
    ),
    # Adicionar sufixo .SA se necessário
    Ticker_SA = ifelse(!str_ends(Ticker, fixed(".SA", ignore_case = TRUE)),
                       paste0(toupper(Ticker), ".SA"),
                       toupper(Ticker))
  ) %>%
  # Filtrar segmentos não desejados (ex: Balcão se não for objeto do estudo)
  filter(Segmento_Limpo != "OTC") %>% # Remover Balcão/SOMA, ajuste se necessário
  select(Ticker_SA, Segmento_Limpo) %>%
  distinct()

# Verificar segmentos não mapeados (excluindo os marcados explicitamente)
segmentos_nao_mapeados <- df_processado %>%
  filter(str_starts(Segmento_Limpo, "NaoMapeado_")) %>%
  distinct(Segmento_Limpo)
if(nrow(segmentos_nao_mapeados) > 0){
  warning("Os seguintes nomes de segmento do CSV não foram mapeados para categorias padrão e serão ignorados: ",
          paste(segmentos_nao_mapeados$Segmento_Limpo, collapse=", "),
          ". Verifique a seção 'case_when' no código ou o conteúdo do CSV.")
  # Filtrar os não mapeados
  df_processado <- df_processado %>%
    filter(!str_starts(Segmento_Limpo, "NaoMapeado_"))
}

print(paste("Tickers processados e válidos após limpeza:", nrow(df_processado)))
print("Distribuição por segmento processado:")
print(table(df_processado$Segmento_Limpo))

# Criar a lista tickers_por_segmento
tickers_por_segmento <- df_processado %>%
  group_by(Segmento_Limpo) %>%
  summarise(Tickers = list(Ticker_SA), .groups = 'drop') %>%
  tibble::deframe()

# Remover segmentos que podem ter ficado vazios após filtros
tickers_por_segmento <- tickers_por_segmento[sapply(tickers_por_segmento, length) > 0]

# Lista única de todos os tickers para download
todos_tickers_unicos <- unique(unlist(tickers_por_segmento))
if(length(todos_tickers_unicos) == 0) {
  stop("A lista final de tickers está vazia após processar o CSV.")
}
todos_tickers <- todos_tickers_unicos

# --- 3. Obtenção de Dados Históricos (Yahoo Finance) ---

print(paste("Iniciando download para", length(todos_tickers), "tickers (baseado no CSV processado)..."))
print(paste("Período:", format(data_inicio, "%Y-%m-%d"), "a", format(data_fim_analise, "%Y-%m-%d")))

# Opções para BatchGetSymbols
options(BatchGetSymbols.max.attempts = 5, BatchGetSymbols.download.timeout = 300)
# Usar um cache persistente dentro do projeto (opcional, mas bom para desenvolvimento)
# cache_folder_path <- here("BGS_cache")
# if (!dir.exists(cache_folder_path)) dir.create(cache_folder_path)
# Usar cache temporário padrão:
cache_folder_path <- file.path(tempdir(), paste0("B3_cache_", format(Sys.Date(), "%Y%m%d")))

df_precos <- BatchGetSymbols(
  tickers = todos_tickers,
  first.date = data_inicio,
  last.date = data_fim_analise, # Usar a data final definida
  freq.data = "daily",
  cache.folder = cache_folder_path,
  be.quiet = FALSE # Mostrar progresso
)

# Verificar tickers baixados e faltantes
tickers_baixados <- character(0)
if (!is.null(df_precos) && !is.null(df_precos$df.tickers) && nrow(df_precos$df.tickers) > 0) {
  tickers_baixados <- unique(df_precos$df.tickers$ticker)
  print(paste("Download concluído. Tickers com dados obtidos:", length(tickers_baixados)))
} else {
  warning("Nenhum ticker foi baixado com sucesso ou a estrutura de retorno está inesperada.")
}
tickers_faltantes_download <- setdiff(todos_tickers, tickers_baixados)

if(length(tickers_faltantes_download) > 0) {
  warning("Alguns tickers do CSV não foram encontrados no Yahoo Finance ou não possuem dados no período: ",
          paste(head(tickers_faltantes_download, 15), collapse=", "),
          if(length(tickers_faltantes_download) > 15) "...")
}

# --- 4. Preparação de Dados e Cálculo de Retornos ---

# Filtrar a lista de tickers por segmento para incluir apenas os baixados
tickers_por_segmento_validos <- lapply(tickers_por_segmento, function(tickers) {
  intersect(tickers, tickers_baixados)
})
# Remover segmentos que ficaram sem tickers válidos após o download
tickers_por_segmento_validos <- tickers_por_segmento_validos[sapply(tickers_por_segmento_validos, length) > 0]

# Verificações essenciais
if (length(tickers_baixados) == 0) {
  stop("Nenhum dado de preço foi baixado com sucesso nesta execução.")
}
if (is.null(df_precos) || is.null(df_precos$df.tickers) || nrow(df_precos$df.tickers) == 0) {
  stop("Estrutura de dados de preços vazia ou inválida após download.")
}
nome_seg_tradicional <- "Tradicional" # Nome padrão para o segmento benchmark
if (!nome_seg_tradicional %in% names(tickers_por_segmento_validos) || length(tickers_por_segmento_validos[[nome_seg_tradicional]]) == 0) {
  stop("Nenhum ticker válido encontrado para o segmento 'Tradicional' após o download. Impossível criar o benchmark.")
}

print("Formatando dados de preços e calculando retornos logarítmicos...")

# Pivotar para formato wide (datas x tickers)
precos_wide <- df_precos$df.tickers %>%
  select(ref.date, ticker, price.adjusted) %>%
  filter(!is.na(price.adjusted)) %>%
  distinct(ref.date, ticker, .keep_all = TRUE) %>% # Garantir unicidade antes do pivot
  pivot_wider(names_from = ticker, values_from = price.adjusted) %>%
  arrange(ref.date) %>%
  mutate(ref.date = as.Date(ref.date))

# Remover colunas que são totalmente NA (acontece se ticker não teve dados válidos)
precos_wide <- precos_wide[, colSums(is.na(precos_wide), na.rm = TRUE) < nrow(precos_wide)]

# Converter para xts
ticker_cols <- setdiff(colnames(precos_wide), "ref.date")
if(length(ticker_cols) == 0) {
  stop("Nenhuma coluna de ticker válida restou após remover NAs.")
}
if(nrow(precos_wide) < 2) {
  stop("Dados de preço insuficientes (menos de 2 dias) após limpeza para calcular retornos.")
}
precos_xts <- xts(precos_wide[,ticker_cols, drop=FALSE], order.by = precos_wide$ref.date)

# Calcular retornos logarítmicos
tryCatch({
  retornos_xts <- Return.calculate(precos_xts, method = "log")
  # Remover linhas que são totalmente NA (pode acontecer no início ou fim)
  if(ncol(retornos_xts)>0) retornos_xts <- retornos_xts[rowSums(is.na(retornos_xts)) < ncol(retornos_xts), ]
  # Remover a primeira linha se for toda NA (resultado comum do cálculo de retorno)
  if(nrow(retornos_xts) > 0 && ncol(retornos_xts) > 0) {
    first_row_all_na <- all(is.na(retornos_xts[1,]))
    if(is.na(first_row_all_na) || first_row_all_na) {
      if (nrow(retornos_xts) > 1) {
        retornos_xts <- retornos_xts[-1,]
      } else {
        retornos_xts <- retornos_xts[0,] # Zera se só tinha uma linha NA
      }
    }
  } else if (ncol(retornos_xts) == 0){
    retornos_xts <- xts() # Define como xts vazio se não houver colunas
    warning("Cálculo de retorno resultou em matriz sem colunas válidas.")
  }
}, error = function(e) {
  stop(paste("Erro ao calcular retornos:", e$message))
})

# Verificar novamente se retornos_xts é válido
if (is.null(retornos_xts) || nrow(retornos_xts) == 0 || ncol(retornos_xts) == 0) {
  stop("Não há dados de retorno válidos após o processamento.")
}
print(paste("Retornos calculados. Dimensões (dias x tickers):", nrow(retornos_xts), "x", ncol(retornos_xts)))

# --- 5. Criação do Benchmark (Segmento Tradicional Equi-ponderado) ---

print("Criando benchmark a partir do Segmento Tradicional (Equi-ponderado)...")

# Validar novamente se o segmento tradicional existe e tem tickers com retornos
if (!nome_seg_tradicional %in% names(tickers_por_segmento_validos)) {
  stop("Segmento Tradicional não encontrado na lista de segmentos válidos após download.")
}
tickers_tradicional_validos <- tickers_por_segmento_validos[[nome_seg_tradicional]]
# Intersectar com as colunas que *realmente* existem em retornos_xts
tickers_tradicional_validos <- intersect(tickers_tradicional_validos, colnames(retornos_xts))

if(length(tickers_tradicional_validos) == 0){
  stop("Nenhum ticker do segmento Tradicional possui dados de retorno válidos para criar o benchmark.")
}

# Calcular média equi-ponderada dos retornos dos tickers tradicionais válidos
retornos_tradicional <- retornos_xts[, tickers_tradicional_validos, drop = FALSE]

if (ncol(retornos_tradicional) > 1) {
  benchmark_returns_raw <- rowMeans(retornos_tradicional, na.rm = TRUE)
} else if (ncol(retornos_tradicional) == 1) {
  # Se só tiver um ticker, o benchmark é o próprio ticker
  benchmark_returns_raw <- coredata(retornos_tradicional)
} else {
  stop("Erro inesperado: Nenhuma coluna de retorno encontrada para o segmento tradicional após filtros.")
}

# Limpar NAs gerados pelo rowMeans (se todas as ações tiverem NA num dia)
benchmark_returns_raw[is.nan(benchmark_returns_raw)] <- NA
retornos_benchmark <- xts(benchmark_returns_raw, order.by = index(retornos_tradicional))
colnames(retornos_benchmark) <- "Tradicional_Benchmark"

# Remover NAs do benchmark final
retornos_benchmark <- na.omit(retornos_benchmark)

# Verificar se o benchmark tem dados suficientes
min_dias_benchmark <- 30 # Mínimo de dias para considerar o benchmark válido
if (nrow(retornos_benchmark) < min_dias_benchmark) {
  stop(paste("Benchmark do Segmento Tradicional possui menos de", min_dias_benchmark,
             "dias de dados válidos (", nrow(retornos_benchmark), " dias). Impossível continuar."))
}
print(paste("Benchmark do Segmento Tradicional criado com", length(tickers_tradicional_validos),
            "tickers e", nrow(retornos_benchmark), "dias válidos."))


# --- 6. Cálculo da Volatilidade (Desvio Padrão Anualizado) por Segmento ---

print("Calculando volatilidade média por segmento (Desvio Padrão Anualizado)...")
volatilidade_segmentos <- list()
num_acoes_vol <- list()
min_obs_vol <- 20 # Mínimo de observações para calcular a volatilidade de uma ação

for (segmento in names(tickers_por_segmento_validos)) {
  tickers_segmento_atual <- tickers_por_segmento_validos[[segmento]]
  num_acoes_vol[[segmento]] <- 0
  volatilidade_segmentos[[segmento]] <- NA # Inicializa como NA
  
  if(length(tickers_segmento_atual) == 0) next # Pula se segmento ficou vazio
  
  # Garantir que só usamos tickers com retornos calculados
  tickers_segmento_atual <- intersect(tickers_segmento_atual, colnames(retornos_xts))
  if(length(tickers_segmento_atual) == 0) next # Pula se nenhum ticker válido restou
  
  retornos_segmento <- retornos_xts[, tickers_segmento_atual, drop = FALSE]
  
  # Calcular volatilidade anualizada para cada ação do segmento
  vols_acoes <- apply(retornos_segmento, 2, function(ret_acao) {
    ret_acao_clean <- na.omit(ret_acao)
    if(length(ret_acao_clean) < min_obs_vol) return(NA) # Pula se ação tem poucos dados
    tryCatch({
      sd_ann <- StdDev.annualized(ret_acao_clean, scale = 252) * 100 # Em percentual
      if(is.finite(sd_ann)) return(sd_ann) else return(NA)
    }, error = function(e) { return(NA) }) # Retorna NA em caso de erro
  })
  
  # Calcular a volatilidade média do segmento e contar ações válidas
  volatilidade_segmentos[[segmento]] <- mean(vols_acoes, na.rm = TRUE)
  num_acoes_vol[[segmento]] <- sum(!is.na(vols_acoes))
}

# Calcular volatilidade do benchmark separadamente
vol_benchmark <- NA
if(nrow(retornos_benchmark) >= min_obs_vol) {
  tryCatch({
    vol_bm_calc <- StdDev.annualized(retornos_benchmark, scale = 252) * 100
    if(is.finite(vol_bm_calc)) vol_benchmark <- vol_bm_calc
  }, error = function(e) { warning("Erro ao calcular volatilidade do benchmark.")})
}

# Organizar resultados da volatilidade em um dataframe
df_volatilidade <- data.frame(
  Segmento = names(volatilidade_segmentos),
  Num_Tickers_Vol = unlist(num_acoes_vol),
  Volatilidade_Anualizada_Media_Pct = unlist(volatilidade_segmentos)
) %>%
  filter(Num_Tickers_Vol > 0, !is.na(Volatilidade_Anualizada_Media_Pct), is.finite(Volatilidade_Anualizada_Media_Pct)) %>%
  mutate(Segmento = ifelse(Segmento == nome_seg_tradicional, paste0(nome_seg_tradicional, " (Ações)"), Segmento)) # Diferenciar média das ações vs benchmark

# Adicionar o benchmark como uma linha separada
if(is.finite(vol_benchmark)){
  df_volatilidade <- df_volatilidade %>%
    bind_rows(
      data.frame(Segmento = "Tradicional (Benchmark EquiPonderado)",
                 Num_Tickers_Vol = length(tickers_tradicional_validos), # Num tickers no benchmark
                 Volatilidade_Anualizada_Media_Pct = vol_benchmark)
    )
} else {
  warning("Volatilidade do benchmark Tradicional não pôde ser calculada ou não é finita.")
}

# Ordenar e exibir
df_volatilidade <- df_volatilidade %>% arrange(desc(Volatilidade_Anualizada_Media_Pct))

print("--- Volatilidade Anualizada Média por Segmento (Desvio Padrão %) ---")
print(df_volatilidade)

# Salvar tabela de volatilidade
tryCatch({
  write.csv(df_volatilidade, file = here(dir_resultados, "tabela_volatilidade_segmentos.csv"), row.names = FALSE)
  print(paste("Tabela de volatilidade salva em:", here(dir_resultados, "tabela_volatilidade_segmentos.csv")))
}, error = function(e){
  warning(paste("Erro ao salvar tabela de volatilidade:", e$message))
})


# --- 7. Cálculo da Volatilidade Relativa (Beta) vs Benchmark Tradicional ---

print("Calculando volatilidade relativa média (Beta) por segmento vs Benchmark Tradicional...")
beta_segmentos <- list()
num_acoes_beta <- list()
min_obs_beta <- 30 # Mínimo de observações conjuntas (ação e benchmark) para calcular Beta

for (segmento in names(tickers_por_segmento_validos)) {
  tickers_segmento_atual <- tickers_por_segmento_validos[[segmento]]
  num_acoes_beta[[segmento]] <- 0
  beta_segmentos[[segmento]] <- NA # Inicializa como NA
  
  if(length(tickers_segmento_atual) == 0) next
  
  tickers_segmento_atual <- intersect(tickers_segmento_atual, colnames(retornos_xts))
  if(length(tickers_segmento_atual) == 0) next
  
  retornos_segmento <- retornos_xts[, tickers_segmento_atual, drop = FALSE]
  
  # Calcular Beta para cada ação do segmento em relação ao benchmark
  betas_acoes <- sapply(tickers_segmento_atual, function(ticker) {
    if(!ticker %in% colnames(retornos_segmento)) return(NA) # Checagem extra
    
    # Combinar retornos da ação e do benchmark, usando apenas datas onde ambos existem
    dados_combinados <- merge.xts(retornos_segmento[, ticker], retornos_benchmark, join = "inner")
    if (nrow(dados_combinados) < min_obs_beta) return(NA) # Pula se poucos dados conjuntos
    
    colnames(dados_combinados) <- c("Acao", "Benchmark")
    
    # Calcular Beta usando a fórmula Cov(Ra, Rb) / Var(Rb)
    var_bench <- var(coredata(dados_combinados$Benchmark), na.rm=TRUE)
    if (is.na(var_bench) || var_bench == 0) return(NA) # Evitar divisão por zero/NA
    
    tryCatch({
      cov_ab <- cov(coredata(dados_combinados$Acao), coredata(dados_combinados$Benchmark), use = "pairwise.complete.obs")
      beta_calc <- cov_ab / var_bench
      if(is.finite(beta_calc)) return(beta_calc) else return(NA) # Retorna NA se Beta não for finito
    }, error = function(e) { return(NA) } ) # Retorna NA em caso de erro
  })
  
  # Calcular o Beta médio do segmento e contar ações válidas
  beta_segmentos[[segmento]] <- mean(betas_acoes, na.rm = TRUE)
  num_acoes_beta[[segmento]] <- sum(!is.na(betas_acoes))
}

# Organizar resultados do Beta em um dataframe
df_beta <- data.frame(
  Segmento = names(beta_segmentos),
  Num_Tickers_Beta = unlist(num_acoes_beta),
  Beta_Medio = unlist(beta_segmentos)
) %>%
  filter(Num_Tickers_Beta > 0, !is.na(Beta_Medio), is.finite(Beta_Medio)) # Filtrar inválidos

# Renomear segmento Tradicional para clareza e ordenar
df_beta <- df_beta %>%
  mutate(Segmento = ifelse(Segmento == nome_seg_tradicional, "Tradicional (Benchmark)", Segmento)) %>%
  arrange(desc(Beta_Medio))

print("--- Volatilidade Relativa Média (Beta) vs Segmento Tradicional ---")
print(df_beta)

# Salvar tabela de Beta
tryCatch({
  write.csv(df_beta, file = here(dir_resultados, "tabela_beta_segmentos.csv"), row.names = FALSE)
  print(paste("Tabela de Beta salva em:", here(dir_resultados, "tabela_beta_segmentos.csv")))
}, error = function(e){
  warning(paste("Erro ao salvar tabela de Beta:", e$message))
})


# --- 8. Visualização dos Resultados (Beta) ---

if(nrow(df_beta) > 0) {
  print("Gerando gráfico de comparação (Beta)...")
  
  # Preparar dados para o gráfico (labels mais informativos)
  df_beta_plot <- df_beta %>%
    mutate(
      Segmento_Label = ifelse(
        Segmento == "Tradicional (Benchmark)",
        Segmento, # Não adicionar contagem ao benchmark
        paste0(Segmento, " (", Num_Tickers_Beta, " tickers)")
      ),
      # Ordenar os fatores pela média do Beta para o gráfico
      Segmento_Label = factor(Segmento_Label, levels = Segmento_Label[order(Beta_Medio)])
    )
  
  # Definir limites do eixo Y dinamicamente, garantindo que 0 e 1 estejam visíveis
  ymin_plot <- min(0, min(df_beta_plot$Beta_Medio, na.rm = TRUE) - 0.1, na.rm = TRUE)
  ymax_plot <- max(1.1, max(df_beta_plot$Beta_Medio, na.rm = TRUE) * 1.2, na.rm = TRUE) # Garante espaço acima de 1
  ymin_plot <- if(is.finite(ymin_plot)) ymin_plot else 0
  ymax_plot <- if(is.finite(ymax_plot)) ymax_plot else 1.5
  
  # Criar o gráfico
  grafico_beta <- ggplot(df_beta_plot, aes(x = Segmento_Label, y = Beta_Medio, fill = Segmento)) +
    geom_col(show.legend = FALSE, alpha = 0.8) + # Colunas com leve transparência
    geom_hline(yintercept = 1.0, linetype = "dashed", color = "red", linewidth = 0.8) + # Linha de referência Beta = 1
    geom_text(aes(label = sprintf("%.2f", Beta_Medio)), hjust = -0.2, size = 3.5, color = "black") + # Rótulos dos valores
    coord_flip(ylim = c(ymin_plot, ymax_plot), clip = "off") + # Virar eixos, permitir texto fora
    scale_fill_viridis_d(option = "D") + # Usar paleta de cores Viridis (boa para daltônicos)
    labs(
      title = "Beta Médio por Segmento de Listagem vs Benchmark Tradicional",
      subtitle = paste("Período:", format(data_inicio, "%Y-%m-%d"), "a", format(data_fim_analise, "%Y-%m-%d"),
                       "\nBenchmark: Média Equi-ponderada dos tickers válidos do Segmento Tradicional.",
                       "\nFonte de Tickers/Segmentos: Arquivo CSV local."),
      x = "Segmento de Listagem (Nº Tickers Válidos)",
      y = "Beta Médio (Volatilidade Relativa ao Benchmark Tradicional)"
    ) +
    theme_minimal(base_size = 12) + # Tema limpo com tamanho base da fonte
    theme(
      plot.title = element_text(face = "bold", size = 16),
      plot.subtitle = element_text(size = 10, color = "gray30"),
      axis.title.x = element_text(margin = margin(t = 10)), # Espaço título eixo X
      axis.title.y = element_text(margin = margin(r = 10)), # Espaço título eixo Y
      panel.grid.major.y = element_blank(), # Remover linhas de grade verticais (após coord_flip)
      panel.grid.minor = element_blank()
    )
  
  print(grafico_beta)
  
  # Salvar o gráfico
  tryCatch({
    ggsave(
      filename = here(dir_resultados, "grafico_beta_segmentos.png"),
      plot = grafico_beta,
      width = 10, # Ajuste a largura conforme necessário
      height = 8, # Ajuste a altura conforme necessário
      dpi = 300 # Boa resolução para publicação
    )
    print(paste("Gráfico de Beta salvo em:", here(dir_resultados, "grafico_beta_segmentos.png")))
  }, error = function(e){
    warning(paste("Erro ao salvar gráfico de Beta:", e$message))
  })
  
} else {
  print("Não há dados de segmentos com Beta válido para gerar o gráfico de comparação.")
}

# --- 9. Conclusão do Script ---

print("----------------------------------------------------")
print("Análise concluída com sucesso.")
print(paste("Resultados salvos na pasta:", dir_resultados))
print("----------------------------------------------------")

# Limpeza opcional de variáveis grandes (se rodar interativamente)
# rm(df_precos, precos_wide, precos_xts, retornos_xts)
# gc() # Garbage collection