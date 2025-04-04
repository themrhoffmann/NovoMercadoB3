# --- 0. Instalar e Carregar Pacotes ---
# install.packages(c("BatchGetSymbols", "dplyr", "lubridate", "PerformanceAnalytics", "tidyr", "ggplot2", "xts", "readr", "stringr"))
library(BatchGetSymbols)
library(dplyr)
library(lubridate)
library(PerformanceAnalytics)
library(tidyr)
library(ggplot2)
library(xts)
library(readr) # Para ler o CSV
library(stringr) # Para manipulação de texto

# --- 1. Parâmetros ---
data_inicio <- as.Date("2000-01-01") # Mantido início em 2000
data_fim <- today() # Usa a data atual da execução
# Benchmark = Segmento Tradicional (definido abaixo)

# --- 1b. Ler Tickers e Segmentos do Arquivo CSV Fornecido ---
# ATENÇÃO: O caminho do arquivo DEVE estar correto para o script funcionar!
# Use barras normais '/' ou barras invertidas duplas '\\' no caminho.
caminho_csv <- "C:/Users/marcos.hoffmann/OneDrive - Elastri Engenharia SA/Área de Trabalho/Congresso/Tickers.csv"

print(paste("Tentando ler arquivo CSV de:", caminho_csv))

if (!file.exists(caminho_csv)) {
  stop(paste("ERRO: Arquivo CSV não encontrado no caminho especificado:", caminho_csv))
}

tryCatch({
  # AJUSTE: Ler como CSV usando ponto e vírgula como separador (delim = ";")
  # Assumindo que a codificação é UTF-8 ou Latin1 (comum no Brasil)
  # Tentar Latin1 primeiro, se falhar, tentar UTF-8
  df_tickers_segmentos_csv <- NULL
  tryCatch({
    df_tickers_segmentos_csv <- read_delim(caminho_csv, delim = ";", locale = locale(encoding = "Latin1"), show_col_types = FALSE)
    print("CSV lido com sucesso usando Latin1 encoding.")
  }, error = function(e_latin1){
    warning("Falha ao ler CSV com Latin1, tentando UTF-8...")
    tryCatch({
      df_tickers_segmentos_csv <<- read_delim(caminho_csv, delim = ";", locale = locale(encoding = "UTF-8"), show_col_types = FALSE)
      print("CSV lido com sucesso usando UTF-8 encoding.")
    }, error = function(e_utf8){
      stop(paste("Erro ao ler CSV com Latin1 e UTF-8. Verifique o arquivo e a codificação. Erro Original:", e_utf8$message))
    })
  })
  
  
  # Verificar se as colunas esperadas existem (case-insensitive)
  expected_cols <- c("Tickers", "Segmentos")
  actual_cols <- colnames(df_tickers_segmentos_csv)
  if (!all(tolower(expected_cols) %in% tolower(actual_cols))) {
    stop(paste("ERRO: Colunas 'Tickers' e/ou 'Segmentos' não encontradas no CSV. Colunas encontradas:", paste(actual_cols, collapse=", ")))
  }
  # Renomear para nomes padrão (case-sensitive) para facilitar
  colnames(df_tickers_segmentos_csv)[tolower(colnames(df_tickers_segmentos_csv)) == "tickers"] <- "Ticker"
  colnames(df_tickers_segmentos_csv)[tolower(colnames(df_tickers_segmentos_csv)) == "segmentos"] <- "Segmento_Original"
  
  
}, error = function(e) {
  stop(paste("Erro ao ler ou processar o arquivo CSV:", e$message))
})

print(paste("Arquivo CSV lido com sucesso. Total de linhas:", nrow(df_tickers_segmentos_csv)))

# --- 1c. Processar Dados do CSV ---

# Limpar nomes dos segmentos e formatar tickers
df_processado <- df_tickers_segmentos_csv %>%
  select(Ticker, Segmento_Original) %>%
  filter(!is.na(Ticker), !is.na(Segmento_Original), Ticker != "", Segmento_Original != "") %>% # Remover NAs e vazios
  mutate(
    Ticker = str_trim(Ticker),
    Segmento_Original = str_trim(Segmento_Original),
    Segmento_Limpo = case_when(
      str_detect(Segmento_Original, regex("NOVO MERCADO", ignore_case = TRUE)) ~ "NovoMercado",
      str_detect(Segmento_Original, regex("NIVEL 2", ignore_case = TRUE)) ~ "Nivel2",
      str_detect(Segmento_Original, regex("NIVEL 1", ignore_case = TRUE)) ~ "Nivel1",
      str_detect(Segmento_Original, regex("M2", ignore_case = TRUE)) ~ "BovespaMaisN2",
      str_detect(Segmento_Original, regex("BOVESPA MAIS", ignore_case = TRUE)) ~ "BovespaMais",
      str_detect(Segmento_Original, regex("BOLSA", ignore_case = TRUE)) ~ "Tradicional",
      str_detect(Segmento_Original, regex("MERCADO DE BALCÃO", ignore_case = TRUE)) ~ "SOMA_OTC",
      # Adicione mapeamentos para outros possíveis nomes de segmento no seu CSV
      TRUE ~ Segmento_Original # Manter original se não mapeado, para inspeção posterior
    ),
    # Adicionar sufixo .SA ao ticker
    Ticker_SA = ifelse(!str_ends(Ticker, fixed(".SA", ignore_case = TRUE)),
                       paste0(toupper(Ticker), ".SA"),
                       toupper(Ticker))
  ) %>%
  # Filtrar segmentos não desejados explicitamente (se necessário)
  # filter(!Segmento_Limpo %in% c("Excluir", "FIP", "OUTROS")) %>%
  select(Ticker_SA, Segmento_Limpo) %>%
  distinct()

# Verificar se algum segmento não foi mapeado corretamente (opcional)
segmentos_nao_mapeados <- df_processado %>%
  filter(!Segmento_Limpo %in% c("NovoMercado", "Nivel2", "Nivel1", "BovespaMaisN2", "BovespaMais", "Tradicional", "SOMA_OTC")) %>%
  distinct(Segmento_Limpo)
if(nrow(segmentos_nao_mapeados) > 0){
  warning("Os seguintes nomes de segmento do CSV não foram mapeados para categorias padrão: ",
          paste(segmentos_nao_mapeados$Segmento_Limpo, collapse=", "),
          ". Verifique a seção 'case_when' no código.")
  # Decidir se filtra ou mantém: por ora, vamos manter e ver se causa erro depois
  # df_processado <- df_processado %>%
  #    filter(Segmento_Limpo %in% c("NovoMercado", "Nivel2", "Nivel1", "BovespaMaisN2", "BovespaMais", "Tradicional", "SOMA_OTC"))
}


print(paste("Tickers processados e válidos após limpeza:", nrow(df_processado)))
print("Distribuição por segmento processado:")
print(table(df_processado$Segmento_Limpo))

# Criar a lista tickers_por_segmento
tickers_por_segmento <- df_processado %>%
  group_by(Segmento_Limpo) %>%
  summarise(Tickers = list(Ticker_SA), .groups = 'drop') %>%
  tibble::deframe()

# Remover segmentos que podem ter ficado vazios
tickers_por_segmento <- tickers_por_segmento[sapply(tickers_por_segmento, length) > 0]

# Criar lista única de todos os tickers para download
todos_tickers_unicos <- unique(unlist(tickers_por_segmento))
if(length(todos_tickers_unicos) == 0) {
  stop("A lista final de tickers está vazia após processar o CSV.")
}
todos_tickers <- todos_tickers_unicos

# --- 2. Obter Dados Históricos ---
print(paste("Iniciando download para", length(todos_tickers), "tickers (baseado no CSV)..."))
print("Período: 2000-01-01 até hoje.")

options(BatchGetSymbols.max.attempts = 5, BatchGetSymbols.download.timeout = 300)

df_precos <- BatchGetSymbols(
  tickers = todos_tickers,
  first.date = data_inicio,
  last.date = data_fim,
  freq.data = "daily",
  cache.folder = file.path(tempdir(), "B3_data_cache_FROM_CSV_2000"), # Novo cache
  be.quiet = FALSE
)

# Verificar quais tickers foram baixados com sucesso
tickers_baixados <- character(0)
if (!is.null(df_precos) && !is.null(df_precos$df.tickers) && nrow(df_precos$df.tickers) > 0) {
  tickers_baixados <- unique(df_precos$df.tickers$ticker)
} else {
  warning("Nenhum ticker foi baixado com sucesso ou a estrutura de retorno está inesperada.")
}
tickers_faltantes_agora <- setdiff(todos_tickers, tickers_baixados)

if(length(tickers_faltantes_agora) > 0) {
  warning("Alguns tickers do CSV não foram encontrados ou não possuem dados no período: ",
          paste(head(tickers_faltantes_agora,15), collapse=", "),
          if(length(tickers_faltantes_agora)>15) "...")
}

# --- 3. Preparar Dados e Calcular Retornos ---
# (Restante do código idêntico às versões anteriores)
# Continuar apenas com tickers que foram realmente baixados NESTA execução
tickers_por_segmento_validos <- lapply(tickers_por_segmento, function(tickers) {
  intersect(tickers, tickers_baixados)
})
tickers_por_segmento_validos <- tickers_por_segmento_validos[sapply(tickers_por_segmento_validos, length) > 0]

if (length(tickers_baixados) == 0) {
  stop("Nenhum dado de preço foi baixado com sucesso nesta execução.")
}
if (is.null(df_precos) || is.null(df_precos$df.tickers) || nrow(df_precos$df.tickers) == 0) {
  stop("Nenhum dado de preço foi baixado com sucesso (df.tickers vazio).")
}
nome_seg_tradicional <- "Tradicional"
if (!nome_seg_tradicional %in% names(tickers_por_segmento_validos) || length(tickers_por_segmento_validos[[nome_seg_tradicional]]) == 0) {
  stop("Nenhum ticker válido encontrado para o segmento 'Tradicional' após o download. Impossível criar o benchmark.")
}

print("Calculando retornos...")
# Adicionado distinct para garantir que não haja linhas duplicadas de ticker/data antes do pivot
precos_wide <- df_precos$df.tickers %>%
  select(ref.date, ticker, price.adjusted) %>%
  filter(!is.na(price.adjusted)) %>%
  distinct(ref.date, ticker, .keep_all = TRUE) %>% # Garantir unicidade
  pivot_wider(names_from = ticker, values_from = price.adjusted) %>%
  arrange(ref.date)

precos_wide <- precos_wide %>% mutate(ref.date = as.Date(ref.date))
precos_wide <- precos_wide[, colSums(is.na(precos_wide), na.rm = TRUE) < nrow(precos_wide)]
ticker_cols <- setdiff(colnames(precos_wide), "ref.date")
if(length(ticker_cols) == 0) {
  stop("Nenhuma coluna de ticker válida restou após remover NAs.")
}
if(nrow(precos_wide) < 2) {
  stop("Dados de preço insuficientes (menos de 2 dias) após limpeza.")
}
precos_xts <- xts(precos_wide[,ticker_cols, drop=FALSE], order.by = precos_wide$ref.date)


if(nrow(precos_xts) < 2) {
  stop("Dados de preços insuficientes para calcular retornos.")
}
tryCatch({
  retornos_xts <- Return.calculate(precos_xts, method = "log")
  if(ncol(retornos_xts)>0) retornos_xts <- retornos_xts[rowSums(is.na(retornos_xts)) < ncol(retornos_xts), ]
  if(nrow(retornos_xts) > 0 && ncol(retornos_xts) > 0) {
    first_row_all_na <- all(is.na(retornos_xts[1,]))
    if(is.na(first_row_all_na) || first_row_all_na) {
      if (nrow(retornos_xts) > 1) {
        retornos_xts <- retornos_xts[-1,]
      } else {
        retornos_xts <- retornos_xts[0,]
      }
    }
  } else if (ncol(retornos_xts) == 0){
    # Se não houver colunas após o cálculo do retorno, definir como xts vazio
    retornos_xts <- xts()
    warning("Cálculo de retorno resultou em matriz sem colunas.")
  }
}, error = function(e) {
  stop(paste("Erro ao calcular retornos:", e$message))
})

# Verificar novamente se retornos_xts é válido
if (is.null(retornos_xts) || nrow(retornos_xts) == 0 || ncol(retornos_xts) == 0) {
  stop("Não há dados de retorno válidos após o processamento.")
}

# --- 3b. Definir o Benchmark como o Segmento Tradicional (Equi-ponderado) ---
print("Criando benchmark a partir do Segmento Tradicional (Equi-ponderado)...")
if (!nome_seg_tradicional %in% names(tickers_por_segmento_validos)) {
  stop("Segmento Tradicional não encontrado na lista de segmentos válidos após download.")
}
tickers_tradicional_validos <- tickers_por_segmento_validos[[nome_seg_tradicional]]
tickers_tradicional_validos <- intersect(tickers_tradicional_validos, colnames(retornos_xts))

if(length(tickers_tradicional_validos) == 0){
  stop("Nenhum ticker do segmento Tradicional possui dados de retorno válidos para criar o benchmark.")
}

retornos_tradicional <- retornos_xts[, tickers_tradicional_validos, drop = FALSE]

if (ncol(retornos_tradicional) > 1) {
  benchmark_returns_raw <- rowMeans(retornos_tradicional, na.rm = TRUE)
} else if (ncol(retornos_tradicional) == 1) {
  benchmark_returns_raw <- coredata(retornos_tradicional)
} else {
  stop("Erro inesperado: Nenhuma coluna de retorno para o segmento tradicional.")
}

benchmark_returns_raw[is.nan(benchmark_returns_raw)] <- NA
retornos_benchmark <- xts(benchmark_returns_raw, order.by = index(retornos_tradicional))
colnames(retornos_benchmark) <- "Tradicional_Benchmark"
retornos_benchmark <- na.omit(retornos_benchmark)

if (nrow(retornos_benchmark) < 30) {
  stop(paste("Benchmark do Segmento Tradicional possui menos de 30 dias de dados válidos (", nrow(retornos_benchmark), " dias)."))
}
print(paste("Benchmark do Segmento Tradicional criado com", length(tickers_tradicional_validos), "tickers e", nrow(retornos_benchmark), "dias válidos."))


# --- 4. Calcular Volatilidade (Desvio Padrão Anualizado) ---
print("Calculando volatilidade (Desvio Padrão Anualizado)...")
vol_benchmark <- StdDev.annualized(retornos_benchmark, scale = 252) * 100
volatilidade_segmentos <- list()
num_acoes_vol <- list()

for (segmento in names(tickers_por_segmento_validos)) {
  tickers_segmento_atual <- tickers_por_segmento_validos[[segmento]]
  num_acoes_vol[[segmento]] <- 0
  volatilidade_segmentos[[segmento]] <- NA
  if(length(tickers_segmento_atual) == 0) next
  
  tickers_segmento_atual <- intersect(tickers_segmento_atual, colnames(retornos_xts))
  if(length(tickers_segmento_atual) == 0) next
  
  retornos_segmento <- retornos_xts[, tickers_segmento_atual, drop = FALSE]
  vols_acoes <- apply(retornos_segmento, 2, function(ret_acao) {
    ret_acao_clean <- na.omit(ret_acao)
    if(length(ret_acao_clean) < 20) return(NA)
    tryCatch({
      sd_ann <- StdDev.annualized(ret_acao_clean, scale = 252) * 100
      if(is.finite(sd_ann)) return(sd_ann) else return(NA)
    }, error = function(e) { return(NA) })
  })
  volatilidade_segmentos[[segmento]] <- mean(vols_acoes, na.rm = TRUE)
  num_acoes_vol[[segmento]] <- sum(!is.na(vols_acoes))
}

df_volatilidade <- data.frame(
  Segmento = names(volatilidade_segmentos),
  Num_Tickers_Considerados = unlist(num_acoes_vol),
  Volatilidade_Anualizada_Media_Percent = unlist(volatilidade_segmentos)
) %>%
  filter(Num_Tickers_Considerados > 0, !is.na(Volatilidade_Anualizada_Media_Percent))

if(is.finite(vol_benchmark)){
  df_volatilidade <- df_volatilidade %>%
    mutate(Segmento = ifelse(Segmento == "Tradicional", "Tradicional (Benchmark)", Segmento)) %>%
    bind_rows(
      data.frame(Segmento = "Tradicional (Benchmark)",
                 Num_Tickers_Considerados = length(tickers_tradicional_validos),
                 Volatilidade_Anualizada_Media_Percent = vol_benchmark)
    ) %>%
    group_by(Segmento) %>%
    slice(1) %>%
    ungroup() %>%
    arrange(desc(Volatilidade_Anualizada_Media_Percent))
  
} else {
  warning("Volatilidade do benchmark Tradicional não pôde ser calculada.")
  df_volatilidade <- df_volatilidade %>% arrange(desc(Volatilidade_Anualizada_Media_Percent))
}

print("--- Volatilidade Anualizada Média por Segmento (Desvio Padrão %) ---")
print(df_volatilidade)


# --- 5. Calcular Volatilidade Relativa (Beta) vs BENCHMARK TRADICIONAL ---
print("Calculando volatilidade relativa (Beta) vs Benchmark Tradicional...")
beta_segmentos <- list()
num_acoes_beta <- list()

for (segmento in names(tickers_por_segmento_validos)) {
  tickers_segmento_atual <- tickers_por_segmento_validos[[segmento]]
  num_acoes_beta[[segmento]] <- 0
  beta_segmentos[[segmento]] <- NA
  if(length(tickers_segmento_atual) == 0) next
  
  tickers_segmento_atual <- intersect(tickers_segmento_atual, colnames(retornos_xts))
  if(length(tickers_segmento_atual) == 0) next
  
  retornos_segmento <- retornos_xts[, tickers_segmento_atual, drop = FALSE]
  betas_acoes <- sapply(tickers_segmento_atual, function(ticker) {
    if(!ticker %in% colnames(retornos_segmento)) return(NA)
    
    dados_combinados <- merge.xts(retornos_segmento[, ticker], retornos_benchmark, join = "inner")
    if (nrow(dados_combinados) < 30) return(NA)
    colnames(dados_combinados) <- c("Acao", "Benchmark")
    var_bench <- var(coredata(dados_combinados$Benchmark), na.rm=TRUE)
    if (is.na(var_bench) || var_bench == 0) return(NA)
    tryCatch({
      cov_ab <- cov(coredata(dados_combinados$Acao), coredata(dados_combinados$Benchmark), use = "pairwise.complete.obs")
      beta_calc <- cov_ab / var_bench
      if(is.finite(beta_calc)) return(beta_calc) else return(NA)
    }, error = function(e) { return(NA) } )
  })
  beta_segmentos[[segmento]] <- mean(betas_acoes, na.rm = TRUE)
  num_acoes_beta[[segmento]] <- sum(!is.na(betas_acoes))
}

df_beta <- data.frame(
  Segmento = names(beta_segmentos),
  Num_Tickers_Considerados = unlist(num_acoes_beta),
  Beta_Medio = unlist(beta_segmentos)
) %>%
  filter(Num_Tickers_Considerados > 0, !is.na(Beta_Medio))

df_beta <- df_beta %>%
  mutate(Segmento = ifelse(Segmento == "Tradicional", "Tradicional (Benchmark)", Segmento)) %>%
  arrange(desc(Beta_Medio))

print("--- Volatilidade Relativa Média (Beta) vs Segmento Tradicional ---")
print(df_beta)


# --- 6. Visualização (Exemplo com Beta) ---
if(nrow(df_beta) > 0) {
  print("Gerando gráfico de comparação (Beta)...")
  df_beta_plot <- df_beta %>%
    mutate(Segmento_Label = paste0(Segmento, " (", Num_Tickers_Considerados, " tickers)")) %>%
    mutate(Segmento_Label = ifelse(str_detect(Segmento, fixed("(Benchmark)")), Segmento, Segmento_Label)) %>%
    mutate(Segmento_Label = factor(Segmento_Label, levels = Segmento_Label[order(Beta_Medio)]))
  
  if(any(!is.finite(df_beta_plot$Beta_Medio))){
    warning("Valores não finitos ou NAs encontrados no Beta_Medio, podem causar problemas no gráfico.")
    print(df_beta_plot[!is.finite(df_beta_plot$Beta_Medio), ])
    df_beta_plot <- df_beta_plot %>% filter(is.finite(Beta_Medio))
  }
  
  if(nrow(df_beta_plot) > 0) {
    ymin <- min(0, min(df_beta_plot$Beta_Medio, na.rm = TRUE) - 0.1, na.rm = TRUE)
    ymax <- max(df_beta_plot$Beta_Medio, na.rm = TRUE) * 1.20
    ymin <- if(is.finite(ymin)) ymin else 0
    ymax <- if(is.finite(ymax)) ymax else 1.5
    
    grafico_beta <- ggplot(df_beta_plot, aes(x = Segmento_Label, y = Beta_Medio, fill = Segmento)) +
      geom_col(show.legend = FALSE) +
      geom_hline(yintercept = 1.0, linetype = "dashed", color = "red") +
      geom_text(aes(label = sprintf("%.2f", Beta_Medio)), hjust = -0.2, size = 3) +
      coord_flip(ylim = c(ymin, ymax)) +
      labs(
        title = "Beta Médio por Segmento vs Segmento Tradicional",
        subtitle = paste("Período:", format(data_inicio, "%Y-%m-%d"), "a", format(data_fim, "%Y-%m-%d"),
                         "\nFonte de Tickers e Segmentos: Arquivo CSV fornecido.", # Subtítulo atualizado
                         "\nBenchmark: Média Equi-ponderada dos tickers válidos do Segmento Tradicional."),
        x = "Segmento de Listagem",
        y = "Beta Médio (Volatilidade Relativa ao Segmento Tradicional)"
      ) +
      theme_minimal() +
      theme(plot.subtitle = element_text(size = 9))
    
    print(grafico_beta)
    # ggsave("comparacao_beta_segmentos_vs_tradicional_CSV_desde2000.png", plot = grafico_beta, width = 12, height = 8)
  } else {
    print("Não há dados finitos suficientes de Beta para gerar o gráfico de comparação.")
  }
  
} else {
  print("Não há dados de segmentos com Beta válido para gerar o gráfico de comparação.")
}

print("Análise concluída.")