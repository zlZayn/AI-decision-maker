# ============================================================
# 分类变量统计分析 — R 端 (tidyverse)
# 由 run_categorical.py 通过 subprocess 调用
# 输入: data/categorical/input/*.csv + output/*_type.json
# 输出: console summary + report.json + report.xlsx (4 sheets)
#
# 决策逻辑 — 根据变量类型组合选择最佳方法:
#   有序 vs 有序 → Spearman rho（利用排序信息）
#   无序 vs 无序 → Cramer's V（无顺序可利用，衡量关联强度）
#   有序 vs 无序 → Kruskal-Wallis（按组检验分布差异）
#   卡方检验     → 通用兜底，所有组合均适用
# ============================================================

suppressPackageStartupMessages({
  library(tidyverse)
  library(jsonlite)
  library(openxlsx)
})

BAR  <- strrep("=", 62)
DASH <- strrep("-", 62)

# ---- 读取命令行参数 ----
args <- commandArgs(trailingOnly = TRUE)
input_dir  <- if (length(args) >= 1) args[1] else "data/categorical/input"
output_dir <- if (length(args) >= 2) args[2] else "data/categorical/output"

# ---- 读取所有 CSV ----
csv_files <- list.files(input_dir, pattern = "\\.csv$", full.names = TRUE)
if (length(csv_files) == 0) {
  cat("  [ERR] No CSV files found in", input_dir, "\n")
  quit(status = 1)
}

cat(sprintf("  input : %s (%d files)\n", input_dir, length(csv_files)))
cat(sprintf("  output: %s\n", output_dir))

# ---- 辅助：转换为 factor ----
to_factor <- function(df, col, type, type_info) {
  if (type == "有序" && col %in% names(type_info$ordinal)) {
    factor(df[[col]], levels = type_info$ordinal[[col]], ordered = TRUE)
  } else {
    as.factor(df[[col]])
  }
}

# ---- 分析单个文件 ----
analyze_file <- function(filepath) {
  filename <- basename(filepath)
  df <- read_csv(filepath, show_col_types = FALSE)

  # 读取 AI 分类结果
  type_file <- file.path(output_dir, gsub("\\.csv$", "_type.json", filename))
  if (!file.exists(type_file)) {
    cat(sprintf("  [SKIP] %s — no type file\n", filename))
    return(list(chi = tibble(), primary = tibble(), decision = ""))
  }
  type_info <- fromJSON(type_file)
  ordinal_names <- names(type_info$ordinal) %||% character(0)
  nominal_names <- as.character(type_info$nominal %||% character(0))

  # 取非 id 的前两个分类变量
  numeric_cols_all <- names(df)[sapply(df, is.numeric)]
  all_cats <- c(ordinal_names, nominal_names)
  all_cats <- all_cats[!(all_cats %in% c("id", numeric_cols_all))]
  if (length(all_cats) < 2) {
    cat(sprintf("  [SKIP] %s — fewer than 2 categorical vars\n", filename))
    return(list(chi = tibble(), primary = tibble(), decision = ""))
  }
  cat1 <- all_cats[1]
  cat2 <- all_cats[2]

  type1 <- if (cat1 %in% ordinal_names) "有序" else "无序"
  type2 <- if (cat2 %in% ordinal_names) "有序" else "无序"

  df[[cat1]] <- to_factor(df, cat1, type1, type_info)
  df[[cat2]] <- to_factor(df, cat2, type2, type_info)

  # ---- 通用：卡方检验 ----
  chi_results <- tibble()
  tbl <- table(df[[cat1]], df[[cat2]])
  if (all(tbl > 0)) {
    chi <- chisq.test(tbl)
    k <- min(nrow(tbl), ncol(tbl))
    v <- sqrt(chi$statistic / (nrow(df) * (k - 1)))
    chi_results <- tibble(
      file = filename,
      var1 = cat1, var1_type = type1,
      var2 = cat2, var2_type = type2,
      method = "卡方检验",
      stat = round(chi$statistic, 3), df = chi$parameter,
      p = chi$p.value, effect = round(v, 3), effect_name = "V"
    )
  }

  # ---- 决策：选择最佳专用方法 ----
  primary_results <- tibble()
  decision <- ""

  if (type1 == "无序" && type2 == "无序") {
    # 无序 vs 无序 → Cramer's V
    if (nrow(chi_results) > 0) {
      decision <- "无序vs无序，无顺序可利用 -> Cramer's V（关联强度）"
      primary_results <- tibble(
        file = filename,
        var1 = cat1, var1_type = type1,
        var2 = cat2, var2_type = type2,
        method = "Cramer V",
        stat = chi_results$effect[1], df = NA,
        p = NA, effect = NA, effect_name = NA
      )
    }

  } else if (type1 == "有序" && type2 == "有序") {
    # 有序 vs 有序 → Spearman
    decision <- "有序vs有序，利用排序信息 -> Spearman rho（秩相关）"
    sp <- cor.test(as.numeric(df[[cat1]]), as.numeric(df[[cat2]]), method = "spearman")
    primary_results <- tibble(
      file = filename,
      var1 = cat1, var1_type = type1,
      var2 = cat2, var2_type = type2,
      method = "Spearman",
      stat = round(sp$estimate, 3), df = NA,
      p = sp$p.value, effect = NA, effect_name = NA
    )

  } else {
    # 有序 vs 无序 → Kruskal-Wallis
    if (type1 == "有序") {
      ord_col <- cat1; nom_col <- cat2
      ord_type <- type1; nom_type <- type2
    } else {
      ord_col <- cat2; nom_col <- cat1
      ord_type <- type2; nom_type <- type1
    }
    decision <- sprintf("有序vs无序，按组检验分布差异 -> Kruskal-Wallis（%s by %s）", ord_col, nom_col)
    groups <- split(df[[ord_col]], df[[nom_col]])
    if (length(groups) >= 2 && all(sapply(groups, length) > 0)) {
      kw <- kruskal.test(groups)
      primary_results <- tibble(
        file = filename,
        var1 = nom_col, var1_type = nom_type,
        var2 = ord_col, var2_type = ord_type,
        method = "Kruskal",
        stat = round(kw$statistic, 3), df = kw$parameter,
        p = kw$p.value, effect = NA, effect_name = NA
      )
    }
  }

  list(chi = chi_results, primary = primary_results, decision = decision)
}

# ---- 运行所有文件 ----
decisions <- list()
chi_all <- tibble()
primary_all <- tibble()

for (fp in csv_files) {
  res <- analyze_file(fp)
  filename <- basename(fp)
  decisions[[filename]] <- res$decision
  chi_all <- bind_rows(chi_all, res$chi)
  primary_all <- bind_rows(primary_all, res$primary)
}

if (nrow(chi_all) == 0 && nrow(primary_all) == 0) {
  cat("  [WARN] No results\n")
  quit(status = 0)
}

# ---- 添加显著性标记 ----
add_sig <- function(df) {
  if (nrow(df) == 0 || !"p" %in% names(df)) return(df)
  df %>% mutate(sig = case_when(
    !is.na(p) & p < 0.001 ~ "***",
    !is.na(p) & p < 0.01  ~ "**",
    !is.na(p) & p < 0.05  ~ "*",
    TRUE                  ~ ""
  ))
}
chi_all <- add_sig(chi_all)
primary_all <- add_sig(primary_all)

# ---- 控制台输出：决策表 ----
cat(sprintf("\n%s\n", DASH))
cat("  Method Selection (decision logic)")
cat(sprintf("\n%s\n", DASH))
cat(sprintf("  %-14s %-10s -> %s\n", "file", "types", "method"))
cat(sprintf("  %-14s %-10s    %s\n", strrep("-", 14), strrep("-", 10), strrep("-", 36)))
for (f in names(decisions)) {
  if (is.null(decisions[[f]]) || decisions[[f]] == "") next
  sub_primary <- primary_all %>% filter(file == f)
  if (nrow(sub_primary) > 0) {
    types_str <- sprintf("%s/%s", sub_primary$var1_type[1], sub_primary$var2_type[1])
    method_str <- sub_primary$method[1]
  } else {
    types_str <- "?"
    method_str <- "N/A"
  }
  cat(sprintf("  %-14s %-10s -> %s\n", f, types_str, method_str))
  cat(sprintf("    %s\n", decisions[[f]]))
}

# ---- 控制台输出：详细结果 ----
print_results <- function(df, label) {
  if (nrow(df) == 0) return(invisible(NULL))
  for (f in unique(df$file)) {
    sub <- df %>% filter(file == f)
    v1 <- sub$var1[1]; t1 <- sub$var1_type[1]
    v2 <- sub$var2[1]; t2 <- sub$var2_type[1]

    cat(sprintf("\n%s\n", DASH))
    cat(sprintf("  %s  |  %s[%s] vs %s[%s]  (%s)\n", f, v1, t1, v2, t2, label))
    cat(sprintf("%s\n", DASH))

    for (i in seq_len(nrow(sub))) {
      r <- sub[i, ]
      if (r$method == "卡方检验") {
        cat(sprintf("  %-10s  X2=%.2f  df=%d  p=%s  V=%.3f %s\n",
                    r$method, r$stat, r$df,
                    format.pval(r$p, digits = 3), r$effect, r$sig))
      } else if (r$method == "Cramer V") {
        cat(sprintf("  %-10s  V=%.3f\n", r$method, r$stat))
      } else if (r$method == "Spearman") {
        cat(sprintf("  %-10s  rho=%+.3f  p=%s %s\n",
                    r$method, r$stat,
                    format.pval(r$p, digits = 3), r$sig))
      } else {
        cat(sprintf("  %-10s  H=%.2f  df=%d  p=%s %s\n",
                    r$method, r$stat, r$df,
                    format.pval(r$p, digits = 3), r$sig))
      }
    }
  }
}

cat("\n")
print_results(primary_all, "primary")
print_results(chi_all, "chi-square fallback")

# ---- 写入 JSON ----
report_path <- file.path(output_dir, "report.json")
all_for_json <- bind_rows(primary_all, chi_all) %>%
  mutate(p = ifelse(is.na(p), NA, format(p, scientific = TRUE)))
write_json(all_for_json, report_path, pretty = TRUE, auto_unbox = TRUE)

# ---- 写入 Excel (4 sheets) ----
xlsx_path <- file.path(output_dir, "report.xlsx")

fmt_xlsx <- function(df) {
  if (nrow(df) == 0) return(tibble())
  df %>%
    mutate(p = ifelse(is.na(p), "", format.pval(p, digits = 3))) %>%
    select(file, var1, var1_type, var2, var2_type, method, stat, df, p, sig, effect, effect_name)
}

# 按方法分组
spearman_df <- fmt_xlsx(primary_all %>% filter(method == "Spearman"))
cramer_df   <- fmt_xlsx(primary_all %>% filter(method == "Cramer V"))
kruskal_df  <- fmt_xlsx(primary_all %>% filter(method == "Kruskal"))
chisq_df    <- fmt_xlsx(chi_all)

sheets <- list(
  "Spearman"       = spearman_df,
  "Cramer V"       = cramer_df,
  "Kruskal-Wallis" = kruskal_df,
  "Chi-square"     = chisq_df
)
# 去掉空 sheet
sheets <- sheets[sapply(sheets, nrow) > 0]

write.xlsx(sheets, xlsx_path, rowNames = FALSE)

cat(sprintf("\n%s\n", BAR))
cat(sprintf("  [OK] -> %s\n", report_path))
cat(sprintf("  [OK] -> %s (%d sheets)\n", xlsx_path, length(sheets)))
cat(sprintf("%s\n", BAR))
