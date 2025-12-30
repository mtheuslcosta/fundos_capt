# ============================================================
# CAPTAÇÃO LÍQUIDA DE FUNDOS (FI) – CVM
# VERSÃO CORRIGIDA COM ROLLING ROBUSTO E SEM DUPLICAÇÃO
# ============================================================
import requests
import zipfile
import io
import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

# ============================================================
# 1. FUNÇÃO PARA GERAR OS ÚLTIMOS N MESES (YYYYMM)
# ============================================================
def ultimos_meses(n=9):
    """Retorna lista dos últimos N meses no formato YYYYMM."""
    hoje = datetime.today().replace(day=1)
    return [
        (hoje - relativedelta(months=i)).strftime("%Y%m")
        for i in range(1, n + 1)
    ]

# ============================================================
# 2. DOWNLOAD DOS DADOS DA CVM
# ============================================================
def download_inf_diario_fi(yyyymm):
    """Baixa arquivo ZIP de informe diário da CVM."""
    url = f"https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_{yyyymm}.zip"
    response = requests.get(url)
    response.raise_for_status()
    return response.content

# ============================================================
# 3. EXTRAÇÃO E CONSOLIDAÇÃO DE DADOS
# ============================================================
print("=" * 60)
print("ETAPA 1: DOWNLOAD E CONSOLIDAÇÃO DOS DADOS")
print("=" * 60)

dfs = []

for mes in ultimos_meses(9):
    print(f"Baixando {mes}...")
    zip_content = download_inf_diario_fi(mes)

    with zipfile.ZipFile(io.BytesIO(zip_content)) as z:
        nome_csv = z.namelist()[0]

        with z.open(nome_csv) as f:
            df = pd.read_csv(f, sep=";", encoding="latin1")

            # Filtrar apenas Fundos de Investimento (FI)
            df = df[df["TP_FUNDO_CLASSE"] == "FI"]

            # Calcular captação líquida: inscrições - resgates
            df["Captacao_Liquida"] = df["CAPTC_DIA"] - df["RESG_DIA"]

            dfs.append(df)

print(f"✓ {len(dfs)} meses carregados com sucesso.")

# ============================================================
# 4. CONSOLIDAÇÃO EM UM ÚNICO DATAFRAME
# ============================================================
df = pd.concat(dfs, ignore_index=True)
print(f"✓ Total de registros: {len(df):,}")

# ============================================================
# 4B. FILTRO FINAL: GARANTIR APENAS TP_FUNDO_CLASSE = "FI"
# ============================================================
# Este filtro adicional garante que NENHUM registro que não seja "FI"
# seja mantido, mesmo que tenha passado pelo filtro anterior
print("\nAplicando filtro final para TP_FUNDO_CLASSE = 'FI'...")

registros_antes = len(df)
df = df[df["TP_FUNDO_CLASSE"] == "FI"].copy()
registros_depois = len(df)

registros_removidos = registros_antes - registros_depois
if registros_removidos > 0:
    print(f"⚠️  {registros_removidos:,} registros NÃO-FI foram removidos")
else:
    print(f"✓ Nenhum registro não-FI encontrado")

print(f"✓ Total de registros após filtro: {registros_depois:,}")

# ============================================================
# 5. CONVERSÃO DE TIPOS E NORMALIZAÇÃO
# ============================================================
print("\n" + "=" * 60)
print("ETAPA 2: CONVERSÃO DE TIPOS E NORMALIZAÇÃO")
print("=" * 60)

# Data: converter DT_COMPTC para datetime
df["Data_Comptc"] = pd.to_datetime(df["DT_COMPTC"], errors="coerce")

# Captação: garantir tipo numérico
df["Captacao_Liquida"] = pd.to_numeric(df["Captacao_Liquida"], errors="coerce")

# CNPJ: remover caracteres especiais (deixar apenas dígitos)
df["CNPJ_FUNDO_CLASSE"] = (
    df["CNPJ_FUNDO_CLASSE"]
    .astype(str)
    .str.replace(r"\D", "", regex=True)
)

print("✓ Tipos convertidos e normalizados.")

# ============================================================
# 6. AGREGAÇÃO 1 LINHA POR FUNDO / DATA
# ============================================================
print("\n" + "=" * 60)
print("ETAPA 3: AGREGAÇÃO (1 LINHA POR FUNDO / DATA)")
print("=" * 60)

# Agrupar por (FUNDO, DATA) e somar captações
# Isso evita múltiplas linhas com mesma combinação
df_agg = (
    df
    .groupby(["CNPJ_FUNDO_CLASSE", "Data_Comptc"], as_index=False)
    .agg({"Captacao_Liquida": "sum"})
)

print(f"✓ Registros antes da agregação: {len(df):,}")
print(f"✓ Registros após agregação: {len(df_agg):,}")

df = df_agg

# ============================================================
# 7. VALIDAÇÃO E DETECÇÃO DE DUPLICAÇÃO
# ============================================================
print("\n" + "=" * 60)
print("ETAPA 4: VALIDAÇÃO (DETECÇÃO DE DUPLICAÇÃO)")
print("=" * 60)

# Verificar se ainda há duplicatas (não deveria haver)
duplicados = df.groupby(["CNPJ_FUNDO_CLASSE", "Data_Comptc"]).size()
n_duplicados = (duplicados > 1).sum()

if n_duplicados > 0:
    print(f"⚠️  AVISO: {n_duplicados} combinações FUNDO/DATA duplicadas detectadas!")
    print("   Isso indica um problema na agregação prévia.")
    print("   Exemplos:")
    print(duplicados[duplicados > 1].head())
    
    # Aplicar agregação novamente (força última tentativa)
    df = df.groupby(["CNPJ_FUNDO_CLASSE", "Data_Comptc"], as_index=False).agg({
        "Captacao_Liquida": "sum"
    })
    print(f"   → Reagregação aplicada. Nova contagem: {len(df):,}")
else:
    print("✓ Nenhuma duplicação detectada — dados estão limpos.")

# ============================================================
# 8. CÁLCULO DAS JANELAS TEMPORAIS (ROLLING)
# ============================================================
print("\n" + "=" * 60)
print("ETAPA 5: CÁLCULO DE JANELAS TEMPORAIS (30/90/180 DIAS)")
print("=" * 60)

# Ordenar por fundo e data (essencial para rolling)
df = df.sort_values(["CNPJ_FUNDO_CLASSE", "Data_Comptc"]).reset_index(drop=True)

# Definir Data_Comptc como índice (obrigatório para rolling com períodos)
df = df.set_index("Data_Comptc")

print("Calculando rolling para:")
print("  • Captacao_30D  (últimos 30 dias)")
print("  • Captacao_90D  (últimos 90 dias)")
print("  • Captacao_180D (últimos 180 dias)")

# JANELA DE 30 DIAS
# Para cada fundo, soma a captação dos últimos 30 dias
# min_periods=1: mesmo com poucos dados, gera um valor
df["Captacao_30D"] = (
    df.groupby("CNPJ_FUNDO_CLASSE")["Captacao_Liquida"]
    .rolling("30D", min_periods=1)
    .sum()
    .reset_index(level=0, drop=True)
)

# JANELA DE 90 DIAS
df["Captacao_90D"] = (
    df.groupby("CNPJ_FUNDO_CLASSE")["Captacao_Liquida"]
    .rolling("90D", min_periods=1)
    .sum()
    .reset_index(level=0, drop=True)
)

# JANELA DE 180 DIAS
df["Captacao_180D"] = (
    df.groupby("CNPJ_FUNDO_CLASSE")["Captacao_Liquida"]
    .rolling("180D", min_periods=1)
    .sum()
    .reset_index(level=0, drop=True)
)

# Resetar índice: colocar Data_Comptc de volta como coluna
df = df.reset_index()

print("✓ Cálculos temporais finalizados com sucesso.")

# ============================================================
# 9. DOWNLOAD DO CADASTRO DE FUNDOS
# ============================================================
print("\n" + "=" * 60)
print("ETAPA 6: ENRIQUECIMENTO COM NOMES DE FUNDOS")
print("=" * 60)

print("Baixando cadastro de fundos...")
df_cad = pd.read_csv(
    "https://dados.cvm.gov.br/dados/FI/CAD/DADOS/cad_fi.csv",
    sep=";",
    encoding="latin1"
)

# Manter apenas CNPJ e nome social
df_cad = df_cad[["CNPJ_FUNDO", "DENOM_SOCIAL"]].drop_duplicates()

# Normalizar CNPJ do cadastro
df_cad["CNPJ_FUNDO"] = (
    df_cad["CNPJ_FUNDO"]
    .astype(str)
    .str.replace(r"\D", "", regex=True)
)

print(f"✓ Cadastro carregado: {len(df_cad):,} fundos únicos.")

# ============================================================
# 10. MERGE COM DADOS DE CAPTAÇÃO
# ============================================================
print("Mesclando dados de captação com nomes de fundos...")

# Merge (left join): manter todos os registros de captação
df = df.merge(
    df_cad,
    left_on="CNPJ_FUNDO_CLASSE",
    right_on="CNPJ_FUNDO",
    how="left"
).drop(columns="CNPJ_FUNDO")

print(f"✓ Merge realizado. Registros finais: {len(df):,}")

# ============================================================
# 15. VERIFICAÇÃO FINAL DE DUPLICAÇÃO
# ============================================================
print("\n" + "=" * 60)
print("ETAPA 11: VERIFICAÇÃO FINAL")
print("=" * 60)

# Contar registros únicos (FUNDO, DATA)
unicos = df.groupby(["CNPJ_FUNDO_CLASSE", "Data_Comptc"]).size()
n_unicos = len(unicos)
n_registros = len(df)

if n_unicos == n_registros:
    print(f"✓ PERFEITO: 1 linha por fundo/data ({n_registros:,} registros únicos)")
else:
    print(f"⚠️  DUPLICAÇÃO DETECTADA: {n_registros:,} registros, mas apenas {n_unicos:,} únicos")
    print(f"   Razão de duplicação: {n_registros / n_unicos:.2f}x")
    duplicados = unicos[unicos > 1]
    print(f"   Fundos/datas com >1 linha: {len(duplicados)}")

# ============================================================
# 12. FILTRO: MANTER APENAS O DIA MAIS RECENTE
# ============================================================
print("\n" + "=" * 60)
print("ETAPA 8: FILTRO PARA DIA MAIS RECENTE")
print("=" * 60)

# Excel tem limite máximo de ~1 milhão de linhas
# Para evitar erro, manteremos apenas o último dia (data máxima)
data_maxima = df["Data_Comptc"].max()
print(f"Data máxima no dataset: {data_maxima}")

df_filtrado = df[df["Data_Comptc"] == data_maxima].copy()

print(f"✓ Registros no último dia: {len(df_filtrado):,}")
print(f"✓ Fundos únicos neste dia: {df_filtrado['CNPJ_FUNDO_CLASSE'].nunique():,}")

# ============================================================
# 13. SELEÇÃO DE COLUNAS E RENOMEAÇÃO
# ============================================================
print("\n" + "=" * 60)
print("ETAPA 9: FORMATAÇÃO DO LAYOUT FINAL")
print("=" * 60)

# Selecionar colunas e renomear para rótulos amigáveis
df_final = df_filtrado[[
    "DENOM_SOCIAL",
    "Data_Comptc",
    "Captacao_Liquida",
    "Captacao_30D",
    "Captacao_90D",
    "Captacao_180D"
]].rename(columns={
    "DENOM_SOCIAL": "Nome_Fundo",
    "Data_Comptc": "Data",
    "Captacao_Liquida": "Captacao_Líquida_Diária"
})

# Remover linhas onde o nome do fundo é nulo (dados incompletos)
df_final = df_final.dropna(subset=["Nome_Fundo"])

# Formatar data como DD/MM/AAAA (string, sem hora)
# OBS: Usar coluna "Data" (já renomeada, não "Data_Comptc")
df_final["Data"] = df_final["Data"].dt.strftime("%d/%m/%Y")

print("✓ Layout final preparado.")

# ============================================================
# 14. EXPORTAÇÃO PARA EXCEL
# ============================================================
print("\n" + "=" * 60)
print("ETAPA 10: EXPORTAÇÃO PARA EXCEL")
print("=" * 60)

output_path = "captacao_liquida_fi.xlsx"

df_final.to_excel(
    output_path,
    index=False,
    sheet_name="Captacao_Liquida"
)

print(f"✓ Arquivo exportado com sucesso: {output_path}")

# ============================================================
# 14. RESUMO E ESTATÍSTICAS
# ============================================================
print("\n" + "=" * 60)
print("RESUMO FINAL")
print("=" * 60)
print(f"Total de registros no arquivo final: {len(df_final):,}")
print(f"Total de fundos únicos: {df_final['Nome_Fundo'].nunique():,}")
print(f"Range de datas: {df_final['Data'].min()} a {df_final['Data'].max()}")
print(f"Arquivo salvo em: {output_path}")
print("=" * 60)
print("✓ ETL FINALIZADO COM SUCESSO!")
print("=" * 60)

# ============================================================
# 15. EXPORTAÇÃO SIMPLES E OTIMIZADA PARA PDF
# ============================================================
print("\n" + "=" * 60)
print("ETAPA 11: EXPORTAÇÃO PARA PDF (FOCO EM NOME_FUNDO)")
print("=" * 60)

from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.pyplot as plt

pdf_path = "captacao_liquida_fi.pdf"

# Menos linhas → mais espaço horizontal
linhas_por_pagina = 18

# Garantir 2 casas decimais
colunas_numericas = [
    "Captacao_Líquida_Diária",
    "Captacao_30D",
    "Captacao_90D",
    "Captacao_180D"
]

df_pdf = df_final.copy()
df_pdf[colunas_numericas] = df_pdf[colunas_numericas].round(2)

with PdfPages(pdf_path) as pdf:
    for i in range(0, len(df_pdf), linhas_por_pagina):
        df_pagina = df_pdf.iloc[i:i + linhas_por_pagina]

        # A4 horizontal
        fig, ax = plt.subplots(figsize=(11.7, 8.3))
        ax.axis("off")

        tabela = ax.table(
            cellText=df_pagina.values,
            colLabels=df_pagina.columns,
            loc="center",
            cellLoc="left"
        )

        tabela.auto_set_font_size(False)
        tabela.set_fontsize(6.5)     # fonte menor
        tabela.scale(0.95, 1.3)      # tabela mais estreita no geral

        # ===============================
        # AJUSTE FORTE DAS LARGURAS
        # ===============================
        colunas = df_pagina.columns.tolist()
        idx_nome = colunas.index("Nome_Fundo")

        for (row, col), cell in tabela.get_celld().items():
            if col == idx_nome:
                cell.set_width(cell.get_width() * 2.2)   # Nome_Fundo bem largo
            else:
                cell.set_width(cell.get_width() * 0.6)   # numéricas bem compactas

        pdf.savefig(fig, bbox_inches="tight")
        plt.close(fig)

print(f"✓ PDF exportado com sucesso: {pdf_path}")




