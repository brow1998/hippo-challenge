import glob
import pandas as pd
import os
import json

# Configurações iniciais
path = 'data/{layout}'

# Mapeamento de funções de leitura conforme o formato do arquivo
file_formats = {
    "json": pd.read_json,
    "csv": pd.read_csv
}

# Schema esperado para cada tipo de dado
schemas = {
    "pharmacies": {
        "chain": "string",
        "npi": "string"
    },
    "claims": {
        "id": "string",
        "npi": "string",
        "ndc": "string",
        "price": "float",
        "quantity": "int",
        "timestamp": "datetime"
    },
    "reverts": {
        "id": "string",
        "claim_id": "string",
        "timestamp": "datetime"
    }
}

final_dfs = {}

# Processa cada layout (pharmacies, claims e reverts)
for file_layout, file_format in [('pharmacies', 'csv'), ('claims', 'json'), ('reverts', 'json')]:
    dataframes = []  # Reinicia os dataframes para cada layout
    for item in glob.glob(os.path.join(path.format(layout=file_layout), f'*.{file_format}')):
        print(f"\nLendo o arquivo: {item}")
        
        try:
            df = file_formats[file_format](item)
        except Exception as e:
            print(f"Erro ao ler o arquivo {item}: {e}")
            continue

        # Se faltar alguma coluna do schema, cria a coluna com valores NA e avisa
        for col in schemas[file_layout].keys():
            if col not in df.columns:
                print(f"Aviso: Coluna '{col}' ausente no arquivo {item}. Criando coluna com valores NA.")
                df[col] = pd.NA

        # Conversão vetorizada para cada coluna do schema
        for col, col_type in schemas[file_layout].items():
            if col_type == "datetime":
                df[col] = pd.to_datetime(df[col], errors='coerce')
            elif col_type in ["float", "int"]:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            elif col_type == "string":
                df[col] = df[col].astype("string")
        
        # Isola somente as colunas definidas no schema para evitar agregação de colunas extras
        df = df[list(schemas[file_layout].keys())]

        # Identifica linhas com problemas (qualquer valor faltante)
        required_cols = list(schemas[file_layout].keys())
        invalid_mask = df[required_cols].isna().any(axis=1)
        invalid_count = invalid_mask.sum()
        if invalid_count > 0:
            print(f"\n*** {invalid_count} LINHA(S) COM PROBLEMAS NO ARQUIVO {item} ***")
            for idx, row in df[invalid_mask].iterrows():
                print("==========================================")
                print(f"LINHA PROBLEMÁTICA - Índice: {idx}")
                print(row.to_dict())
                print("==========================================")
        
        # Remove as linhas problemáticas
        df_valid = df.dropna(subset=required_cols)
        print(f"Arquivo {item}: {len(df_valid)} linhas válidas de {len(df)}.")
        dataframes.append(df_valid)

    if dataframes:
        final_df = pd.concat(dataframes, ignore_index=True)
        print(f"\nDados de {file_layout} carregados e validados:")
        print(final_df.head())
        final_dfs[file_layout] = final_df
    else:
        print(f"Nenhum dado válido encontrado para {file_layout}.")


# pharmacies = final_dfs.get('pharmacies')
# claims = final_dfs.get('claims')
# reverts = final_dfs.get('reverts')

# --- Cálculo de métricas para claims e reverts ---

# Primeiro, vamos trabalhar com o DataFrame de claims que já está validado
claims_df = final_dfs.get("claims")
reverts_df = final_dfs.get("reverts")

if claims_df is not None:
    # Cria uma coluna com o preço unitário para cada claim
    claims_df["unit_price"] = claims_df["price"] / claims_df["quantity"]
    
    # Agrupa os dados de claims por npi e ndc para calcular os indicadores
    metrics_claims = claims_df.groupby(["npi", "ndc"]).agg(
        fills=("id", "count"),
        total_price=("price", "sum"),
        avg_price=("unit_price", "mean")
    ).reset_index()
    
    # Para contar os reverts, precisamos relacionar os reverts com os claims
    # Fazemos o merge dos claims com reverts usando o id do claim e claim_id do revert
    if reverts_df is not None:
        claims_reverts = claims_df[["id", "npi", "ndc"]].merge(
            reverts_df,
            left_on="id",
            right_on="claim_id",
            how="left"
        )
        # Em cada linha, se houver um valor em 'claim_id' significa que esse claim foi revertido
        reverts_count = claims_reverts.groupby(["npi", "ndc"]).apply(
            lambda df: df["claim_id"].notna().sum()
        ).reset_index(name="reverted")
    else:
        print("Nenhum dado de reverts encontrado.")
        reverts_count = pd.DataFrame(columns=["npi", "ndc", "reverted"])
    
    # Junta os dados de métricas dos claims com a contagem de reverts
    metrics = metrics_claims.merge(
        reverts_count,
        on=["npi", "ndc"],
        how="left"
    )
    metrics["reverted"] = metrics["reverted"].fillna(0).astype(int)
    metrics["total_price"] = metrics["total_price"].round(2)
    metrics["avg_price"] = metrics["avg_price"].round(2)
    
    print("\n--- Métricas calculadas ---")
    print(metrics.head())
    
    # Aqui você pode salvar o resultado em JSON:
    metrics.to_json("data/output/metrics.json", orient="records", indent=4)
else:
    print("Nenhum dado de claims encontrado.")

# --- Goal 3: Top 2 Chain por Drug (ndc) com base no preço unitário médio ---
pharmacies_df = final_dfs.get("pharmacies")
claims_df = final_dfs.get("claims")

if claims_df is not None and pharmacies_df is not None:
    # Realiza o merge dos claims com as pharmacies para obter a coluna "chain" via "npi"
    claims_with_chain = claims_df.merge(pharmacies_df, on="npi", how="left")
    
    # Garante que o preço unitário esteja calculado
    if "unit_price" not in claims_with_chain.columns:
        claims_with_chain["unit_price"] = claims_with_chain["price"] / claims_with_chain["quantity"]
    
    # Agrupa por ndc e chain para calcular a média do preço unitário
    group_chain = claims_with_chain.groupby(["ndc", "chain"]).agg(
        avg_unit_price=("unit_price", "mean")
    ).reset_index()
    
    # Para cada ndc, seleciona os top 2 chains com menor preço unitário médio
    top_chains = []
    for ndc, group in group_chain.groupby("ndc"):
        group_sorted = group.sort_values("avg_unit_price")
        top_2 = group_sorted.head(2)
        chain_list = [
            {"name": row["chain"], "avg_price": round(row["avg_unit_price"], 2)}
            for _, row in top_2.iterrows()
        ]
        top_chains.append({"ndc": ndc, "chain": chain_list})
    
    print("\n--- Top 2 Chain por Drug ---")
    print(json.dumps(top_chains[:5], indent=4))  # Exibe as 5 primeiras para conferência
    
    # Opcional: salvar em arquivo JSON
    with open("data/output/top_chains.json", "w", encoding="utf-8") as f:
        json.dump(top_chains, f, indent=4)
else:
    print("Dados insuficientes para calcular Top 2 Chain por Drug.")

# --- Goal 4: Quantidade Mais Comum Prescrita para Cada Drug (ndc) ---
if claims_df is not None:
    # Agrupa por ndc e quantidade, contando a frequência de cada quantidade
    freq = claims_df.groupby(["ndc", "quantity"]).size().reset_index(name="count")
    
    top_quantities = []
    for ndc, group in freq.groupby("ndc"):
        # Ordena as quantidades por frequência (maior para menor)
        group_sorted = group.sort_values("count", ascending=False)
        quantities_list = group_sorted["quantity"].tolist()
        top_quantities.append({
            "ndc": ndc,
            "most_prescribed_quantity": quantities_list
        })
    
    print("\n--- Quantidade Mais Comum Prescrita por Drug ---")
    print(json.dumps(top_quantities[:5], indent=4))
    
    # Opcional: salvar em arquivo JSON
    with open("data/output/most_prescribed_quantities.json", "w", encoding="utf-8") as f:
        json.dump(top_quantities, f, indent=4)
else:
    print("Dados de claims insuficientes para calcular a quantidade mais comum prescrita.")
