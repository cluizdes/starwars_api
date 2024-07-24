# %% [markdown]
# #   Insights STARS WARS

# %%
import os
import shutil
import duckdb

duckdb_conn = duckdb.connect(database=":memory:", read_only=False)

def mover_arquivos_parquet():
    if not os.path.exists('parquet'):
        os.makedirs('parquet')
    [shutil.move(f, 'parquet') for f in os.listdir('.') if f.endswith('.parquet')]

mover_arquivos_parquet()

df_films = duckdb_conn.from_parquet("./parquet/films_data.parquet").to_df()
df_people = duckdb_conn.from_parquet("./parquet/people_data.parquet").to_df()
df_films = duckdb_conn.from_parquet("./parquet/films_data.parquet").to_df()
df_starships = duckdb_conn.from_parquet("./parquet/starships_data.parquet").to_df()
df_vehicles = duckdb_conn.from_parquet("./parquet/vehicles_data.parquet").to_df()
df_planets = duckdb_conn.from_parquet("./parquet/planets_data.parquet").to_df()
df_species = duckdb_conn.from_parquet("./parquet/species_data.parquet").to_df()

# %% [markdown]
# ## Número total de veículos de Star Wars

# %%
query = """
SELECT COUNT(*) AS Total_Veiculos
FROM df_vehicles;

"""
result = duckdb_conn.execute(query)
result.df()

# %% [markdown]
# ## Fabricantes mais comuns de veículos

# %%

query = """
SELECT manufacturer as Fabricante, COUNT(*) AS Qtd_Veiculos
FROM df_vehicles
WHERE manufacturer not in ('None', 'unknown')
GROUP BY manufacturer
ORDER BY Qtd_Veiculos DESC;
"""
result = duckdb_conn.execute(query)
result.df()

# %% [markdown]
# ## Classes de veículos mais comuns

# %%
query = """
SELECT vehicle_class as Classe_Veiculo, COUNT(*) AS Qtd_Veiculo
FROM df_vehicles
WHERE vehicle_class not in ('None', 'unknown')
GROUP BY vehicle_class
ORDER BY Qtd_Veiculo DESC;
"""
result = duckdb_conn.execute(query)
result.df()


# %% [markdown]
# ## Veículos com o maior número de pilotos

# %%
query = """
SELECT v.name AS Nome_Veiculo, 
       COUNT(p.url) AS Numero_Pilotos
FROM df_vehicles as v
JOIN df_people as p 
ON ARRAY_CONTAINS(p.vehicles, v.url)
WHERE v.vehicle_class NOT IN ('None', 'unknown')
GROUP BY v.name
ORDER BY Numero_Pilotos DESC;
 
"""
result = duckdb_conn.execute(query)
result.df()

# %% [markdown]
# ## Veículos com a maior capacidade de carga

# %%
query = """
SELECT name AS Nome_Veiculo, 
       CASE 
           WHEN TRY_CAST(cargo_capacity AS int64) IS NULL THEN NULL 
           ELSE CAST(cargo_capacity AS int64) 
       END AS Capacidade_Carga
FROM df_vehicles
WHERE cargo_capacity NOT IN ('None', 'unknown')


"""
result = duckdb_conn.execute(query)
result.df()

# %% [markdown]
# ## Veículos que aparecem em mais filmes

# %%
query = """
SELECT v.name AS Nome_Veiculo, COUNT(f.url) AS Numero_Filmes
FROM df_vehicles v
JOIN df_films f 
ON ARRAY_CONTAINS(f.vehicles, v.url)
GROUP BY v.name
ORDER BY Numero_Filmes DESC;
"""
result = duckdb_conn.execute(query)
result.df()


