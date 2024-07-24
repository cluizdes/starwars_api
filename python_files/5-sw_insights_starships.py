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
# ## Número total de naves de Star Wars

# %%
query = """
SELECT COUNT(*) AS Total_StarShips
FROM df_starships;
"""
result = duckdb_conn.execute(query)
result.df()

# %% [markdown]
# ## Fabricantes mais comuns de naves

# %%

query = """
SELECT manufacturer as Fabricante, COUNT(*) AS Naves
FROM df_starships
WHERE Fabricante <> ('None')
GROUP BY manufacturer
ORDER BY Naves DESC;

"""
result = duckdb_conn.execute(query)
result.df()

# %% [markdown]
# ## Comprimento médio das naves por classe

# %%
query = """
SELECT starship_class as Classe, AVG(CAST(REPLACE(length, ',', '.') AS DOUBLE)) AS Comprimento_Medio
FROM df_starships
WHERE starship_class <> ('None')
GROUP BY starship_class;
"""

result = duckdb_conn.execute(query)
result.df()


# %% [markdown]
# ## Naves com maior capacidade de carga

# %%
query = """
SELECT name as Nome, CAST (cargo_capacity AS int64) as Capacidade
FROM df_starships
WHERE cargo_capacity not in ('None', 'unknown')
ORDER BY cargo_capacity DESC
LIMIT 10;
"""
result = duckdb_conn.execute(query)
result.df()

# %% [markdown]
# ## Filmes em que cada nave aparece

# %%
query = """
SELECT s.name AS Nome_Nave, f.title AS Nome_Filme
FROM df_starships s
JOIN df_films f 
ON ARRAY_CONTAINS(f.starships, s.url)
"""
result = duckdb_conn.execute(query)
result.df()

# %% [markdown]
# ## Número de naves por piloto

# %%
query = """
SELECT p.name AS Nome_Piloto, COUNT(*) AS Qtd_Nave
FROM df_starships s
JOIN df_people p ON p.url = ANY(s.pilots)
GROUP BY p.name
ORDER BY Qtd_Nave DESC;

"""
result = duckdb_conn.execute(query)
result.df()

# %% [markdown]
# ## Naves que podem transportar mais passageiros do que a tripulação

# %%
query = """
SELECT name as Nome_Nave, passengers as Qtd_Passageiros, crew as Tripulacao
FROM df_starships
WHERE passengers > crew and passengers not in ('None', 'None', 'unknown');
"""
result = duckdb_conn.execute(query)
result.df()


