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
# ## Planetas mais populosos

# %%
query = """
SELECT 
  name,
  CAST (population as int64) as population
FROM 
  df_planets
WHERE
population <> 'unknown'
ORDER BY 
  population DESC
LIMIT 10;

"""
result = duckdb_conn.execute(query)
result.df()

# %% [markdown]
# ## Os diferentes tipos de clima

# %%

query = """
SELECT 
  climate,
  COUNT(*) AS count
FROM 
  df_planets
WHERE climate not in ('None', 'unknown')  
GROUP BY 
  climate
ORDER BY 
  count DESC;
"""
result = duckdb_conn.execute(query)
result.df()

# %% [markdown]
# ## Tamanho dos planeta

# %%
query = """
SELECT 
  name as Nome,
  CAST(diameter AS int64) AS Diametro
FROM 
  df_planets
WHERE diameter not in ('None', 'unknown')  
ORDER BY 
  diameter DESC
LIMIT 10;


"""
result = duckdb_conn.execute(query)
result.df()


# %% [markdown]
# ## Filmes em que os planetas aparecerem

# %%
query = """
SELECT 
    p.name AS Planeta,
    f.title AS Filme
FROM df_planets as p
JOIN df_films as f
on ARRAY_CONTAINS(p.films, f.url)
WHERE p.name not in ('None', 'unknown') 
"""
result = duckdb_conn.execute(query)
result.df()

# %% [markdown]
# ## Quem mora nos planetas

# %%
query = """
SELECT p.name AS Nome,
    pl.name AS Planeta
FROM df_people as p
join df_planets as pl
on p.homeworld = pl.url
ORDER BY pl.name
"""
result = duckdb_conn.execute(query)
result.df()

# %% [markdown]
# ## Temperatura dos planeta

# %%
query = """
SELECT 
name as Nome,
climate as Clima,
terrain as Terreno,
surface_water as Agua
FROM df_planets as p
WHERE diameter not in ('None', 'unknown') 
ORDER BY Agua, Clima
"""
result = duckdb_conn.execute(query)
result.df()


