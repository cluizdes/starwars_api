# %% [markdown]
# #   Insights STARS WARS - Teste Prático Globo

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
# ## Quantidade de personagens nos filmes

# %%
query = """
SELECT 
  title AS Filme,
  COUNT(DISTINCT character) AS Personagens
FROM 
  df_films,
  UNNEST(STRING_SPLIT(CAST(characters AS VARCHAR), ',')) AS character
GROUP BY 
  title;
"""
result = duckdb_conn.execute(query)
result.df()

# %% [markdown]
# ## Top 10 Personagens que apareceram nos filmes

# %%

query = """
WITH CharacterCounts AS (
  SELECT 
    films.title,
    characters.element AS most_appeared_character,
    COUNT(*) AS appearances
  FROM 
    df_films AS films
  CROSS JOIN UNNEST(films.characters) AS characters(element)
  GROUP BY 
    films.title, characters.element
),

People AS (
  SELECT
    url,
    name
  FROM
    df_people
),

CharacterFilmCounts AS (
  SELECT
    p.name AS Personagem,
    COUNT(DISTINCT cc.title) AS Qtde_de_Filmes
  FROM
    CharacterCounts cc
  JOIN
    People p
  ON
    cc.most_appeared_character = p.url
  GROUP BY
    p.name
)

SELECT * FROM CharacterFilmCounts
ORDER BY Qtde_de_Filmes DESC
LIMIT 10;


"""
result = duckdb_conn.execute(query)
result.df()

# %%
query = """
SELECT 
  *
FROM 
  df_films
LIMIT 5; -- Verificar os dados do DataFrame df_films

SELECT 
  *
FROM 
  df_people
LIMIT 5; -- Verificar os dados do DataFrame df_people

"""
result = duckdb_conn.execute(query)
result.df()

