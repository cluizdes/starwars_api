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

# %%
query = """
SELECT *
FROM df_species as s
limit 5
;

"""
result = duckdb_conn.execute(query)
result.df()

# %% [markdown]
# ## Distribuição de Espécies por Planeta

# %%
query = """
SELECT p.name as Planeta, s.name as Especie, COUNT(s.name) AS Qtd_Especie
FROM df_species as s
JOIN df_planets as p
ON s.homeworld = p.url
GROUP BY p.name, s.name;

"""
result = duckdb_conn.execute(query)
result.df()

# %% [markdown]
# ## Relação entre Espécies e Filmes

# %%

query = """
SELECT
    s.name AS Especie,
    COUNT(DISTINCT f.title) AS Qtd_Filmes
FROM
    df_species as s
JOIN
    df_films as f 
ON ARRAY_CONTAINS(f.species, s.url)
GROUP BY
    s.name

"""
result = duckdb_conn.execute(query)
result.df()

# %% [markdown]
# ## Comparação de Características Físicas

# %%
query = """
SELECT
    classification as Classificação,
    ROUND(AVG(TRY_CAST(REPLACE(REPLACE(REPLACE(average_height, 'unknown', '0'), 'n/a', '0'), 'indefinite', '0') AS FLOAT)),2) AS Altura_Media,
    ROUND(AVG(TRY_CAST(REPLACE(REPLACE(REPLACE(average_lifespan, 'unknown', '0'), 'n/a', '0'), 'indefinite', '0') AS FLOAT)),2) AS Expectativa_Med_Vida
FROM
    df_species
WHERE
    classification not in ('unknown','n/a','indefinite') 
GROUP BY
    classification;
"""
result = duckdb_conn.execute(query)
result.df()


# %% [markdown]
# ## Idiomas

# %%
query = """
SELECT
    language as Lingua,
    COUNT(DISTINCT name) AS Qtd_Especies
FROM
    df_species
WHERE language not in('unknown', 'n/a') 
GROUP BY
    language;

"""
result = duckdb_conn.execute(query)
result.df()

# %% [markdown]
# ## Qtde de Especies por Filmes

# %%
query = """
SELECT
    f.title AS Filme,
    COUNT(DISTINCT s.name) AS Qtde_Especie
FROM
    df_species as s
JOIN
    df_films as f 
ON ARRAY_CONTAINS(f.species, s.url)
GROUP BY
    f.title;

"""
result = duckdb_conn.execute(query)
result.df()


