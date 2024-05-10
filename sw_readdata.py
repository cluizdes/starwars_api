# %%
import duckdb
from google.cloud import storage
import os

duckdb_conn = duckdb.connect(database=":memory:", read_only=False)

bucket_name = "globo_swdata"
json_path = "credentials_valhalla.json"


def list_gcs_files():
    storage_client = storage.Client.from_service_account_json(json_path)
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix="sw_parquet")
    files = list()
    for blob in blobs:
        name = blob.name.split("/")[-1]
        if name.endswith("parquet"):
            bucket_files = name
            files.append(bucket_files)
    return files
def download_gcs_file(file_name):
    storage_client = storage.Client.from_service_account_json(json_path)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(f"sw_parquet/{file_name}")
    local_path = os.path.join("/mnt/f/github/globo/", file_name)
    blob.download_to_filename(local_path)
    return local_path
# Download the first file from the list
files = list_gcs_files()
for file in files:
    download_gcs_file(file)
#Load the downloaded file into a DataFrame
df_films = duckdb_conn.from_parquet("films_data.parquet").to_df()
df_people = duckdb_conn.from_parquet("people_data.parquet").to_df()
df_films = duckdb_conn.from_parquet("films_data.parquet").to_df()
df_starships = duckdb_conn.from_parquet("starships_data.parquet").to_df()
df_vehicles = duckdb_conn.from_parquet("vehicles_data.parquet").to_df()
df_planets = duckdb_conn.from_parquet("planets_data.parquet").to_df()
df_species = duckdb_conn.from_parquet("species_data.parquet").to_df()

# %%
query = """
    SELECT name, 
    	rotation_period, 
        orbital_period, 
        diameter, 
        climate, 
        gravity, 
        terrain, 
        surface_water, 
        population
FROM df_planets
ORDER BY 
climate ASC,
population ASC,
name ASC
LIMIT 60

"""
result = duckdb_conn.execute(query)
result.df()



SELECT 
  title,
  COUNT(DISTINCT character.element) AS count_characters
FROM 
  `starwar.sw_films`, 
  UNNEST(characters.list) AS character
GROUP BY 
  title;

