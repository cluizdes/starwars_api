import pandas as pd
import requests
import shutil
import os

def get_data(url):
    """
    Retorna os dados da url:
    :param url: string
    """
    r = requests.get(url)
    return r.json()

people_data = []
starships_data = []
planets_data = []
species_data = []
vehicles_data = []
films_data = []

for x in range(1, 84):
    url = f'https://swapi.dev/api/people/{x}'
    people = get_data(url)
    people_data.append(people)
people_data = pd.DataFrame(people_data)
people_data.to_parquet('people_data.parquet', index=True)

for x in range(1, 44):
    url = f'https://swapi.dev/api/starships/{x}'
    starships = get_data(url)
    starships_data.append(starships)
starships_data = pd.DataFrame(starships_data)
starships_data.to_parquet('starships_data.parquet', index=True)

for x in range(1,38):
    url = f'https://swapi.dev/api/species/{x}'
    species = get_data(url)
    species_data.append(species)
species_data = pd.DataFrame(species_data)
species_data.to_parquet('species_data.parquet', index=True)

for x in range(1, 74):
    url = f'https://swapi.dev/api/vehicles/{x}'
    vehicles = get_data(url)
    vehicles_data.append(vehicles)
vehicles_data = pd.DataFrame(vehicles_data)
vehicles_data.to_parquet('vehicles_data.parquet', index=True)

for x in range(1, 7):
    url = f'https://swapi.dev/api/films/{x}'
    films = get_data(url)
    films_data.append(films)
films_data = pd.DataFrame(films_data)
films_data.to_parquet('films_data.parquet', index=True)

def organizar_parquet():
    """
    Organizes the parquet files by moving them to a 'parquet' directory.
    """
    if not os.path.exists("parquet"):
        os.makedirs("parquet")
    
    for arquivo in [arq for arq in os.listdir() if arq.endswith(".parquet")]:
        shutil.move(arquivo, "parquet")


organizar_parquet()