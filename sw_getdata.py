import pandas as pd
import requests

def get_data(url):
    """
    Fetches data from the given URL and returns it as JSON.

    Args:
        url (str): The URL to fetch data from.

    Returns:
        dict: The fetched data in JSON format.
    """
    r = requests.get(url)
    return r.json()

people_data = []
starships_data = []
planets_data = []
species_data = []
vehicles_data = []
films_data = []

for x in range(1, 3000):
    url = f'https://swapi.dev/api/people/{x}'
    people = get_data(url)
    people_data.append(people)
people_data = pd.DataFrame(people_data)
people_data.to_parquet('people_data.parquet')


for x in range(1, 1000):
    url = f'https://swapi.dev/api/starships/{x}'
    starships = get_data(url)
    starships_data.append(starships)
starships_data = pd.DataFrame(starships_data)
starships_data.to_parquet('starships_data.parquet')

for x in range(1, 2000):
    url = f'https://swapi.dev/api/planets/{x}'
    planets = get_data(url)
    planets_data.append(planets)
planets_data = pd.DataFrame(planets_data)
planets_data.to_parquet('planets_data.parquet')

for x in range(1, 2000):
    url = f'https://swapi.dev/api/species/{x}'
    species = get_data(url)
    species_data.append(species)
species_data = pd.DataFrame(species_data)
species_data.to_parquet('species_data.parquet')

for x in range(1, 1000):
    url = f'https://swapi.dev/api/vehicles/{x}'
    vehicles = get_data(url)
    vehicles_data.append(vehicles)
vehicles_data = pd.DataFrame(vehicles_data)
vehicles_data.to_parquet('people_data.parquet')

for x in range(1, 100):
    url = f'https://swapi.dev/api/films/{x}'
    films = get_data(url)
    films_data.append(films)
films_data = pd.DataFrame(films_data)
films_data.to_parquet('films_data.parquet')