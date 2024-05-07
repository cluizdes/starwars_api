#%%
import pandas as pd
import requests

def get_data(url):
    r = requests.get(url)
    return r.json()

people_data = []
starships_data = []
planets_data = []
species_data = []
vehicles_data = []
films_data = []

for x in range(1, 15):
    url = f'https://swapi.dev/api/people/{x}'
    people = get_data(url)
    people_data.append(people)
people_data = pd.DataFrame(people_data)


for x in range(1, 15):
    url = f'https://swapi.dev/api/starships/{x}'
    starships = get_data(url)
    starships_data.append(starships)
starships_data = pd.DataFrame(starships_data)

for x in range(1, 15):
    url = f'https://swapi.dev/api/planets/{x}'
    planets = get_data(url)
    planets_data.append(planets)
planets_data = pd.DataFrame(planets_data)

for x in range(1, 15):
    url = f'https://swapi.dev/api/species/{x}'
    species = get_data(url)
    species_data.append(species)
species_data = pd.DataFrame(species_data)

for x in range(1, 15):
    url = f'https://swapi.dev/api/vehicles/{x}'
    vehicles = get_data(url)
    vehicles_data.append(vehicles)
vehicles_data = pd.DataFrame(vehicles_data)

for x in range(1, 15):
    url = f'https://swapi.dev/api/films/{x}'
    films = get_data(url)
    films_data.append(films)
films_data = pd.DataFrame(films_data)