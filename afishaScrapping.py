from bs4 import BeautifulSoup
import requests
from pyspark.sql import SparkSession
import pandas as pd

url = "https://afisha.tut.by/film/"
response = requests.get(url)

soup = BeautifulSoup(response.text, 'html.parser')
list_elements = soup.findAll('div', {'id': 'events-block'})[0].findAll('li', {'class': 'lists__li'})

links = []
for el in list_elements:
    ln = el.findAll('a', {'class': 'name'})
    if ln:
        links.append(ln[0]['href'])

movies = {}
i = 0
for url in links:
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    name = soup.find('h1', {'id': 'event-name'}).text

    if soup.find('td', {'class', 'year'}) is not None:
        year = soup.find('td', {'class', 'year'}).text
    else:
        year = 'Unknown'

    if soup.find('td', {'class', 'author'}) is not None:
        country = soup.find('td', {'class', 'author'}).text
    else:
        country = 'Unknown'

    if soup.find('td', {'class', 'duration'}) is not None:
        duration = soup.find('td', {'class', 'duration'}).text
    else:
        duration = 'Unknown'

    container = soup.find('div', {'class': 'title__labels'})
    if len(container.findAll('span', {'class', 'label'})) != 0:
        if len(container.findAll('span', {'class', 'label'})) == 1:
            for element in container.findAll('span', {'class', 'label'}):
                if 'Премьера' not in element.text:
                    audience = container.find('span', {'class', 'label'}).text
                else:
                    audience = 'Unknown'
        else:
            h = container.findAll('span', {'class', 'label'})[0]
            h.decompose()
            audience = container.find('span', {'class', 'label'}).text
    else:
        audience = 'Unknown'

    if soup.find('td', {'class', 'genre'}) is not None:
        all_genres = ""
        for genre in soup.find('td', {'class', 'genre'}).findAll('p'):
            all_genres = all_genres + genre.text + ' '
    else:
        genres = 'Unknown'

    if soup.find('span', {'class', 'rating-big__value'}) is not None:
        rate = soup.find('span', {'class', 'rating-big__value'}).text
    else:
        rate = 'Unknown'

    for element in soup.findAll("p"):
        if "Режиссер: " not in element.text:
            continue
        director = element.text[10:]

    for element in soup.findAll("p"):
        if "В ролях: " not in element.text:
            continue
        actors = element.text[8:]

    movies[name] = {}
    movies[name]['name'] = name
    movies[name]['url'] = url
    movies[name]['director'] = director
    movies[name]['year'] = year
    movies[name]['country'] = country
    movies[name]['duration'] = duration
    movies[name]['audience'] = audience
    movies[name]['genres'] = all_genres
    movies[name]['rate'] = rate
    movies[name]['actors'] = actors
    print(movies[name])
