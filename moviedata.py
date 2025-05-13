import requests
from bs4 import BeautifulSoup
import time
import boto3
import pandas as pd
import datetime
today=datetime.date.today
s3 =boto3.client('s3')
bucket_name="movies-raw-data"
s3_key=f"imdb_data/movie.csv"

top_250_url = "https://www.imdb.com/chart/top"

headers = {'Accept': '*/*', 'Connection': 'keep-alive', 'User-Agent': 'Mozilla/5.0 (Windows NT 6.0) AppleWebKit/537.36 (KHTML,like Gecko) Chrome/70.0.3538.110 Safari/537.36', 'Accept-Language':'en-US;q=0.5,en;q=0.3', 'Cache-Control': 'max-age=0', 'Upgrade-Insecure-Requests': '1'}

response = requests.get(top_250_url, headers=headers)

soup = BeautifulSoup(response.content, 'html.parser')
movies = soup.select('a.ipc-title-link-wrapper')[:25]  # top 25 movies
print("gettting movies links")
movie_data = []

for movie in movies:
    title = movie.text.split(".")[1]
    link = "https://www.imdb.com" + movie['href']
    
    movie_response = requests.get(link, headers=headers)
    movie_soup = BeautifulSoup(movie_response.text, 'html.parser')
    
    try:
        rating = movie_soup.select_one('span.sc-d541859f-1').text
    except:
        rating = "N/A"

    try:
        director_section = movie_soup.select_one('a.ipc-metadata-list-item__list-content-item')
        director = director_section.text.strip() if director_section else "N/A"
    except:
        director = "N/A"
    
    movie_data.append({
        "Title": title.strip(),
        "IMDb Rating": rating,
        "Director": director
    })
    lenth=len(movie_data)
    print(f'{lenth} movies data stored')
    time.sleep(1)

df=pd.DataFrame(movie_data)
file_path=f"revision//database//movie.csv"
df.to_csv(file_path,index=False)

s3.upload_file(file_path,bucket_name,s3_key)
print("file uploaded to s3")

