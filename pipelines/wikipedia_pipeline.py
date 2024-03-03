import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
from datetime import datetime
from dotenv import load_dotenv
import os
from geopy import Nominatim

no_image = 'https://upload.wikimedia.org/wikipedia/commons/thumb/0/0a/No-image-available.png/480px-No-image-available.png'

current_dir = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.join(current_dir, '.env')
load_dotenv(env_path)


def get_wikipedia_page(url):
    print("Getting wikipedia page ...", url)

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()  # check if the req is successful

        return response.text
    except requests.RequestException as e:
        print(f"An error occurred: {e}")


def get_wikipedia_data(html):
    soup = BeautifulSoup(html, 'html.parser')
    # it takes just the first table with that class
    table = soup.find_all(name='table', attrs={'class': 'wikitable'})[1]
    table_rows = table.find_all('tr')

    return table_rows


def clean_text(text):
    text = str(text).strip()
    text = text.replace('&nbsp', '')
    if ' ♦' in text:
        text = text.replace(' ♦', '')
    if text.find('[') != -1:
        text = text.split('[')[0]
    if text.find(' (formerly)') != -1:
        text = text.split(' (formerly)')[0]

    return text.replace('\n', '')


def extract_wikipedia_data(**kwargs):
    url = kwargs['url']
    html = get_wikipedia_page(url)
    rows = get_wikipedia_data(html)

    data = []
    stadiums_names_count = {}

    for i in range(1, len(rows)):
        tds = rows[i].find_all('td')
        stadium_name = clean_text(tds[0].text)
        city = clean_text(tds[4].text).split(',')[0]

        if stadium_name in stadiums_names_count:
            stadiums_names_count[stadium_name] += 1
            unique_stadium_name = f'{stadium_name}, {city}'
        else:
            stadiums_names_count[stadium_name] = 1
            unique_stadium_name = stadium_name

        values = {
            'rank': i,
            'stadium': unique_stadium_name,
            'capacity': clean_text(tds[1].text).replace(',', '').replace('.', ''),
            'region': clean_text(tds[2].text),
            'country': clean_text(tds[3].text),
            'city': city,
            'image': 'https://' + tds[5].find('img').get('src').split('//')[1] if tds[5].find('img') else 'no_image',
            'home_team': clean_text(tds[6].text),
        }
        data.append(values)

    # data_df = pd.DataFrame(data)
    # data_df.to_csv('data/output.csv', index=False)

    json_rows = json.dumps(data)
    kwargs['ti'].xcom_push(key='rows', value=json_rows)

    return 'Ok'


def get_lat_long(country, city):
    geolocator = Nominatim(user_agent='stadiums-de')
    location = geolocator.geocode(f'{city}, {country}')

    if location:
        return location.latitude, location.longitude

    return None


def transform_wikipedia_data(**kwargs):
    data = kwargs['ti'].xcom_pull(key='rows', task_ids='extract_data_from_wikipedia')

    data = json.loads(data)
    stadiums_df = pd.DataFrame(data)

    stadiums_df['location'] = stadiums_df.apply(lambda x: get_lat_long(x['country'], x['stadium']), axis=1)

    # handle the duplicates
    duplicates = stadiums_df[stadiums_df.duplicated(['location'])]
    duplicates['location'] = duplicates.apply(lambda x: get_lat_long(x['country'], x['city']), axis=1)
    stadiums_df.update(duplicates)

    stadiums_df['capacity'] = stadiums_df['capacity'].astype(int)
    stadiums_df['image'] = stadiums_df['image'].apply(lambda x: x if x not in ['no_image', '', None] else no_image)

    # push to xcom
    kwargs['ti'].xcom_push(key='rows', value=stadiums_df.to_json())

    return 'Ok'


def write_wikipedia_data(**kwargs):
    data = kwargs['ti'].xcom_pull(key='rows', task_ids='transform_wikipedia_data')

    data = json.loads(data)
    data = pd.DataFrame(data)

    file_name = ('stadium_cleaned_' + str(datetime.now().date())
                 + '_' + str(datetime.now().time()).replace(':', '_') + '.csv')

    # data.to_csv('data/' + file_name, index=False)

    # abfs (is a protocol)://sa name@ container name
    data.to_csv('abfs://stadiumsdataengineering@stadiumsdataengineering.dfs.core.windows.net/data/' + file_name,
                storage_options={
                    'account_key': os.environ.get('ACCOUNT_KEY')
                }, index=False)
