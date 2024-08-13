import requests
import time
import json
from bs4 import BeautifulSoup
from datetime import datetime
api_key = 'INSERT YOUR KEY'
url = f'https://newsapi.org/v2/top-headlines?country=in&apiKey={api_key}'
news_data = []
def get_from_news_api():
    page = 1
    all_articles = []

    while True:
        response = requests.get(f'{url}&page={page}')
        if response.status_code != 200:
                print(f"Failed to retrieve the page. Status code: {response.status_code}")
                break
        news = response.json()  

        if news['articles']:
            all_articles.extend(news['articles'])

            
            if len(news['articles']) < 20:
                break

            page += 1  
        else:
            break

    print(f"Total articles retrieved: {len(all_articles)}")
    
    
    for article in all_articles:
        newsObject = {
            '_id': article["publishedAt"],
            'text': article["title"],
            'source': "newsapi"
        }
        news_data.append(newsObject)

    with open('news_articles.json', 'w', encoding='utf-8') as f:
        json.dump(news_data, f, ensure_ascii=False, indent=4)



def remove_milliseconds(datetime_str):
    dt = datetime.strptime(datetime_str, "%Y-%m-%dT%H:%M:%S.%fZ")
    
    return dt.strftime("%Y-%m-%dT%H:%M:%S") + "Z"


def get_from_inshorts(cat):
    url='https://www.inshorts.com/en/read/'
    
    body=requests.get(url+cat)
    if body.status_code != 200:
        print(f"Failed to retrieve the web page. Status code: {body.status_code}")
        
    else:
        soup=BeautifulSoup(body.text,'html.parser')
        headlines = soup.find_all('span', itemprop="headline")
        date_published = soup.find_all('span', itemprop="datePublished")

        for i in range(len(headlines)):
            content=headlines[i].text
            time=remove_milliseconds(date_published[i].get('content', 'No content date found'))
            if not any(news['_id'] == time for news in news_data):
                newsObject = {
                    '_id': time,
                    'text': content,
                    'source': "inshorts"
                }
                
                news_data.append(newsObject)
            with open('news_articles.json', 'w', encoding='utf-8') as f:
                json.dump(news_data, f, ensure_ascii=False, indent=4)
        


def inshorts_categories():
    categories=["national","business","sports","world","politics","technology","startup","entertainment","miscellaneous","hatke","science","automobile"]
    for i in categories:
        get_from_inshorts(i)

def indianewslive():
    url = "https://real-time-news-data.p.rapidapi.com/top-headlines"

    querystring = {"limit":"50","country":"IN","lang":"en"}

    headers = {
        "x-rapidapi-key": "INSERT YOUR KEY",
        "x-rapidapi-host": "real-time-news-data.p.rapidapi.com"
    }

    response = requests.get(url, headers=headers, params=querystring)
    if response.status_code != 200:
            print(f"Failed to retrieve the page. Status code: {response.status_code}")
    else:
        response=response.json()
        for response in response["data"]:
            time=response['published_datetime_utc']
            time=remove_milliseconds(time)
            content=response['title']
            newsObject = {
                '_id':time ,  
                'text':content ,                  
                'source': "websearch"                       
            }

            news_data.append(newsObject)
            with open('news_articles.json', 'w', encoding='utf-8') as f:
                json.dump(news_data, f, ensure_ascii=False, indent=4)


while True:
    get_from_news_api()
    inshorts_categories()
    indianewslive()
    print("Dumped!")
    time.sleep(1800) 
