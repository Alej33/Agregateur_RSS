import os
import json
import subprocess
from time import sleep
from kafka import KafkaProducer
import xml.etree.ElementTree as ET
from hashlib import blake2b
import random
import requests

# Configuration d'Apache Kafka
KAFKA_SERVERS = ['localhost:9092']
KAFKA_TOPIC = 'rss_feeds'

# Liste des flux RSS à scraper
RSS_FEEDS = [
    "https://www.lemonde.fr/rss/une.xml",
   "https://www.numerama.com/feed/"
]

#Générateur de clé
CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"
def key_generator(data, size=32):
    h = blake2b(digest_size=8)
    h.update(data.encode("utf-8"))
    hash = int(h.hexdigest(), 16)
    rand = random.Random(hash)
    result = [CHARS[rand.randint(0, len(CHARS) - 1)] for i in range(0, size)]
    return "".join(result)

# Configuration du producteur Kafka
producer = KafkaProducer(bootstrap_servers=KAFKA_SERVERS, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def fetch_rss_feed(url):
    curl_command = f"curl -s {url}"
    output = subprocess.check_output(curl_command, shell=True).decode("utf-8")
    return output

def parse_rss_feed(output, feed):
    root = ET.fromstring(output)
    
    feed_id = key_generator(feed)
    articles = []
    for item in root.findall(".//item"):
        article = {
            "feed_id": feed_id,
            "article_id": key_generator(item.find("link").text),
            "title": item.find("title").text,
            "pubDate": item.find("pubDate").text,
            "description": item.find("description").text,
            "link": item.find("link").text
        }
        articles.append(article)
    return articles

def send_articles_to_kafka(articles):
    for article in articles:
        key = article["feed_id"].encode('utf-8')
        producer.send(KAFKA_TOPIC, key=key, value=article)

FLASK_SERVER_URL = "http://127.0.0.1:5000/articles"

def send_articles_to_flask(articles):
    send_articles_to_kafka(articles)
    response = requests.post(FLASK_SERVER_URL)
    if response.status_code == 204:
        print("Les articles ont été envoyés avec succès à l'application Flask.")
    else:
        print(f"Échec de l'envoi des articles à l'application Flask (Code: {response.status_code})")

def main():
    while True:
        for rss_feed_url in RSS_FEEDS:
            rss_output = fetch_rss_feed(rss_feed_url)
            articles = parse_rss_feed(rss_output, rss_feed_url)
            #print(articles)
            send_articles_to_flask(articles)
        sleep(20)  # Attendez 20 secondes avant de consulter à nouveau les flux RSS

if __name__ == "__main__":
    main()
