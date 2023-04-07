import json
import threading
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra.query import dict_factory
from flask import Flask, request, jsonify
from kafka import KafkaConsumer

# Configuration d'Apache Kafka
KAFKA_SERVERS = ['localhost:9092']
KAFKA_TOPIC = 'rss_feeds'

# Configuration d'Apache Cassandra
CASSANDRA_SERVERS = ['localhost']
CASSANDRA_KEYSPACE = 'rss_aggregator'

app = Flask(__name__)

def create_keyspace():
    cluster = Cluster(CASSANDRA_SERVERS)
    session = cluster.connect()

    existing_keyspaces = session.execute("SELECT keyspace_name FROM system_schema.keyspaces;")
    if CASSANDRA_KEYSPACE not in [row[0] for row in existing_keyspaces]:
        session.execute("""
        CREATE KEYSPACE rss_aggregator
        WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
        """)
        print("Keyspace 'rss_aggregator' créé.")
    else:
        print("Keyspace 'rss_aggregator' existe déjà.")

    session.shutdown()
    cluster.shutdown()

create_keyspace()

# Connexion à Cassandra
cluster = Cluster(CASSANDRA_SERVERS)
session = cluster.connect(CASSANDRA_KEYSPACE)
session.row_factory = dict_factory

# Créez la table 'articles' si elle n'existe pas
session.execute("""
CREATE TABLE IF NOT EXISTS articles (
    feed_id text,
    article_id text,
    title text,
    pubDate text,
    description text,
    link text,
    PRIMARY KEY (feed_id, article_id)
)
""")

def consume_kafka_messages():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_SERVERS,
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='rss_aggregator_group',
        max_poll_records=1000
    )

    messages = consumer.poll(timeout_ms=1000)
    for tp, msgs in messages.items():
        for message in msgs:
            article = message.value
            save_article(article)

    consumer.close()

def save_article(article):
    query = SimpleStatement("""
    INSERT INTO articles (feed_id, article_id, title, pubDate, description, link)
    VALUES (%(feed_id)s, %(article_id)s, %(title)s, %(pubDate)s, %(description)s, %(link)s)
    """)
    session.execute(query, article)

consume_kafka_messages()

@app.route('/articles', methods=['POST'])
def consume_and_save_articles():
    consume_kafka_messages()
    return '', 204

@app.route('/articles', methods=['GET'])
def get_last_articles():
    user_id = request.args.get('user_id')  # Récupérer l'argument user_id de la requête
    query = "SELECT * FROM articles"
    rows = session.execute(query)
    articles = [row for row in rows]

    # Trier les articles par date de publication décroissante et retourner les 10 derniers
    sorted_articles = sorted(articles, key=lambda x: x['pubdate'], reverse=True)
    return jsonify(sorted_articles[:10])


@app.route('/articles/<article_id>', methods=['GET'])
def get_article_details(article_id):
    query = "SELECT * FROM articles WHERE article_id = %s ALLOW FILTERING"
    params = (article_id,)

    row = session.execute(query, params).one()
    if row:
        article = {
            "feed_id": row['feed_id'],
            "article_id": row['article_id'],
            "title": row['title'],
            "pubDate": row['pubdate'],
            "description": row['description'],
            "link": row['link']
        }
        return jsonify(article)
    else:
        return "Article not found", 404

if __name__ == '__main__':

    # Démarrer l'application Flask
    app.run(debug=True)
