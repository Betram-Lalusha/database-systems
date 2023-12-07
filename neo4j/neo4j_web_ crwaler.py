import mechanicalsoup as ms
import redis
from elasticsearch import Elasticsearch, helpers
import configparser
from neo4j import GraphDatabase
from dotenv import load_dotenv
import os

r = redis.Redis()
URL = "https://en.wikipedia.org/wiki/Redis"
r.lpush("links", URL)
browser = ms.StatefulBrowser()
config = configparser.ConfigParser()
config.read('example.ini')
es = Elasticsearch(
    cloud_id=config['ELASTIC']['cloud_id'],
    basic_auth=(config['ELASTIC']['user'], config['ELASTIC']['password'])
)

class Neo4JConnector:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def add_links(self, page, links):
        with self.driver.session() as session:
           session.execute_write(self._create_links, page, links)

    def flush_db(self):
        with self.driver.session() as session:
           session.execute_write(self._flush_db)

    def write_1(self):
        with self.driver.session() as session:
           session.execute_write(self._write_1)
    
    @staticmethod
    def _create_links(tx, page, links):
        page = page.decode('utf-8')
        print("page ", page)
        tx.run("CREATE (:Page {url: $page })", page=page)
        for link in links:
            print(link)
            tx.run("MATCH (p:Page) WHERE p.url = $page "
              "CREATE (:Page {url: $link}) -[l:links_to]-> (p)", link=str(link), page=page)
            # tx.run("CREATE (a:Page {url: $link}) -[l:links_to]-> (:Page {url: $page})",link=str(link), page=page.decode('utf-8'))

    @staticmethod
    def _flush_db(tx):
        tx.run("MATCH (a) -[r]-> () DELETE a, r")
        tx.run("MATCH (a) DELETE a")

    @staticmethod
    def _write_1(tx):
        tx.run("CREATE (:Page)")

load_dotenv()
neo4j_connector = Neo4JConnector("bolt://127.0.0.1:7689","neo4j", os.getenv('neo4j_password'))
neo4j_connector.flush_db()
# neo4j_connector.write_1()
# exit()

def write_to_elastic_search(es, url, html):
    url = url.decode('utf-8')
    es.index (index= 'webpages',document = {'url': url,'html': html })

def crawl(link, r, browser, es, connector):
    browser.open(link)

    write_to_elastic_search(es, link, str(browser.page))
    a_tags = browser.page.find_all("a")
    hrefs = [a.get("href") for a in a_tags]
    # print(hrefs)
    wikipedia_domain = "https://en.wikipedia.org"
    links = [wikipedia_domain + a for a in hrefs if a and a.startswith("/wiki/")]
    # create a linked list in redis called "links"
    r.lpush("links", *links)
    connector.add_links(link, links)

count = 0

while link := r.rpop("links"):
    if "jesus" in str(link) or count == 10:
        es.indices.refresh(index='webpages')
        break
    crawl(link, r, browser, es, neo4j_connector)
    count += 1

result = es.search (
    index = 'webpages',
    query= {
        'match': {'html': 'html'}
    }
)

# print(result)