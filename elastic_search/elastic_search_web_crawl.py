import mechanicalsoup as ms
import redis
from elasticsearch import Elasticsearch, helpers
import configparser

def crawl(link, r, browser, es):
    browser.open(link)
    a_tags = browser.page.find_all("a")
    hrefs = [a.get("href") for a in a_tags]
    # print(hrefs)
    wikipedia_domain = "https://en.wikipedia.org"
    links = [wikipedia_domain + a for a in hrefs if a and a.startswith("/wiki/")]
    # create a linked list in redis called "links"
    r.lpush("links", *links)
    es.index(
        index='web-crawl',
        document= {
            'link': link,
            'document': browser.page
        }
    )

r = redis.Redis()
URL = "https://en.wikipedia.org/wiki/Redis"
r.lpush("links", URL)
browser = ms.StatefulBrowser()
config = configparser.ConfigParser()
config.read('example.ini')
es = Elasticsearch(
    cloud_id=config['ELASTIC']['cloud_id'],
    http_auth=(config['ELASTIC']['user'], config['ELASTIC']['password'])
)

count = 0

while link := r.rpop("links"):
    if "jesus" in str(link) or count == 10:
        es.indices.refresh(index='web-crawl')
        break
    crawl(link, r, browser, es)
    count += 1