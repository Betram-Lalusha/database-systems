import mechanicalsoup as ms
import redis
from elasticsearch import Elasticsearch, helpers
import configparser

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

def write_to_elastic_search(es, url, html):
    url = url.decode('utf-8')
    es.index (index= 'webpages',document = {'url': url,'html': html })

def crawl(link, r, browser, es):
    browser.open(link)

    write_to_elastic_search(es, link, str(browser.page))
    a_tags = browser.page.find_all("a")
    hrefs = [a.get("href") for a in a_tags]
    # print(hrefs)
    wikipedia_domain = "https://en.wikipedia.org"
    links = [wikipedia_domain + a for a in hrefs if a and a.startswith("/wiki/")]
    # create a linked list in redis called "links"
    r.lpush("links", *links)

count = 0

while link := r.rpop("links"):
    if "jesus" in str(link) or count == 10:
        es.indices.refresh(index='webpages')
        break
    crawl(link, r, browser, es)
    count += 1

result = es.search (
    index = 'webpages',
    query= {
        'match': {'html': 'html'}
    }
)

# print(result)