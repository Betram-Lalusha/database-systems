import mechanicalsoup as ms
import redis

def crawl(link, r, browser):
    browser.open(link)
    a_tags = browser.page.find_all("a")
    hrefs = [a.get("href") for a in a_tags]
    # print(hrefs)
    wikipedia_domain = "https://en.wikipedia.org"
    links = [wikipedia_domain + a for a in hrefs if a and a.startswith("/wiki/")]
    # create a linked list in redis called "links"
    r.lpush("links", *links)

r = redis.Redis()
URL = "https://en.wikipedia.org/wiki/Redis"
r.lpush("links", URL)
browser = ms.StatefulBrowser()

while link := r.rpop("links"):
    if "jesus" in str(link):
        break
    crawl(link, r, browser)