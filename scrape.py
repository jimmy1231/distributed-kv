from bs4 import BeautifulSoup
from bs4.element import Comment
import urllib.request
import uuid

def tag_visible(element):
    if element.parent.name in ['style', 'script', 'head', 'title', 'meta', '[document]']:
        return False
    if isinstance(element, Comment):
        return False
    return True

def text_from_html(body):
    soup = BeautifulSoup(body, 'html.parser')
    texts = soup.findAll(text=True)
    visible_texts = filter(tag_visible, texts)
    return u" ".join(t.strip() for t in visible_texts)

def scrape(urls):
    for URL in urls:
        text = text_from_html(urllib.request.urlopen(URL))
        fname = "files/" + str(uuid.uuid4())
        f = open(fname, "a")
        f.write(text)
        f.close()

if __name__ == "__main__":
    scrape(["https://en.wikipedia.org/wiki/British_Isles"]);