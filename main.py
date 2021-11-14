import pickle
import datetime
import requests
from urlextract import URLExtract
import datetime
import time
import threading
from loguru import logger
from urllib.parse import urlparse

start = datetime.datetime.now()
logger.add("debug.log",
           format='{time} {level} {message}',
           level="DEBUG",
           rotation="5 min",
           retention='20 min',
           compression="zip"
           )

with open("messages_to_parse.dat", 'rb') as file:
    content = pickle.load(file)
joined_content = " ".join(content)

extractor = URLExtract()
parsed_urls = extractor.find_urls(joined_content)

tested_urls = {}
unshorten_urls = {}


def ping_urls(url):
    if url[0] == '.':
        url = 'http://www'+ url
    try:

        x = requests.head(url, timeout=2)
        url_dict = {}
        url_dict[url] = x.status_code
    except Exception:
        url_dict = {}
        url_dict[url] = 404
        logger.debug(f'Exception handling while requesting urls {url}')
    else:
        if x.status_code // 100 == 3:
            location = x.headers['location']
            url_parsed = urlparse(url)[1]
            location_parsed = urlparse(location)[1]

            if len(location_parsed)-len(url_parsed) == 4:
                location_parsed = location_parsed[4:]
            if location_parsed != url_parsed:
                unshorten_urls[url] = location
    finally:
        tested_urls.update(url_dict)



threads = []
for url in parsed_urls:
    thread = threading.Thread(target=ping_urls, args=(url,))
    threads.append(thread)
    thread.start()
for thread in threads:
    thread.join()
time.sleep(1)



print(tested_urls)
print(len(tested_urls))
print("------------")
print(unshorten_urls)
print(len(unshorten_urls))

end2 = datetime.datetime.now()
takes_time2 = end2-start
print(takes_time2.seconds)

with open('readme.txt', 'a') as f:
    f.writelines([f"\nDate  {end2.date()}.",
                  f"\nTime of project execution  {takes_time2.seconds}s.",
                  f"\nDict length art3 {len(tested_urls)}.",
                  f"\nDict length art3  {len(unshorten_urls)}."])

