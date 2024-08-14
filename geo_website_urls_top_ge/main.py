"""
Loads website listing pages from top.ge and saves urls and page numbers in a file. 
Example content of a first few lines of generated file:

http://ambebi.ge,1
http://primetime.ge,1
http://myauto.ge,1
http://www.aversi.ge,1
http://www.interpressnews.ge,1
http://ss.ge,1
http://www.kvirispalitra.ge,1


"""

import os
import concurrent.futures
from datetime import datetime

# from concurrent.futures import ThreadPoolExecutor

from requests_html import (
    HTMLSession,
)  # install with "python3 -m pip install requests-html"


# TODO: consider using locks if data is super crucial to be correct,
# this is just a quick script to get some Georgian website urls.
# https://stackoverflow.com/questions/30154903/is-pythons-file-write-atomic


# define configuration
MAX_PAGE_NUM = 250
FILENAME = f"top_ge_links__{datetime.now().isoformat()}.txt"
MAX_WORKERS = os.cpu_count()


def process_url(url, session, f):
    print(f"processing {url}")

    page_num = int(url.split("/")[-1])

    resp = session.get(url, timeout=20)

    links = [f"{i.attrs['href']},{page_num}" for i in resp.html.find("a.stie_title[href]")]

    f.write("\n".join(links) + "\n")

    print(f"Done {url}")


with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    urls = [
        f"https://top.ge/page/{i}" for i in range(1, MAX_PAGE_NUM + 1)
    ]  # each page has 20 links as of 08/2024
    session = HTMLSession()

    with open(FILENAME, "w") as f:

        future_to_url = {
            executor.submit(process_url, url, session, f): url for url in urls
        }

        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]

            try:
                data = future.result()
            except Exception as ex:
                print(f"{url} generated an exception: {ex}")
