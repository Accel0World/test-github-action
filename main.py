from datetime import datetime
from lxml import html
import pandas as pd
import aiohttp
import asyncio
import json

# The Constant Variables:
MAIN_URL = "https://books.toscrape.com/catalogue/page-{}.html"
connector = aiohttp.TCPConnector(limit=100, limit_per_host=0, ssl=False)


# The MAin function:


async def send_req(session: aiohttp.ClientSession, main_url: str, page_number: int):

    async with session.get(main_url.format(page_number)) as response:
        if response.status != 200:
            response.raise_for_status()
        return await response.text()


async def fetch_all(fetch, session: aiohttp.ClientSession, main_url: str, num_pages: int):
    tasks = []
    for page_number in range(1, num_pages + 1):
        task = asyncio.create_task(fetch(session, main_url, page_number))
        tasks.append(task)

    responses = await asyncio.gather(*tasks)
    return responses


async def main_runner(fetch, main_url: str, num_pages: int):
    async with aiohttp.ClientSession(headers=headers, connector=connector) as session:
        data = await fetch_all(fetch, session, main_url, num_pages)
    return data


def parse_html(page):

    tree = html.fromstring(page)
    title = tree.cssselect('article.product_pod > h3 > a')
    price = tree.cssselect('.product_price > p.price_color')

    price_list = [t.text.strip() for t in price]
    title_list = [t.text.strip() for t in title]

    item_list = []
    for i in range(len(price_list)):
        dict_ = {
            "Product Title": title_list[i],
            "Product Price": price_list[i]
        }
        item_list.append(dict_)
    return item_list


def return_headers(filename):
    with open(filename, 'r') as file:
        data = json.load(file)
        return data


def build_file_name():
    date_time = datetime.now().strftime("(%d-%m-%Y at %H-%M)")
    return f"books_{date_time}.xlsx"


def write_data_to_excel(file_name, list_):
    df = pd.DataFrame(list_)
    df.to_excel(file_name, index=True, index_label="Entry Index")


if __name__ == "__main__":

    headers = return_headers("headers.json")
    master_list = []

    loop = asyncio.get_event_loop()
    results = loop.run_until_complete(main_runner(
        send_req, MAIN_URL, 50))

    for index, page in enumerate(results):
        try:
            master_list.extend(parse_html(page))
        except:
            print(f"An error has occurred while parsing page {index + 1}")
    write_data_to_excel(build_file_name(), master_list)
