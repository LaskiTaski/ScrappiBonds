import asyncio
import aiohttp
import datetime
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from time import sleep

from data_validation import safe_int, safe_float, safe_date, calculate_days_to_maturity
from models import create_tables
from db_manager import insert_change_table

x = 0
def generic_urls():
    urls = [f'https://smart-lab.ru/q/bonds/order_by_val_to_day/desc/page{pagen}/' for pagen in range(1, 19)]
    urls.extend(['https://smart-lab.ru/q/ofz/'])
    urls.extend(['https://smart-lab.ru/q/subfed/'])
    return urls


def parse_bond_data(soup, url):
    try:
        info = {
            item.text.strip(): item.find_next_sibling().text.strip()
            for item in soup.select('.quotes-simple-table__item') if item.find_next_sibling()
        }

        return {
            "url": url,
            "name": info.get("Название", "N/A"),
            "quoting": safe_float(info.get("Котировка облигации, %")),
            "repayment": safe_float(info.get("Доходность*")),
            "market": safe_float(info.get("Текущ. дох. купона")),
            "nominal": safe_float(info.get("Ставка купона")),
            "frequency": safe_int(info.get("Частота купона, раз в год")),
            "date": safe_date(info.get("Дата погашения")),
            "days": calculate_days_to_maturity(safe_date(info.get("Дата погашения"))),
            "isin": info.get("ISIN"),
            "code": info.get("Код бумаги"),
            "qualification": info.get("Только для квалов?"),
            "update_time": datetime.datetime.now().strftime("%d.%m.%Y %H:%M")
        }

    except Exception as e:
        print(f"Ошибка при анализе данных из {url}: {e}")
        return None


async def fetch_data(link, session, semaphore):
    global x
    x += 1
    print(x)
    async with semaphore:
        try:
            async with session.get(link) as response:
                response.raise_for_status()
                soup = BeautifulSoup(await response.text(), 'lxml')
                bond_data = parse_bond_data(soup, link)
                if bond_data:
                    insert_change_table(bond_data)

        except aiohttp.ClientError as e:
            print(f"Получена ошибка {link}: {e}")

        except Exception as e:
            print(f"Получена неожиданная ошибка {link}: {e}")


async def main():
    semaphore = asyncio.Semaphore(5)
    ua = UserAgent()
    fake_user = {'user-agent': ua.random}
    urls = generic_urls()
    domain = 'https://smart-lab.ru/'

    async with aiohttp.ClientSession(headers=fake_user) as session:
        tasks = []
        for url in urls:
            try:
                async with session.get(url) as response:
                    response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
                    soup = BeautifulSoup(await response.text(), 'lxml')
                    links = [domain + a.get('href') for a in soup.select('.trades-table__name a')]
                    tasks.extend([fetch_data(link, session, semaphore) for link in links[1::]])
            except aiohttp.ClientError as e:
                print(f"Ошибка при работе с main функцией {url}: {e}")
            except Exception as e:
                print(f"Неожиданная ошибка при работе с main функцией {url}: {e}")
        await asyncio.gather(*tasks)


if __name__ == '__main__':
    create_tables()
    while True:
        asyncio.run(main())
        sleep(600)
