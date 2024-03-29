import time

from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from db_bonds import create_db, create_table_all_bonds, insert_change_into_table
import asyncio
import aiohttp
import datetime
from asyncio import Semaphore

async def fetch_data(link, session, semaphore):
    async with semaphore:
        async with session.get(link) as response:
            soup = BeautifulSoup(await response.text(), 'lxml')
            info = soup.find_all('div', class_='quotes-simple-table__item')

            information_dict = {info[x].text.strip(): info[x + 1].text.strip() for x in range(0, len(info), 2)}

            name_bond = information_dict['Название']

            bond_quotation = information_dict['Котировка облигации, %']  # Котировка облигации
            bond_quotation = float(bond_quotation[:-1]) if bond_quotation[:-1] else None

            bond_yield = information_dict['Доходность*']  # Доходность к погашению
            bond_yield = float(bond_yield[:-1]) if bond_yield[:-1] else None

            coupon_yield_market = information_dict['Текущая доходность купона']  # Доходность купона от рыночной цены
            coupon_yield_market = float(coupon_yield_market[:-1]) if coupon_yield_market[:-1] else None

            coupon_yield_nominal = information_dict['Доходность купона от номинала']  # Доходность купона от номинальной цены
            coupon_yield_nominal = float(coupon_yield_nominal[:-1]) if coupon_yield_nominal[:-1] else None

            coupon_frequency = information_dict['Частота купона, раз в год']  # Частота купона
            coupon_frequency = round(float(coupon_frequency)) if coupon_frequency else None

            repayment_date = information_dict['Дата погашения']  # Дата погашения
            repayment_date = datetime.datetime.strptime(repayment_date, '%d-%m-%Y').date()  # Перевод в дату
            days_to_maturity = (repayment_date - datetime.date.today()).days  # Дней до погашения

            isin = information_dict['ISIN']  # ISIN
            paper_code = information_dict['Код бумаги']  # Код бумаги
            only_for_quals = information_dict['Только для квалов?']  # Только для квалов?

            current_datetime = datetime.datetime.now() + datetime.timedelta(hours=3)
            TIME_DATE = current_datetime.strftime("%d.%m.%Y %H:%M")

            information_bonds = [link, name_bond, bond_quotation, bond_yield,
                                 coupon_yield_market, coupon_yield_nominal,
                                 coupon_frequency, repayment_date, days_to_maturity,
                                 isin, paper_code, only_for_quals, TIME_DATE]
            insert_change_into_table(information_bonds)


async def main():
    semaphore = Semaphore(5)

    ua = UserAgent()
    fake_user = {'user-agent': ua.random}

    urls = [f'https://smart-lab.ru/q/bonds/order_by_val_to_day/desc/page{pagen}/' for pagen in range(1, 19)]
    urls.extend(['https://smart-lab.ru/q/ofz/'])
    urls.extend(['https://smart-lab.ru/q/subfed/'])

    async with aiohttp.ClientSession(headers=fake_user) as session:
        tasks = []
        for url in urls:
            async with session.get(url) as response:
                soup = BeautifulSoup(await response.text(), 'lxml')
                domain = 'https://smart-lab.ru/'
                links = [domain + url.find('a').get('href') for url in soup.find_all('td', class_='trades-table__name')]
                tasks.extend([fetch_data(link, session, semaphore) for link in links])
        await asyncio.gather(*tasks)


if __name__ == '__main__':
    create_db()
    create_table_all_bonds()
    while True:
        asyncio.run(main())

