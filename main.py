import aiohttp
import asyncio
import logging
import sys
from urllib.parse import urlparse
import xml.etree.ElementTree as ET


DUMP = 'dump.xml'

# Зададим параметры логгирования
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter(u'%(levelname)-8s [%(asctime)s]  %(message)s')
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

def get_data_for_checker():
    """
    Функция возвращает URL адреса и адреса доменов для проверки их на доступность.
    Данные из XML дамп файла выбираем по следующему алгоритму:
    - Если в реестровой записи есть URL, выбираем его
    - Если нет URL записи, но есть доменное имя, добавляем к общему списку домен
    - IP адреса в текущей версии не проверяются
    """
    tree = ET.parse('dump_f')
    root = tree.getroot()
    data_for_check = []
    for elem_content in root:
        # Вначале перебираем url записи
        url_elems = elem_content.findall('url')
        for url in url_elems:
            data_for_check.append(url.text)
        # Переходим к обработке доменов только
        # в том случае, если отсутствует url запись
        # if url_elems:
        #     continue
        # domain_elems = elem_content.findall('domain')
        # if domain_elems is not None:
        #     for domain in domain_elems:
        #         scheme = urlparse(domain).scheme
        #         if not scheme:
        #             domain = 'http://' + domain
        #         data_for_check.add(domain)
    return data_for_check

async def fetch(session, url):
    try:
        async with session.get(url) as response:
            logger.debug('Checking {} status {}'.format(url, response.status))
            return response.status
    except Exception as e:
        logger.debug('Can not open {} : {}'.format(url, e))
        return -1

async def fetch_all(session, essiourls, loop):
    done, pending = await asyncio.wait([loop.create_task(fetch(session, url))
                                  for url in urls])
    return done

if __name__ == '__main__':
    loop = asyncio.get_event_loop()

    urls = get_data_for_checker()
    urls = urls[:1000]
    print(len(urls))
    with aiohttp.ClientSession(loop=loop) as session:
        res = loop.run_until_complete(
            fetch_all(session, urls, loop))
    print(len(res))

    # for it in htmls:
    #     print(it.result())