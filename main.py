import aiohttp
import asyncio
import logging
import sys
from urllib.parse import urlparse
import xml.etree.ElementTree as ET


DUMP = 'dump.xml'
NUM_CONSUMERS = 10

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
    tree = ET.parse(DUMP)
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

async def fetch(q, session):
    while True:
        url = await q.get()
        if url:
            try:
                async with session.get(url, timeout=10) as response:
                    logger.debug('Checking {} status {}'.format(url, response.status))
            except Exception as e:
                logger.debug('Can not open {} : {}'.format(url, e))
        else:
            # None is the signal to stop.
            q.task_done()
            break
    print('consumer ending')
    print('queue size {}'.format(q.qsize()))

async def producer(q, urls):
    print('producer: starting')
    # Add some numbers to the queue to simulate jobs
    for url in urls:
        await q.put(url)

    # Add None entries in the queue
    # to signal the consumers to exit
    print('producer: adding stop signals to the queue')
    for i in range(10):
        await q.put(None)
    print('producer: waiting for queue to empty')
    print('queue size {}'.format(q.qsize()))
    q.join()
    print('producer: ending')

async def main(loop):
    # Create the queue with a fixed size so the producer
    # will block until the consumers pull some items out.
    q = asyncio.Queue(maxsize=NUM_CONSUMERS)

    urls = get_data_for_checker()
    urls = urls[:50]
    print(len(urls))

    with aiohttp.ClientSession(loop=loop) as session:
        # Scheduled the consumer tasks.
        consumers = [
            loop.create_task(fetch(q, session)) for i in range(NUM_CONSUMERS)
            ]

        # Schedule the producer task.
        prod = loop.create_task(producer(q, urls))
        await asyncio.wait(consumers + [prod])
    print('ok')

if __name__ == '__main__':
    loop = asyncio.get_event_loop()

    try:
        res = loop.run_until_complete(main(loop))
    finally:
        loop.close()

