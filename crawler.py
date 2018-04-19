import os
import re
import sys
import json
import random
import asyncio
from asyncio import Queue
import logging
import concurrent

import aiohttp
import async_timeout
import redis
from lxml import etree


class Crawler(object):
    def __init__(self, max_tasks=100, store_path='.', *, loop=None):
        self.loop = loop or asyncio.get_event_loop()
        self.max_tasks = max_tasks
        self.store_path = store_path
        self.session = aiohttp.ClientSession(loop=self.loop)
        self.headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36',
        }
        self.redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        self.start_page_key = 'start_page'
        self.detail_page_key = 'detail_page'
        self.download_page_key = 'download_page'
        self.start_coroutine_count = self.max_tasks // 20
        self.detail_coroutine_count = self.max_tasks // 5
        self.download_coroutine_count = self.max_tasks - self.start_coroutine_count - self.detail_coroutine_count
        self.download_semaphore = asyncio.Semaphore(self.download_coroutine_count)
        self.semaphore = asyncio.Semaphore(self.start_coroutine_count+self.detail_coroutine_count)
        self.handle_failed_semaphore = asyncio.Semaphore(10)
        self.q = Queue(loop=self.loop)

    async def start_task(self):
        while True:
            await self.create_task(self.start_page_key, self.parse_detail_task)

    async def detail_task(self):
        while True:
            await self.create_task(self.detail_page_key, self.parse_download_task)

    async def download_task(self):
        while True:
            async with self.download_semaphore:
                task, json_data = await self.get_task(self.download_page_key)
                if task is None:
                    await asyncio.sleep(10)
                    continue
                content = await self.fetch(json_data['url'], self.download_page_key, task, type_='content')
                if content is not None:
                    self.loop.run_in_executor(None, self.save_image, json_data['path'], content)

    async def handle_failed_task(self):
        while True:
            async with self.handle_failed_semaphore:
                redis_key, task = await self.q.get()
                self.loop.run_in_executor(None, self.insert_task, redis_key, task)
                logging.info('handle failed task: {}'.format(task))
                self.q.task_done()

    def get_proxy(self):
        try:
            proxy = random.choice(self.redis_client.keys('http://*'))
        except:
            return None
        return proxy

    async def close(self):
        await self.session.close()

    async def get_task(self, redis_key):
        task = self.redis_client.spop(redis_key)
        return task, (task is not None) and json.loads(task)

    async def parse_detail_task(self, text, json_data):
        selector = etree.HTML(text)
        for sel in selector.xpath('//*[@class="gallery_image"]'):
            xpath_ = './/img[@class="img-responsive img-rounded"]/@src'
            category = re.findall('ua/(.*?)/page', json_data['url'])[0]
            image_dir, image_number = re.findall('/mini/(\d+)/(\d+)\.jpg', sel.xpath(xpath_)[0])[0]
            meta = {
                'url': sel.xpath('./@href')[0],
                'image_number': image_number,
                'image_dir': image_dir,
                'category': category,
            }
            self.loop.run_in_executor(None, self.insert_task, self.detail_page_key, json.dumps(meta))

    async def parse_download_task(self, text, json_data):
        base_url = 'https://look.com.ua/pic'
        selector = etree.HTML(text)
        for url in selector.xpath('//*[@class="llink list-inline"]/li/a/@href'):
            resolution = re.findall(r'download/\d+/(\d+x\d+)/', url)[0]
            path = os.path.join(os.path.abspath(self.store_path), 'images', json_data['category'],
                                json_data['image_number'], resolution + '.jpg')
            url = '/'.join([base_url, json_data['image_dir'], resolution,
                            'look.com.ua-' + json_data['image_number'] + '.jpg'])
            if os.path.exists(path):
                logging.info('image {} already downloaded'.format(path))
                continue
            meta = {'url': url, 'path': path, }
            self.loop.run_in_executor(None, self.insert_task, self.download_page_key, json.dumps(meta))

    async def create_task(self, redis_key, operate_func):
        async with self.semaphore:
            task, json_data = await self.get_task(redis_key)
            if task is None:
                await asyncio.sleep(10)
            else:
                url = json_data['url']
                html = await self.fetch(url, redis_key, task)
                if html is not None:
                    await operate_func(html, json_data)

    def insert_task(self, redis_key, task):
        self.redis_client.sadd(redis_key, task)

    async def fetch(self, url, key, value, type_='text'):
        logging.info('active tasks count: {}'.format(len(asyncio.Task.all_tasks())))
        try:
            async with async_timeout.timeout(30):
                async with self.session.get(url, headers=self.headers, ssl=False, timeout=30, allow_redirects=False,
                                            proxy=self.get_proxy()) as response:
                    return await response.read() if type_ == 'content' else await response.text()
        except Exception as err:
            if isinstance(err, concurrent.futures._base.TimeoutError):
                logging.warning('{} raised TimeoutError'.format(url))
            else:
                logging.warning('{} raised {}'.format(url, str(err)))
            self.loop.run_in_executor(None, self.insert_task, key, value)
            return None

    def save_image(self, path, content):
        if not os.path.exists('\\'.join(path.split('\\')[:-1])):
            os.makedirs('\\'.join(path.split('\\')[:-1]))
        with open(path, 'wb') as f:
            f.write(content)
        logging.info('{}: downloaded'.format(path))

    async def crawl(self):
        """Run the crawler until all finished."""
        workers = []
        workers.extend([asyncio.Task(self.start_task(), loop=self.loop) for _ in range(self.start_coroutine_count)])
        workers.extend([asyncio.Task(self.detail_task(), loop=self.loop) for _ in range(self.detail_coroutine_count)])
        workers.extend([asyncio.Task(self.download_task(), loop=self.loop) for _ in range(self.download_coroutine_count)])
        # asyncio.Task(self.start_task(), loop=self.loop)
        # asyncio.Task(self.detail_task(), loop=self.loop)
        # asyncio.Task(self.download_task(), loop=self.loop)
        while True:
            await asyncio.sleep(60)
        # for w in workers:
        #     w.cancel()


def main():
    pass


if __name__ == '__main__':
    main()
