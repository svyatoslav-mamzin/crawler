import asyncio
import os
import logging

from pages import Page, Post, parse_main_page, is_downloaded, is_url_valid, save_page, save
from pages import INIT_URL, ROOT_FOLDER

LOGGING_FORMAT = '[%(asctime)s] %(levelname).1s %(message)s'
LOGGING_LEVEL = logging.INFO

CHECK_NEW_TIMEOUT = 30

headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
                      ' (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'keep-alive'
    }


async def check_for_new_posts(queue: asyncio.Queue):
    while True:
        logging.info(f'CHECK NEWS: MAIN PAGE ')
        main_page = Page(INIT_URL)
        logging.info(f'CHECK NEWS: MAIN PAGE LOADING {main_page.url}')
        html = await main_page.get()
        logging.info(f'CHECK NEWS: GET POSTS ')
        top_posts_data = parse_main_page(html)
        top_posts = [Post(*post_data) for post_data in top_posts_data]
        posts_to_parse = [post for post in top_posts
                          if not is_downloaded(post.folder) and is_url_valid(post.url)]
        logging.info(f'CHECK NEWS: PUTTING NEW POSTS TO QUEUE')
        for post in posts_to_parse:
            queue.put_nowait(post)
        logging.info(f'CHECK NEWS: SLEEP')
        await asyncio.sleep(CHECK_NEW_TIMEOUT)


async def post_worker(post_queue: asyncio.Queue, comments_queue: asyncio.Queue):
    while True:
        logging.info(f'POST WORKER: EXPECTNG POST ')
        post = await post_queue.get()
        logging.info(f'POST WORKER: GET POST {post.url} ')
        html = await post.get()
        logging.info(f'POST WORKER: GET POST COMMENTS URLS {post.url} ')
        comments_urls = await post.get_all_urls_from_comment_page()
        logging.info(f'POST WORKER: SAVE POST {post.url} ')
        os.makedirs(post.folder, exist_ok=True)
        save_page(html, post.filename, post.folder)
        logging.info(f'POST WORKER: PUT COMMENTS TO QUEUE: {len(comments_urls)} {post.url} ')
        for url in comments_urls:
            comments_queue.put_nowait((url, post.folder,))
        logging.info(f'POST WORKER: TASK DONE {post.url} ')
        post_queue.task_done()


async def comment_worker(comments_queue: asyncio.Queue):
    while True:
        logging.info(f'COMMENT WORKER: EXPECTING COMMENT ')
        url, folder = await comments_queue.get()
        page = Page(url=url)
        logging.info(f'COMMENT WORKER: LOAD COMMENT {page.url} ')
        html = await page.get()
        logging.info(f'COMMENT WORKER: SAVE COMMENT {page.url} ')
        save(url, html, folder)
        logging.info(f'COMMENT WORKER: TASK DONE {page.url} ')
        comments_queue.task_done()


async def run_forever(post_queue, comments_queue):
    tasks = [asyncio.ensure_future(check_for_new_posts(post_queue)),
             asyncio.ensure_future(post_worker(post_queue, comments_queue)),
             asyncio.ensure_future(comment_worker(comments_queue))
             ]
    await asyncio.wait(tasks)
    post_queue.join()
    comments_queue.join()


if __name__ == '__main__':
    logging.basicConfig(format=LOGGING_FORMAT, datefmt='%Y.%m.%d %H:%M:%S', level=LOGGING_LEVEL)
    os.makedirs(ROOT_FOLDER, exist_ok=True)
    ioloop = asyncio.get_event_loop()
    post_queue = asyncio.Queue()
    comments_queue = asyncio.Queue()
    try:
        logging.info('run parse all coro')
        ioloop.create_task(run_forever(post_queue, comments_queue))
        ioloop.run_forever()
        logging.info('exit from async coroutines')
    except KeyboardInterrupt:
        logging.info('KeyboardInterrupt, exiting')
    except Exception as e:
        logging.exception(f"Unexpected exception: {e}")
