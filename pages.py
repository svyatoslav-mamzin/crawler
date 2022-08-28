import re
import os
import io
import logging
import asyncio
import async_timeout
import aiohttp

from bs4 import BeautifulSoup

INIT_URL = 'https://news.ycombinator.com/'
ROOT_FOLDER = 'haker_news'
IGNORE = ['.pdf', '.jpg']
FETCH_TIMEOUT = 10
FILENAME_LIMIT = 80


def clean_filename(filename):
    filename = re.sub(r"[^\w\s]", '_', filename)
    filename = re.sub(r"\s+", '-', filename)
    filename = filename.replace('__', '_')
    return filename[:FILENAME_LIMIT]


def is_url_ignored(url):
    for ignore_suffix in IGNORE:
        if url.endswith(ignore_suffix):
            return True
    return False


def is_url_valid(url):
    if url.startswith('http://') or url.startswith('https://'):
        return True
    else:
        return False


async def simple_fetch(url, session):
    async with session.get(url) as response:
        try:
            return await response.text()
        except UnicodeDecodeError:
            logging.info(f'ERROR: UnicodeDecodeError in {url}')
            return ''


def save_page(html, filename: str, folder: str):
    filename = filename + '.html'
    filename = os.path.join(folder, filename)
    with open(filename, 'w') as f:
        f.write(html)
    logging.info(f'SAVED: {filename}')


def get_title(html):
    bsObj = BeautifulSoup(html, "html.parser")
    title = bsObj.title.text if bsObj.title else None
    return title


def save(url, html, page_folder):
    logging.info(f'LOAD TITLE FROM COMMENT {url}')
    if not html:
        return
    title = get_title(html)
    if not title:
        return
    sub_filename = clean_filename(title)
    logging.info(f'SAVE COMMENT {page_folder} / {sub_filename}')
    save_page(html, sub_filename, folder=page_folder)


def get_filename(comments, title):
    uid = comments.split('=')[1]
    filename = clean_filename(uid + '_' + title)
    return filename


def is_downloaded(folder):
    return os.path.exists(folder)


class Page:
    def __init__(self, url):
        self.url = url

    async def get(self) -> str:
        html = ''
        if is_url_ignored(self.url):
            return html
        try:
            with async_timeout.timeout(FETCH_TIMEOUT):
                async with aiohttp.ClientSession() as session:
                    html = await self._fetch(session)
        except aiohttp.client_exceptions.ClientConnectorError:
            logging.warning(f'Connection Error with {self.url}')
        except asyncio.TimeoutError:
            logging.warning(f'Timeout Error with {self.url}')
        except aiohttp.client_exceptions.ServerDisconnectedError:
            logging.warning(f'Server Disconnected Error with {self.url}')
        except aiohttp.client_exceptions.TooManyRedirects:
            logging.warning(f'Too many redirects on {self.url}')
        return html

    async def _fetch(self, session):
        f = io.BytesIO()
        try:
            async with session.get(self.url) as response:
                    while True:
                        chunk = await response.content.read(1024)
                        if not chunk:
                            break
                        f.write(chunk)
            return f.getvalue().decode()
        except UnicodeDecodeError:
            logging.info(f'ERROR: UnicodeDecodeError in {self.url}')
            return ''


class Post(Page):
    def __init__(self, post_url, post_title, comments_url):
        self.url = post_url
        self.title = post_title
        self.comments = comments_url
        self.filename = get_filename(self.comments, self.title)
        self.folder = os.path.join(ROOT_FOLDER, self.filename)
        self.html = ''

    async def get_all_urls_from_comment_page(self) -> list:
        urls = []
        comments_page = Page(self.comments)
        html = await comments_page.get()
        bsObj = BeautifulSoup(html, "html.parser")
        comments_raw = bsObj.find_all("span", attrs={"class": "c00"})
        for comment in comments_raw:
            if not comment.a:
                continue
            href = comment.a["href"]
            if 'reply' in href:
                continue
            urls.append(href)
        return urls


def parse_main_page(html: str) -> list:
    """ return list of
        (post_url, post_title, comments_on_post_url)
    """
    top_posts = []
    bsObj = BeautifulSoup(html, "html.parser")
    top_news = bsObj.find_all("a", attrs={"class": "titlelink"})
    comment_urls = bsObj.find_all("td", attrs={"class": "subtext"})
    comment_urls = [INIT_URL + comment.find_all("a")[-1]["href"] for comment in comment_urls]
    if len(top_news) != len(comment_urls):
        raise ValueError('Len top news list and len comments list is not equal')
    for post, comment_url in zip(top_news, comment_urls):
        post_url = post['href']
        if post_url.startswith('item'):
            post_url = INIT_URL + post_url
        top_posts.append((post_url, post.text, comment_url))
    return top_posts
