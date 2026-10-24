"""Асинхронный веб-краулер.

Модуль собирает все компоненты краулера: загрузку страниц, разбор HTML,
очередь и ограничение конкурентности, rate limiting и robots.txt,
обработку ошибок с повторами и сохранение данных.
"""

import argparse
import asyncio
import copy
import csv
import io
import yaml
import itertools
import json
import logging
import logging.handlers
import random
import re
import textwrap
import time
import xml.etree.ElementTree as ET
from html import escape
from abc import ABC, abstractmethod
from collections import Counter, deque
from contextlib import asynccontextmanager, suppress
from datetime import datetime
from pathlib import Path
from urllib.parse import urldefrag, urljoin, urlparse
from urllib.robotparser import RobotFileParser

import aiofiles
import aiohttp
import aiosqlite
from bs4 import BeautifulSoup

logger = logging.getLogger("crawler")

DEFAULT_CONFIG = {
    "start_urls": [],
    "crawler": {
        "max_concurrent": 10,
        "max_depth": 2,
        "max_pages": 100,
        "rate_limit": None,
        "per_domain_limit": None,
        "respect_robots": False,
        "user_agent": "AsyncCrawler/1.0",
        "min_delay": 0.0,
        "jitter": 0.0,
        "max_retries": 0,
    },
    "filters": {
        "same_domain_only": False,
        "exclude_patterns": [],
        "include_patterns": [],
    },
    "storage": {
        "type": None,
        "path": None,
    },
        "storage": {
        "type": None,
        "path": None,
    },
    "logging": {
        "log_file": "crawler.log",
        "console_level": "INFO",
        "file_level": "DEBUG",
        "max_bytes": 1_000_000,
        "backup_count": 3,
    },
}

__all__ = [
    "CrawlerError",
    "TransientError",
    "PermanentError",
    "NetworkError",
    "ParseError",
    "CircuitOpenError",
    "classify_error",
    "RetryStrategy",
    "CircuitBreaker",
    "HTMLParser",
    "CrawlerQueue",
    "SemaphoreManager",
    "RateLimiter",
    "RobotsParser",
    "DataStorage",
    "JSONStorage",
    "CSVStorage",
    "SQLiteStorage",
    "AsyncCrawler",
    "SitemapParser",
    "CrawlerStats",
    "DEFAULT_CONFIG",
    "load_config",
    "create_storage",
    "setup_logging",
    "AdvancedCrawler",
]


class CrawlerError(Exception):
    def __init__(self, message: str, url: str = "", status: int | None = None):
        super().__init__(message)
        self.url = url
        self.status = status


class TransientError(CrawlerError):
    pass


class PermanentError(CrawlerError):
    pass


class NetworkError(CrawlerError):
    pass


class ParseError(CrawlerError):
    pass


def classify_error(error: Exception, url: str) -> CrawlerError:
    if isinstance(error, CrawlerError):
        return error
    if isinstance(error, aiohttp.ClientResponseError):
        message = f"HTTP {error.status}"
        if error.status == 429 or error.status >= 500:
            return TransientError(message, url, error.status)
        return PermanentError(message, url, error.status)
    if isinstance(error, asyncio.TimeoutError):
        return TransientError(type(error).__name__, url)
    if isinstance(error, aiohttp.ClientError):
        return NetworkError(type(error).__name__, url)
    return CrawlerError(type(error).__name__, url)

class CircuitOpenError(CrawlerError):
    pass


class RetryStrategy:
    def __init__(
        self,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
        retry_on: list = None,
        base_delay: float = 1.0,
        retry_limits: dict | None = None,
        base_delays: dict | None = None,
        status_limits: dict | None = None,
        status_delay_multipliers: dict | None = None,
    ):
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.retry_on = tuple(retry_on or [TransientError, NetworkError])
        self.base_delay = base_delay
        self.retry_limits = retry_limits or {}
        self.base_delays = base_delays or {}
        self.status_limits = {500: 1} if status_limits is None else status_limits
        self.status_delay_multipliers = (
            {429: 3.0} if status_delay_multipliers is None else status_delay_multipliers
        )

        self.errors_by_type: Counter = Counter()
        self.retry_count = 0
        self.retry_time_total = 0.0
        self.successful_retries = 0
        self.permanent_urls: dict[str, str] = {}

    @staticmethod
    def lookup(settings: dict, error: Exception, default):
        for error_type, value in settings.items():
            if isinstance(error, error_type):
                return value
        return default

    def get_limit(self, error: Exception) -> int:
        status = getattr(error, "status", None)
        limit = self.lookup(self.retry_limits, error, self.max_retries)
        if status in self.status_limits:
            limit = min(limit, self.status_limits[status])
        return limit

    def get_delay(self, retry_number: int, error: Exception | None = None) -> float:
        base = self.lookup(self.base_delays, error, self.base_delay)
        multiplier = self.status_delay_multipliers.get(getattr(error, "status", None), 1.0)
        return base * multiplier * self.backoff_factor ** (retry_number - 1)

    def finish_retry(self, started_at: float):
        self.retry_count += 1
        self.retry_time_total += time.monotonic() - started_at

    async def execute_with_retry(self, coro, *args, **kwargs):
        url = next((arg for arg in args if isinstance(arg, str)), "")
        retries: dict[tuple, int] = {}
        attempt = 0
        retry_started: float | None = None
        while True:
            attempt += 1
            try:
                result = await coro(*args, **kwargs)
            except Exception as raw_error:
                if retry_started is not None:
                    self.finish_retry(retry_started)
                error = classify_error(raw_error, url)
                self.errors_by_type[type(error).__name__] += 1
                key = (type(error), error.status)
                retries[key] = retries.get(key, 0) + 1
                can_retry = (
                    isinstance(error, self.retry_on)
                    and attempt <= self.max_retries
                    and retries[key] <= self.get_limit(error)
                )
                if not can_retry:
                    if isinstance(error, PermanentError):
                        self.permanent_urls[url] = str(error)
                    if error is raw_error:
                        raise
                    raise error from raw_error
                delay = self.get_delay(retries[key], error)
                logger.warning(
                    "%s (%s) на попытке %d, повтор через %.2f сек: %s",
                    type(error).__name__, error, attempt, delay, url,
                )
                retry_started = time.monotonic()
                await asyncio.sleep(delay)
            else:
                if retry_started is not None:
                    self.finish_retry(retry_started)
                    self.successful_retries += 1
                return result

    def get_stats(self) -> dict:
        average = self.retry_time_total / self.retry_count if self.retry_count else 0.0
        return {
            "errors_by_type": dict(self.errors_by_type),
            "retries": self.retry_count,
            "successful_retries": self.successful_retries,
            "avg_retry_time": average,
            "permanent_urls": dict(self.permanent_urls),
        }


class CircuitBreaker:
    def __init__(
        self,
        failure_rate: float = 0.5,
        window_size: int = 10,
        min_requests: int = 5,
        recovery_timeout: float = 30.0,
    ):
        self.failure_rate = failure_rate
        self.window_size = window_size
        self.min_requests = min_requests
        self.recovery_timeout = recovery_timeout
        self.results: dict[str, deque[bool]] = {}
        self.opened_at: dict[str, float] = {}
        self.probing: set[str] = set()

    def get_state(self, domain: str) -> str:
        if domain not in self.opened_at:
            return "closed"
        if time.monotonic() - self.opened_at[domain] >= self.recovery_timeout:
            return "half-open"
        return "open"

    def allow_request(self, domain: str) -> bool:
        state = self.get_state(domain)
        if state == "closed":
            return True
        if state == "half-open" and domain not in self.probing:
            self.probing.add(domain)
            logger.info("Circuit breaker: пробный запрос к %s", domain)
            return True
        return False

    def record_success(self, domain: str):
        if domain in self.opened_at:
            if domain not in self.probing:
                return
            del self.opened_at[domain]
            self.probing.discard(domain)
            self.results[domain] = deque(maxlen=self.window_size)
            logger.info("Circuit breaker: %s снова доступен", domain)
        self.results.setdefault(domain, deque(maxlen=self.window_size)).append(True)

    def record_failure(self, domain: str):
        if domain in self.probing:
            self.probing.discard(domain)
            self.opened_at[domain] = time.monotonic()
            logger.warning("Circuit breaker: пробный запрос к %s не удался, блокировка продлена", domain)
            return
        results = self.results.setdefault(domain, deque(maxlen=self.window_size))
        results.append(False)
        failures = results.count(False)
        if (
            domain not in self.opened_at
            and len(results) >= self.min_requests
            and failures / len(results) >= self.failure_rate
        ):
            self.opened_at[domain] = time.monotonic()
            logger.warning(
                "Circuit breaker: %s заблокирован на %.1f сек (%d ошибок из %d)",
                domain, self.recovery_timeout, failures, len(results),
            )


class HTMLParser:
    def __init__(self):
        self.parse_errors: list[ParseError] = []

    def empty_result(self, url: str) -> dict:
        return {
            "url": url,
            "title": "",
            "text": "",
            "links": [],
            "metadata": {},
            "images": [],
            "headings": {},
            "tables": [],
            "lists": [],
        }

    async def parse_html(self, html: str, url: str) -> dict:
        result = self.empty_result(url)
        try:
            soup = await asyncio.to_thread(BeautifulSoup, html, "lxml")
        except Exception as e:
            logger.warning("Не удалось разобрать HTML (%s): %s", type(e).__name__, url)
            self.parse_errors.append(ParseError(type(e).__name__, url))
            return result

        extractors = {
            "metadata": lambda: self.extract_metadata(soup),
            "text": lambda: self.extract_text(soup),
            "links": lambda: self.extract_links(soup, url),
            "images": lambda: self.extract_images(soup, url),
            "headings": lambda: self.extract_headings(soup),
            "tables": lambda: self.extract_tables(soup),
            "lists": lambda: self.extract_lists(soup),
        }
        for field, extract in extractors.items():
            try:
                result[field] = extract()
            except Exception as e:
                logger.warning(
                    "Ошибка при извлечении %s (%s): %s", field, type(e).__name__, url
                )
                self.parse_errors.append(ParseError(f"{field}: {type(e).__name__}", url))

        result["title"] = result["metadata"].get("title", "")
        return result

    def extract_links(self, soup: BeautifulSoup, base_url: str) -> list[str]:
        links = []
        for tag in soup.find_all("a", href=True):
            absolute_url = urljoin(base_url, tag["href"])
            if self.is_valid_url(absolute_url):
                links.append(absolute_url)
        return links

    def is_valid_url(self, url: str) -> bool:
        parsed = urlparse(url)
        return parsed.scheme in ("http", "https") and bool(parsed.netloc)

    def extract_text(self, soup: BeautifulSoup, selector: str = None) -> str:
        root = soup.select_one(selector) if selector else soup
        if root is None:
            return ""
        return root.get_text(separator=" ", strip=True)

    def extract_metadata(self, soup: BeautifulSoup) -> dict:
        def meta_content(name: str) -> str:
            tag = soup.find("meta", attrs={"name": name})
            return tag.get("content", "") if tag else ""

        title = soup.title.get_text(strip=True) if soup.title else ""
        return {
            "title": title,
            "description": meta_content("description"),
            "keywords": meta_content("keywords"),
        }
    
    def extract_images(self, soup: BeautifulSoup, base_url: str) -> list[dict]:
        images = []
        for tag in soup.find_all("img", src=True):
            images.append({
                "src": urljoin(base_url, tag["src"]),
                "alt": tag.get("alt", ""),
            })
        return images

    def extract_headings(self, soup: BeautifulSoup) -> dict[str, list[str]]:
        headings = {}
        for level in ("h1", "h2", "h3"):
            headings[level] = [
                tag.get_text(strip=True) for tag in soup.find_all(level)
            ]
        return headings

    def extract_tables(self, soup: BeautifulSoup) -> list[list[list[str]]]:
        tables = []
        for table in soup.find_all("table"):
            rows = []
            for tr in table.find_all("tr"):
                cells = [
                    cell.get_text(strip=True) for cell in tr.find_all(["th", "td"])
                ]
                if cells:
                    rows.append(cells)
            tables.append(rows)
        return tables

    def extract_lists(self, soup: BeautifulSoup) -> list[dict]:
        lists = []
        for tag in soup.find_all(["ul", "ol"]):
            items = [
                li.get_text(strip=True) for li in tag.find_all("li", recursive=False)
            ]
            lists.append({"type": tag.name, "items": items})
        return lists


class CrawlerQueue:
    def __init__(self):
        self.queue = asyncio.PriorityQueue()
        self.counter = itertools.count()
        self.seen: set[str] = set()
        self.processed: set[str] = set()
        self.failed: dict[str, str] = {}

    def add_url(self, url: str, priority: int = 0):
        if url in self.seen:
            return
        self.seen.add(url)
        self.queue.put_nowait((-priority, next(self.counter), url))

    def requeue(self, url: str, priority: int = 0):
        self.queue.put_nowait((-priority, next(self.counter), url))

    async def get_next(self, timeout: float | None = None) -> str | None:
        if self.queue.empty() and timeout is None:
            return None
        try:
            _, _, url = await asyncio.wait_for(self.queue.get(), timeout)
        except asyncio.TimeoutError:
            return None
        return url

    def mark_processed(self, url: str):
        self.processed.add(url)

    def mark_failed(self, url: str, error: str):
        self.failed[url] = error

    def get_stats(self) -> dict:
        return {
            "queued": self.queue.qsize(),
            "processed": len(self.processed),
            "failed": len(self.failed),
            "total_seen": len(self.seen),
        }


class SemaphoreManager:
    def __init__(self, global_limit: int = 10, per_domain_limit: int = 2):
        self.global_semaphore = asyncio.Semaphore(global_limit)
        self.per_domain_limit = per_domain_limit
        self.domain_semaphores: dict[str, asyncio.Semaphore] = {}
        self.active_total = 0
        self.active_by_domain: dict[str, int] = {}

    @asynccontextmanager
    async def acquire(self, url: str):
        domain = urlparse(url).netloc
        if domain not in self.domain_semaphores:
            self.domain_semaphores[domain] = asyncio.Semaphore(self.per_domain_limit)

        async with self.domain_semaphores[domain]:
            async with self.global_semaphore:
                self.active_total += 1
                self.active_by_domain[domain] = self.active_by_domain.get(domain, 0) + 1
                try:
                    yield
                finally:
                    self.active_total -= 1
                    self.active_by_domain[domain] -= 1

    def get_active(self) -> dict:
        return {
            "total": self.active_total,
            "by_domain": {
                domain: count
                for domain, count in self.active_by_domain.items()
                if count > 0
            },
        }


class RateLimiter:
    def __init__(
        self,
        requests_per_second: float = 1.0,
        per_domain: bool = True,
        min_delay: float = 0.0,
        jitter: float = 0.0,
        stats_window: float = 5.0,
    ):
        self.interval = max(1.0 / requests_per_second, min_delay)
        self.jitter = jitter
        self.per_domain = per_domain
        self.next_allowed: dict[str, float] = {}
        self.domain_intervals: dict[str, float] = {}

        self.stats_window = stats_window
        self.recent_requests: deque[float] = deque()
        self.first_request: float | None = None
        self.last_request: dict[str, float] = {}
        self.waiting_domains: set[str] = set()
        self.gaps_total = 0.0
        self.gaps_count = 0
        self.min_gap: float | None = None

    def set_domain_delay(self, domain: str, delay: float):
        self.domain_intervals[domain] = max(self.interval, delay)

    def time_until_allowed(self, domain: str = None) -> float:
        key = domain if self.per_domain and domain else "global"
        return max(0.0, self.next_allowed.get(key, 0.0) - time.monotonic())

    def mark_waiting(self, domain: str):
        self.waiting_domains.add(domain)

    async def acquire(self, domain: str = None):
        key = domain if self.per_domain and domain else "global"
        interval = self.domain_intervals.get(key, self.interval)
        now = time.monotonic()
        slot = max(now, self.next_allowed.get(key, now))
        self.next_allowed[key] = slot + interval + random.uniform(0, self.jitter)
        wait = slot - now
        if wait > 0:
            await asyncio.sleep(wait)
        self.record_request(domain or "global", throttled=wait > 0)

    def record_request(self, domain: str, throttled: bool):
        now = time.monotonic()
        if self.first_request is None:
            self.first_request = now
        self.recent_requests.append(now)
        throttled = throttled or domain in self.waiting_domains
        self.waiting_domains.discard(domain)
        if throttled and domain in self.last_request:
            gap = now - self.last_request[domain]
            self.gaps_total += gap
            self.gaps_count += 1
            if self.min_gap is None or gap < self.min_gap:
                self.min_gap = gap
        self.last_request[domain] = now

    def get_stats(self) -> dict:
        now = time.monotonic()
        while self.recent_requests and now - self.recent_requests[0] > self.stats_window:
            self.recent_requests.popleft()
        if self.first_request is None:
            current_rate = 0.0
        else:
            window = max(min(self.stats_window, now - self.first_request), 1.0)
            current_rate = len(self.recent_requests) / window
        avg_delay = self.gaps_total / self.gaps_count if self.gaps_count else 0.0
        return {
            "requests_per_second": current_rate,
            "avg_delay": avg_delay,
            "min_delay": self.min_gap or 0.0,
        }


class RobotsParser:
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session
        self.rules: dict[str, RobotFileParser] = {}
        self.crawl_delays: dict[str, dict[str, float]] = {}
        self.locks: dict[str, asyncio.Lock] = {}

    async def fetch_robots(self, base_url: str) -> dict:
        parsed = urlparse(base_url)
        domain = parsed.netloc
        robots_url = f"{parsed.scheme}://{domain}/robots.txt"

        if domain not in self.locks:
            self.locks[domain] = asyncio.Lock()
        async with self.locks[domain]:
            if domain not in self.rules:
                parser = RobotFileParser(robots_url)
                delays: dict[str, float] = {}
                try:
                    async with self.session.get(robots_url) as response:
                        if response.status == 200:
                            text = await response.text()
                            parser.parse(text.splitlines())
                            delays = self.parse_crawl_delays(text)
                        elif response.status in (401, 403):
                            parser.disallow_all = True
                        else:
                            parser.allow_all = True
                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                    logger.warning("Не удалось загрузить robots.txt (%s): %s", type(e).__name__, robots_url)
                    parser.allow_all = True
                self.rules[domain] = parser
                self.crawl_delays[domain] = delays

        parser = self.rules[domain]
        return {
            "domain": domain,
            "robots_url": robots_url,
            "allow_all": parser.allow_all,
            "disallow_all": parser.disallow_all,
            "crawl_delay": self.get_crawl_delay(domain=domain),
        }

    def parse_crawl_delays(self, text: str) -> dict[str, float]:
        delays: dict[str, float] = {}
        agents: list[str] = []
        in_rules = False
        for raw_line in text.splitlines():
            line = raw_line.split("#", 1)[0].strip()
            if ":" not in line:
                continue
            field, value = line.split(":", 1)
            field = field.strip().lower()
            value = value.strip()
            if field == "user-agent":
                if in_rules:
                    agents = []
                    in_rules = False
                agents.append(value.lower())
                continue
            in_rules = True
            if field == "crawl-delay":
                try:
                    delay = float(value)
                except ValueError:
                    continue
                for agent in agents:
                    delays[agent] = delay
        return delays

    def can_fetch(self, url: str, user_agent: str = "*") -> bool:
        parser = self.rules.get(urlparse(url).netloc)
        if parser is None:
            return True
        return parser.can_fetch(user_agent, url)

    def get_crawl_delay(self, user_agent: str = "*", domain: str | None = None) -> float:
        delays = self.crawl_delays.get(domain, {})
        name = user_agent.split("/")[0].lower()
        for agent, delay in delays.items():
            if agent != "*" and agent in name:
                return delay
        return delays.get("*", 0.0)


class DataStorage(ABC):
    FIELDS = [
        "url", "title", "text", "links", "metadata",
        "crawled_at", "status_code", "content_type",
    ]

    @staticmethod
    def normalize(data: dict) -> dict:
        return {
            "url": str(data.get("url", "")),
            "title": data.get("title") or "",
            "text": data.get("text") or "",
            "links": list(data.get("links") or []),
            "metadata": dict(data.get("metadata") or {}),
            "crawled_at": data.get("crawled_at") or datetime.now(),
            "status_code": data.get("status_code"),
            "content_type": data.get("content_type") or "",
        }

    @abstractmethod
    async def save(self, data: dict):
        pass

    @abstractmethod
    async def close(self):
        pass


class JSONStorage(DataStorage):
    def __init__(
        self,
        path: str,
        pretty: bool = False,
        encoding: str = "utf-8",
        buffer_size: int = 50,
    ):
        self.path = path
        self.pretty = pretty
        self.encoding = encoding
        self.buffer_size = buffer_size
        self.buffer: list[str] = []
        self.file = None
        self.count = 0
        self.writes = 0
        self.closed = False
        self.lock = asyncio.Lock()

    async def open(self):
        self.file = await aiofiles.open(self.path, "w", encoding=self.encoding)
        if self.pretty:
            await self.file.write("[\n")

    def serialize(self, data: dict) -> str:
        if self.pretty:
            text = json.dumps(data, ensure_ascii=False, indent=2, default=str)
            return textwrap.indent(text, "  ")
        return json.dumps(data, ensure_ascii=False, default=str)

    async def flush(self):
        if not self.buffer:
            return
        await self.file.write("".join(self.buffer))
        self.buffer.clear()
        self.writes += 1

    async def save(self, data: dict):
        data = self.normalize(data)
        async with self.lock:
            if self.file is None:
                await self.open()
            text = self.serialize(data)
            if self.pretty:
                separator = ",\n" if self.count else ""
                self.buffer.append(separator + text)
            else:
                self.buffer.append(text + "\n")
            self.count += 1
            if len(self.buffer) >= self.buffer_size:
                await self.flush()

    async def close(self):
        async with self.lock:
            if self.closed:
                return
            if self.file is None:
                await self.open()
            await self.flush()
            if self.pretty:
                await self.file.write("\n]\n")
            await self.file.close()
            self.closed = True


class CSVStorage(DataStorage):
    def __init__(
        self,
        path: str,
        encoding: str = "utf-8-sig",
        delimiter: str = ",",
        buffer_size: int = 50,
    ):
        self.path = path
        self.encoding = encoding
        self.delimiter = delimiter
        self.buffer_size = buffer_size
        self.buffer: list[list[str]] = []
        self.fieldnames: list[str] | None = None
        self.file = None
        self.writes = 0
        self.closed = False
        self.lock = asyncio.Lock()

    @staticmethod
    def to_cell(value) -> str:
        if value is None:
            return ""
        if isinstance(value, (list, dict)):
            return json.dumps(value, ensure_ascii=False, default=str)
        return str(value)

    def format_rows(self, rows: list[list[str]]) -> str:
        buffer = io.StringIO()
        csv.writer(buffer, delimiter=self.delimiter).writerows(rows)
        return buffer.getvalue()

    async def flush(self):
        if not self.buffer:
            return
        await self.file.write(self.format_rows(self.buffer))
        self.buffer.clear()
        self.writes += 1

    async def save(self, data: dict):
        data = self.normalize(data)
        async with self.lock:
            if self.file is None:
                self.file = await aiofiles.open(
                    self.path, "w", encoding=self.encoding, errors="replace", newline=""
                )
                self.fieldnames = list(data.keys())
                self.buffer.append(self.fieldnames)
            self.buffer.append([self.to_cell(data.get(field)) for field in self.fieldnames])
            if len(self.buffer) >= self.buffer_size:
                await self.flush()

    async def close(self):
        async with self.lock:
            if self.closed:
                return
            if self.file is not None:
                await self.flush()
                await self.file.close()
            self.closed = True


class SQLiteStorage(DataStorage):
    COLUMNS = DataStorage.FIELDS

    def __init__(self, path: str, batch_size: int = 50):
        self.path = path
        self.batch_size = batch_size
        self.db: aiosqlite.Connection | None = None
        self.buffer: list[tuple] = []
        self.batches_written = 0
        self.closed = False
        self.lock = asyncio.Lock()

    async def init_db(self):
        self.db = await aiosqlite.connect(self.path)
        await self.db.execute(
            """
            CREATE TABLE IF NOT EXISTS pages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT NOT NULL UNIQUE,
                title TEXT,
                text TEXT,
                links TEXT,
                metadata TEXT,
                crawled_at TEXT,
                status_code INTEGER,
                content_type TEXT
            )
            """
        )
        await self.db.execute("CREATE INDEX IF NOT EXISTS idx_pages_status ON pages(status_code)")
        await self.db.execute("CREATE INDEX IF NOT EXISTS idx_pages_crawled_at ON pages(crawled_at)")
        await self.db.commit()

    @staticmethod
    def to_value(value):
        if isinstance(value, (list, dict)):
            return json.dumps(value, ensure_ascii=False, default=str)
        if isinstance(value, datetime):
            return value.isoformat()
        return value

    async def flush(self):
        if not self.buffer:
            return
        placeholders = ", ".join("?" for _ in self.COLUMNS)
        await self.db.executemany(
            f"INSERT OR REPLACE INTO pages ({', '.join(self.COLUMNS)}) VALUES ({placeholders})",
            self.buffer,
        )
        await self.db.commit()
        self.buffer.clear()
        self.batches_written += 1

    async def save(self, data: dict):
        async with self.lock:
            if self.db is None:
                await self.init_db()
            self.buffer.append(tuple(self.to_value(data.get(column)) for column in self.COLUMNS))
            if len(self.buffer) >= self.batch_size:
                await self.flush()

    async def close(self):
        async with self.lock:
            if self.closed:
                return
            if self.db is None:
                await self.init_db()
            await self.flush()
            await self.db.close()
            self.closed = True

def setup_logging(
    log_file: str | None = "crawler.log",
    console_level: str = "INFO",
    file_level: str = "DEBUG",
    max_bytes: int = 1_000_000,
    backup_count: int = 3,
) -> logging.Logger:
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
        handler.close()

    console = logging.StreamHandler()
    console.setLevel(console_level.upper())
    console.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S"))
    logger.addHandler(console)

    if log_file:
        file_handler = logging.handlers.RotatingFileHandler(
            log_file, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8"
        )
        file_handler.setLevel(file_level.upper())
        file_handler.setFormatter(logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
        ))
        logger.addHandler(file_handler)
    return logger

def load_config(path: str) -> dict:
    with open(path, encoding="utf-8") as file:
        loaded = yaml.safe_load(file) or {}

    config = copy.deepcopy(DEFAULT_CONFIG)
    for section, value in loaded.items():
        if section not in config:
            logger.warning("Неизвестный раздел конфигурации: %s", section)
            continue
        if isinstance(config[section], dict):
            for key, item in (value or {}).items():
                if key in config[section]:
                    config[section][key] = item
                else:
                    logger.warning("Неизвестный параметр конфигурации: %s.%s", section, key)
        else:
            config[section] = value
    logger.info("Конфигурация загружена из %s", path)
    return config


def create_storage(settings: dict) -> DataStorage | None:
    storage_types = {"json": JSONStorage, "csv": CSVStorage, "sqlite": SQLiteStorage}
    storage_type = settings.get("type")
    if not storage_type:
        return None
    if storage_type not in storage_types:
        raise ValueError(f"Неизвестный тип хранилища: {storage_type}")
    return storage_types[storage_type](settings["path"])

class CrawlerStats:
    def __init__(self):
        self.started_at: float | None = None
        self.finished_at: float | None = None
        self.successful = 0
        self.failed = 0
        self.blocked = 0
        self.status_codes: Counter = Counter()
        self.domains: Counter = Counter()
        self.errors_by_type: Counter = Counter()

    def start(self):
        self.started_at = time.monotonic()
        self.finished_at = None

    def finish(self):
        self.finished_at = time.monotonic()

    def record_success(self, url: str, status_code: int | None):
        self.successful += 1
        if status_code is not None:
            self.status_codes[status_code] += 1
        self.domains[urlparse(url).netloc] += 1

    def record_failure(self, url: str, error_type: str, status_code: int | None = None):
        self.failed += 1
        self.errors_by_type[error_type] += 1
        if status_code is not None:
            self.status_codes[status_code] += 1

    def record_blocked(self, url: str):
        self.blocked += 1

    @property
    def duration(self) -> float:
        if self.started_at is None:
            return 0.0
        end = self.finished_at if self.finished_at is not None else time.monotonic()
        return end - self.started_at

    def get_stats(self, top_n: int = 5) -> dict:
        total = self.successful + self.failed
        duration = self.duration
        return {
            "total_pages": total,
            "successful": self.successful,
            "failed": self.failed,
            "blocked": self.blocked,
            "success_rate": round(self.successful / total * 100, 1) if total else 0.0,
            "duration": round(duration, 2),
            "avg_speed": round(total / duration, 2) if duration else 0.0,
            "status_codes": dict(sorted(self.status_codes.items())),
            "top_domains": self.domains.most_common(top_n),
            "errors_by_type": dict(self.errors_by_type),
        }

    def export_to_json(self, filename: str):
        report = {"generated_at": datetime.now().isoformat(timespec="seconds"), **self.get_stats()}
        with open(filename, "w", encoding="utf-8") as file:
            json.dump(report, file, ensure_ascii=False, indent=2)
        logger.info("Статистика сохранена в %s", filename)

    def export_to_html_report(self, filename: str):
        stats = self.get_stats(top_n=10)
        generated_at = datetime.now().strftime("%d.%m.%Y %H:%M:%S")

        cards = [
            ("Всего страниц", stats["total_pages"]),
            ("Успешно", stats["successful"]),
            ("С ошибкой", stats["failed"]),
            ("Заблокировано", stats["blocked"]),
            ("Успешных, %", stats["success_rate"]),
            ("Время работы, с", stats["duration"]),
            ("Скорость, стр/с", stats["avg_speed"]),
        ]
        cards_html = "".join(
            f'<div class="card"><div class="value">{value}</div><div class="label">{label}</div></div>'
            for label, value in cards
        )

        def bar_table(title, rows, column):
            if not rows:
                return f"<h2>{title}</h2><p class='empty'>Нет данных</p>"
            maximum = max(count for _, count in rows)
            body = "".join(
                f"<tr><td>{escape(str(name))}</td><td class='num'>{count}</td>"
                f"<td class='bar-cell'><div class='bar' style='width:{count / maximum * 100:.0f}%'></div></td></tr>"
                for name, count in rows
            )
            return (f"<h2>{title}</h2><table><tr><th>{column}</th><th>Страниц</th><th></th></tr>"
                    f"{body}</table>")

        sections = [
            bar_table("Распределение по статус-кодам", list(stats["status_codes"].items()), "Код"),
            bar_table("Топ доменов", stats["top_domains"], "Домен"),
            bar_table("Ошибки по типам", list(stats["errors_by_type"].items()), "Тип ошибки"),
        ]

        page = f"""<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="utf-8">
<title>Отчёт краулера</title>
<style>
  body {{ font-family: -apple-system, Arial, sans-serif; margin: 40px; color: #222; background: #fafafa; }}
  h1 {{ margin-bottom: 4px; }}
  .meta {{ color: #777; margin-bottom: 24px; }}
  .cards {{ display: flex; flex-wrap: wrap; gap: 12px; margin-bottom: 32px; }}
  .card {{ background: #fff; border: 1px solid #e3e3e3; border-radius: 8px; padding: 14px 18px; min-width: 120px; }}
  .value {{ font-size: 26px; font-weight: 600; }}
  .label {{ color: #777; font-size: 13px; }}
  table {{ border-collapse: collapse; background: #fff; margin-bottom: 28px; min-width: 520px; }}
  th, td {{ border-bottom: 1px solid #eee; padding: 8px 12px; text-align: left; }}
  .num {{ text-align: right; }}
  .bar-cell {{ width: 240px; }}
  .bar {{ height: 12px; background: #4f7cff; border-radius: 3px; }}
  .empty {{ color: #999; }}
</style>
</head>
<body>
<h1>Отчёт краулера</h1>
<div class="meta">Сформирован {generated_at}</div>
<div class="cards">{cards_html}</div>
{"".join(sections)}
</body>
</html>
"""
        with open(filename, "w", encoding="utf-8") as file:
            file.write(page)
        logger.info("HTML отчёт сохранён в %s", filename)

class SitemapParser:
    def __init__(self, session: aiohttp.ClientSession, max_nesting: int = 3):
        self.session = session
        self.max_nesting = max_nesting

    async def fetch_sitemap(self, sitemap_url: str) -> list[str]:
        urls = await self.process_sitemap(sitemap_url, level=0, seen=set())
        logger.info("Из sitemap получено адресов: %d", len(urls))
        return urls

    async def process_sitemap(self, sitemap_url: str, level: int, seen: set[str]) -> list[str]:
        if sitemap_url in seen or level > self.max_nesting:
            return []
        seen.add(sitemap_url)

        try:
            async with self.session.get(sitemap_url) as response:
                response.raise_for_status()
                content = await response.read()
        except (aiohttp.ClientError, asyncio.TimeoutError) as error:
            logger.warning("Не удалось загрузить sitemap (%s): %s", type(error).__name__, sitemap_url)
            return []

        try:
            root = ET.fromstring(content)
        except ET.ParseError:
            logger.warning("Некорректный XML в sitemap: %s", sitemap_url)
            return []

        root_tag = root.tag.split("}")[-1]
        locations = [
            element.text.strip()
            for element in root.iter()
            if element.tag.split("}")[-1] == "loc" and element.text
        ]

        if root_tag == "sitemapindex":
            logger.info("Индекс sitemap, вложенных карт: %d: %s", len(locations), sitemap_url)
            nested = await asyncio.gather(
                *(self.process_sitemap(location, level + 1, seen) for location in locations)
            )
            urls = [url for group in nested for url in group]
        elif root_tag == "urlset":
            urls = locations
        else:
            logger.warning("Неизвестный формат sitemap <%s>: %s", root_tag, sitemap_url)
            return []

        return list(dict.fromkeys(urls))

class AsyncCrawler:
    def __init__(
        self,
        max_concurrent: int = 10,
        max_depth: int = 2,
        per_domain_limit: int | None = None,
        requests_per_second: float | None = None,
        respect_robots: bool = False,
        user_agent: str = "AsyncCrawler/1.0",
        min_delay: float = 0.0,
        jitter: float = 0.0,
        max_retries: int = 0,
        backoff_base: float = 1.0,
        retry_strategy: RetryStrategy | None = None,
        circuit_breaker: CircuitBreaker | None = None,
        storage: DataStorage | None = None,
        save_retries: int = 2,
        save_retry_delay: float = 0.1,
        connect_timeout: float = 5,
        read_timeout: float = 10,
        total_timeout: float | None = None,
        timeout_growth: float = 1.5,
    ):
        self.max_concurrent = max_concurrent
        self.max_depth = max_depth
        self.respect_robots = respect_robots
        self.user_agent = user_agent
        self.jitter = jitter
        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout
        self.total_timeout = total_timeout
        self.timeout_growth = timeout_growth
        self.circuit_breaker = circuit_breaker
        self.storage = storage
        self.save_retries = save_retries
        self.save_retry_delay = save_retry_delay
        self.retry_strategy = retry_strategy or RetryStrategy(
            max_retries=max_retries,
            base_delay=backoff_base,
        )
        self.semaphores = SemaphoreManager(
            global_limit=max_concurrent,
            per_domain_limit=per_domain_limit or max_concurrent,
        )
        self.parser = HTMLParser()
        self.rate_limiter = RateLimiter(
            requests_per_second=requests_per_second or float("inf"),
            min_delay=min_delay,
            jitter=jitter,
        )

        self.queue = CrawlerQueue()
        self.visited_urls: set[str] = set()
        self.failed_urls: dict[str, str] = {}
        self.processed_urls: dict[str, dict] = {}
        self.blocked_urls: set[str] = set()
        self.url_depths: dict[str, int] = {}
        self.attempts: dict[str, int] = {}
        self.responses: dict[str, dict] = {}
        self.saved_count = 0
        self.save_errors: dict[str, str] = {}
        self.in_progress = 0
        self.max_pages = 0
        self.same_domain_only = False
        self.allowed_domains: set[str] = set()
        self.exclude_patterns: list[re.Pattern] = []
        self.include_patterns: list[re.Pattern] = []

        # используется системный поиск адресов, чтобы краулер работал через VPN
        connector = aiohttp.TCPConnector(
            limit=max_concurrent,
            resolver=aiohttp.ThreadedResolver(),
        )
        self.session = aiohttp.ClientSession(
            timeout=self.make_timeout(attempt=1),
            connector=connector,
            headers={"User-Agent": user_agent},
        )
        self.robots = RobotsParser(self.session)
        self.sitemap = SitemapParser(self.session)

    def make_timeout(self, attempt: int) -> aiohttp.ClientTimeout:
        factor = self.timeout_growth ** (attempt - 1)
        return aiohttp.ClientTimeout(
            total=self.total_timeout * factor if self.total_timeout else None,
            connect=self.connect_timeout * factor,
            sock_read=self.read_timeout * factor,
        )

    async def check_robots(self, url: str) -> bool:
        domain = urlparse(url).netloc
        await self.robots.fetch_robots(url)
        delay = self.robots.get_crawl_delay(self.user_agent, domain)
        if delay:
            self.rate_limiter.set_domain_delay(domain, delay)
        if self.robots.can_fetch(url, self.user_agent):
            return True
        logger.warning("Заблокировано robots.txt: %s", url)
        self.blocked_urls.add(url)
        return False

    async def fetch_url(self, url: str) -> str | None:
        if self.respect_robots and not await self.check_robots(url):
            return None
        self.attempts[url] = 0
        try:
            text = await self.retry_strategy.execute_with_retry(self.try_fetch, url)
        except CrawlerError as error:
            logger.error(
                "Итог: неудача после %d попыток (%s: %s): %s",
                self.attempts[url], type(error).__name__, error, url,
            )
            self.failed_urls[url] = f"{type(error).__name__}: {error}"
            if error.status is not None:
                self.responses[url] = {"status_code": error.status, "content_type": ""}
            return None
        logger.info("Итог: успех с попытки %d: %s", self.attempts[url], url)
        return text

    async def try_fetch(self, url: str) -> str:
        self.attempts[url] = self.attempts.get(url, 0) + 1
        attempt = self.attempts[url]
        domain = urlparse(url).netloc
        if self.circuit_breaker and not self.circuit_breaker.allow_request(domain):
            logger.warning("Попытка %d: домен %s временно заблокирован: %s", attempt, domain, url)
            raise CircuitOpenError(f"домен {domain} временно заблокирован", url)

        timeout = self.make_timeout(attempt)
        async with self.semaphores.acquire(url):
            await self.rate_limiter.acquire(domain)
            logger.info(
                "Начало загрузки (попытка %d, таймаут чтения %.1f сек): %s",
                attempt, timeout.sock_read, url,
            )
            try:
                async with self.session.get(url, timeout=timeout) as response:
                    response.raise_for_status()
                    text = await response.text()
            except Exception as raw_error:
                error = classify_error(raw_error, url)
                logger.error(
                    "Попытка %d: %s (%s): %s",
                    attempt, type(error).__name__, error, url,
                )
                if self.circuit_breaker:
                    if isinstance(error, (TransientError, NetworkError)):
                        self.circuit_breaker.record_failure(domain)
                    else:
                        self.circuit_breaker.record_success(domain)
                if error is raw_error:
                    raise
                raise error from raw_error
            if self.circuit_breaker:
                self.circuit_breaker.record_success(domain)
            self.responses[url] = {
                "status_code": response.status,
                "content_type": response.headers.get("Content-Type", ""),
            }
            logger.info(
                "Попытка %d успешна: %s (статус %s, %d символов)",
                attempt, url, response.status, len(text),
            )
            return text

    async def save_page(self, record: dict) -> bool:
        url = record.get("url", "")
        for attempt in range(1, self.save_retries + 2):
            try:
                await self.storage.save(record)
            except Exception as error:
                if attempt > self.save_retries:
                    logger.error(
                        "Не удалось сохранить после %d попыток (%s: %s): %s",
                        attempt, type(error).__name__, error, url,
                    )
                    self.save_errors[url] = f"{type(error).__name__}: {error}"
                    return False
                delay = self.save_retry_delay * 2 ** (attempt - 1)
                logger.warning(
                    "Ошибка сохранения на попытке %d (%s: %s), повтор через %.2f сек: %s",
                    attempt, type(error).__name__, error, delay, url,
                )
                await asyncio.sleep(delay)
            else:
                self.saved_count += 1
                return True

    async def fetch_urls(self, urls: list[str]) -> dict[str, str]:
        tasks = [self.fetch_url(url) for url in urls]
        pages = await asyncio.gather(*tasks)
        return {url: page for url, page in zip(urls, pages) if page is not None}

    async def fetch_and_parse(self, url: str) -> dict:
        html = await self.fetch_url(url)
        if html is None:
            return self.parser.empty_result(url)
        return await self.parser.parse_html(html, url)

    async def crawl(
        self,
        start_urls: list[str],
        max_pages: int = 100,
        same_domain_only: bool = False,
        exclude_patterns: list[str] | None = None,
        include_patterns: list[str] | None = None,
    ) -> dict[str, dict]:
        self.queue = CrawlerQueue()
        self.visited_urls = set()
        self.failed_urls = {}
        self.processed_urls = {}
        self.blocked_urls = set()
        self.url_depths = {}
        self.attempts = {}
        self.responses = {}
        self.saved_count = 0
        self.save_errors = {}
        self.in_progress = 0
        self.max_pages = max_pages
        self.same_domain_only = same_domain_only
        self.allowed_domains = {urlparse(url).netloc for url in start_urls}
        self.exclude_patterns = [re.compile(p) for p in exclude_patterns or []]
        self.include_patterns = [re.compile(p) for p in include_patterns or []]

        for url in start_urls:
            url, _ = urldefrag(url)
            self.url_depths[url] = 0
            self.queue.add_url(url, priority=0)

        start_time = time.perf_counter()
        progress = asyncio.create_task(self.report_progress(start_time))
        workers = [
            asyncio.create_task(self.worker(max_pages))
            for _ in range(self.max_concurrent)
        ]
        try:
            await asyncio.gather(*workers)
        finally:
            progress.cancel()
            with suppress(asyncio.CancelledError):
                await progress
            self.print_progress(start_time)
        return self.processed_urls

    async def crawl_sitemap(self, sitemap_url: str, max_pages: int = 100, **filters) -> dict[str, dict]:
        urls = await self.sitemap.fetch_sitemap(sitemap_url)
        return await self.crawl(urls, max_pages=max_pages, **filters)

    def is_allowed(self, url: str) -> bool:
        if self.same_domain_only and urlparse(url).netloc not in self.allowed_domains:
            return False
        if any(pattern.search(url) for pattern in self.exclude_patterns):
            return False
        if self.include_patterns and not any(
            pattern.search(url) for pattern in self.include_patterns
        ):
            return False
        return True

    async def report_progress(self, start_time: float, interval: float = 1.0):
        while True:
            await asyncio.sleep(interval)
            self.print_progress(start_time)

    def print_progress(self, start_time: float):
        stats = self.queue.get_stats()
        rate_stats = self.rate_limiter.get_stats()
        elapsed = time.perf_counter() - start_time
        done = len(self.visited_urls) - self.in_progress
        total = max(min(self.max_pages, stats["total_seen"]), done, 1)
        percent = done / total
        speed = done / elapsed if elapsed > 0 else 0.0
        eta = f"{(total - done) / speed:.0f} сек" if speed > 0 else "неизвестно"
        filled = int(percent * 20)
        bar = "█" * filled + "░" * (20 - filled)
        print(
            f"[{bar}] {percent:4.0%} ({done}/{total}) | "
            f"Скорость: {speed:.1f} стр/сек | "
            f"Осталось: ~{eta} | "
            f"Активных задач: {self.in_progress} | "
            f"В очереди: {stats['queued']} | "
            f"Ошибок: {stats['failed']} | "
            f"Заблокировано: {len(self.blocked_urls)} | "
            f"Запросов: {rate_stats['requests_per_second']:.1f} req/sec | "
            f"Средняя задержка: {rate_stats['avg_delay']:.2f} сек"
        )

    async def worker(self, max_pages: int):
        while len(self.visited_urls) < max_pages:
            url = await self.queue.get_next(timeout=0.1)
            if url is None:
                if self.in_progress == 0:
                    break
                await asyncio.sleep(0.1)
                continue

            domain = urlparse(url).netloc
            wait = self.rate_limiter.time_until_allowed(domain)
            if wait > 0:
                self.rate_limiter.mark_waiting(domain)
                self.queue.requeue(url, priority=-self.url_depths[url])
                await asyncio.sleep(min(wait, 0.05))
                continue

            self.visited_urls.add(url)
            self.in_progress += 1
            try:
                await self.process_url(url)
            finally:
                self.in_progress -= 1

    async def process_url(self, url: str):
        page = await self.fetch_and_parse(url)

        if url in self.blocked_urls:
            return
        if url in self.failed_urls:
            self.queue.mark_failed(url, self.failed_urls[url])
            return

        if self.storage:
            record = {**page, **self.responses.get(url, {}), "crawled_at": datetime.now()}
            if not await self.save_page(record):
                self.failed_urls[url] = f"SaveError: {self.save_errors[url]}"
                self.queue.mark_failed(url, self.failed_urls[url])
                self.expand_links(url, page["links"])
                return

        self.processed_urls[url] = page
        self.queue.mark_processed(url)
        self.expand_links(url)

    def expand_links(self, url: str, links: list[str] | None = None):
        depth = self.url_depths[url]
        if depth >= self.max_depth:
            return
        if links is None:
            links = self.processed_urls[url]["links"]
        for link in links:
            link, _ = urldefrag(link)
            if self.is_allowed(link):
                self.add_link(link, depth + 1)

    def add_link(self, link: str, depth: int):
        known_depth = self.url_depths.get(link)
        if known_depth is not None and known_depth <= depth:
            return
        self.url_depths[link] = depth
        if link in self.processed_urls:
            self.expand_links(link)
        else:
            self.queue.add_url(link, priority=-depth)

    def get_error_stats(self) -> dict:
        stats = self.retry_strategy.get_stats()
        errors_by_type = Counter(stats["errors_by_type"])
        if self.parser.parse_errors:
            errors_by_type["ParseError"] += len(self.parser.parse_errors)
        stats["errors_by_type"] = dict(errors_by_type)
        return stats

    async def close(self):
        if self.storage:
            try:
                await self.storage.close()
            except Exception as error:
                logger.error("Не удалось закрыть хранилище (%s: %s)", type(error).__name__, error)
        await self.session.close()

class AdvancedCrawler(AsyncCrawler):
    def __init__(self, config: dict | None = None):
        self.config = copy.deepcopy(config or DEFAULT_CONFIG)
        setup_logging(**self.config["logging"])
        settings = self.config["crawler"]
        super().__init__(
            max_concurrent=settings["max_concurrent"],
            max_depth=settings["max_depth"],
            per_domain_limit=settings["per_domain_limit"],
            requests_per_second=settings["rate_limit"],
            respect_robots=settings["respect_robots"],
            user_agent=settings["user_agent"],
            min_delay=settings["min_delay"],
            jitter=settings["jitter"],
            max_retries=settings["max_retries"],
            storage=create_storage(self.config["storage"]),
        )
        self.stats = CrawlerStats()

    @classmethod
    def from_config(cls, path: str) -> "AdvancedCrawler":
        return cls(load_config(path))

    async def crawl(
        self,
        start_urls: list[str] | None = None,
        max_pages: int | None = None,
        **filters,
    ) -> dict[str, dict]:
        start_urls = start_urls or self.config["start_urls"]
        if not start_urls:
            raise ValueError("Не заданы стартовые URL")
        max_pages = max_pages or self.config["crawler"]["max_pages"]
        filters = {**self.config["filters"], **filters}

        self.stats = CrawlerStats()
        self.stats.start()
        logger.info("Старт обхода: %d стартовых URL, лимит %d страниц", len(start_urls), max_pages)
        try:
            return await super().crawl(start_urls, max_pages=max_pages, **filters)
        finally:
            self.stats.finish()
            stats = self.stats.get_stats()
            logger.info(
                "Обход завершён: %d страниц, успешно %d, с ошибкой %d, за %.1f сек",
                stats["total_pages"], stats["successful"], stats["failed"], stats["duration"],
            )

    async def process_url(self, url: str):
        await super().process_url(url)
        status_code = self.responses.get(url, {}).get("status_code")
        if url in self.blocked_urls:
            self.stats.record_blocked(url)
        elif url in self.failed_urls:
            error_type = self.failed_urls[url].split(":")[0]
            self.stats.record_failure(url, error_type, status_code)
        else:
            self.stats.record_success(url, status_code)

    def get_stats(self) -> dict:
        return self.stats.get_stats()

    def export_to_json(self, filename: str):
        self.stats.export_to_json(filename)

    def export_to_html_report(self, filename: str):
        self.stats.export_to_html_report(filename)

STORAGE_BY_EXTENSION = {".json": "json", ".csv": "csv", ".db": "sqlite", ".sqlite": "sqlite"}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Асинхронный веб-краулер")
    parser.add_argument("--urls", nargs="+", help="стартовые URL через пробел")
    parser.add_argument("--max-pages", type=int, help="максимум страниц")
    parser.add_argument("--max-depth", type=int, help="максимальная глубина обхода")
    parser.add_argument("--output", help="файл результатов: .json, .csv, .db или .sqlite")
    parser.add_argument("--config", help="путь к YAML конфигу")
    parser.add_argument("--respect-robots", action="store_true", default=None, help="соблюдать robots.txt")
    parser.add_argument("--rate-limit", type=float, help="запросов в секунду на домен")
    args = parser.parse_args(argv)
    if args.output and Path(args.output).suffix.lower() not in STORAGE_BY_EXTENSION:
        parser.error("--output должен оканчиваться на .json, .csv, .db или .sqlite")
    return args


def build_config(args: argparse.Namespace) -> dict:
    config = load_config(args.config) if args.config else copy.deepcopy(DEFAULT_CONFIG)
    if args.urls:
        config["start_urls"] = args.urls
    overrides = {
        "max_pages": args.max_pages,
        "max_depth": args.max_depth,
        "respect_robots": args.respect_robots,
        "rate_limit": args.rate_limit,
    }
    for key, value in overrides.items():
        if value is not None:
            config["crawler"][key] = value
    if args.output:
        config["storage"] = {
            "type": STORAGE_BY_EXTENSION[Path(args.output).suffix.lower()],
            "path": args.output,
        }
    return config


async def run(config: dict):
    crawler = AdvancedCrawler(config)
    try:
        await crawler.crawl()
    finally:
        await crawler.close()
    stats = crawler.get_stats()
    print(f"Обработано: {stats['total_pages']}")
    print(f"Успешно: {stats['successful']}")
    print(f"Ошибок: {stats['failed']}")
    if config["storage"]["path"]:
        print(f"Результаты сохранены в {config['storage']['path']}")


def main(argv: list[str] | None = None):
    args = parse_args(argv)
    config = build_config(args)
    if not config["start_urls"]:
        print("Не заданы стартовые URL: укажите --urls или start_urls в конфиге")
        return
    asyncio.run(run(config))


if __name__ == "__main__":
    main()