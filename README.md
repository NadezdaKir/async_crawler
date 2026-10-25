# Асинхронный веб-краулер

Учебный проект курса «Параллельность и асинхронность». Краулер обходит сайты асинхронно на `asyncio` и `aiohttp`, разбирает HTML, соблюдает правила вежливости, повторяет запросы при временных ошибках и сохраняет результаты в JSON, CSV или SQLite. Проект собирался семь дней, от простой загрузки страниц до итогового класса `AdvancedCrawler` с конфигурацией, статистикой, отчётами и командной строкой.

## Возможности

- Асинхронная загрузка страниц с ограничением числа одновременных запросов, общего и для каждого домена
- Обход в ширину с приоритетом по глубине, ограничение глубины и числа страниц, учёт кратчайшей глубины
- Фильтры адресов: только стартовые домены, исключающие и включающие регулярные выражения
- Разбор HTML: заголовок, текст, ссылки, метаданные, изображения, заголовки h1–h3, таблицы, списки
- Вежливость: `robots.txt` и `Crawl-delay`, ограничение запросов в секунду на домен, минимальная пауза и случайный разброс, свой `User-Agent`
- Обработка ошибок: классификация (временные, постоянные, сетевые, ошибки разбора), повторы с экспоненциальной задержкой, отдельные правила для 429 и 5xx, таймауты, растущие с каждой попыткой, Circuit Breaker для проблемных доменов
- Хранилища: JSON (JSON Lines или массив), CSV, SQLite, запись пачками, повторы при ошибках сохранения без дублей, включая финальную запись при закрытии
- Загрузка адресов из `sitemap.xml`, включая индексные карты
- Статистика обхода, экспорт в JSON и HTML-отчёт с таблицами и диаграммами
- Конфигурация в YAML, командная строка, логирование в файл с ротацией, прогресс в реальном времени

## Структура проекта

| Файл | Назначение |
|---|---|
| `crawler.py` | Модуль со всеми классами и командной строкой |
| `crawler.ipynb` | Демонстрации и тесты по дням, тесты производительности |
| `config.yaml` | Пример конфигурации |
| `demo.py` | Полный пример использования `AdvancedCrawler` |
| `requirements.txt` | Зависимости |

## Установка

Нужен Python 3.10 или новее.

```bash
git clone https://github.com/NadezdaKir/async_crawler.git
cd async_crawler
pip install -r requirements.txt
```

## Быстрый старт

### Командная строка

```bash
python crawler.py --urls https://quotes.toscrape.com/ --max-pages 100 --output results.json
```

| Параметр | Описание |
|---|---|
| `--urls` | Стартовые URL через пробел |
| `--max-pages` | Максимальное количество страниц |
| `--max-depth` | Максимальная глубина обхода |
| `--output` | Файл результатов, тип хранилища выбирается по расширению: `.json`, `.csv`, `.db` или `.sqlite` |
| `--config` | Путь к YAML конфигу |
| `--respect-robots` | Соблюдать `robots.txt` |
| `--rate-limit` | Запросов в секунду к одному домену |

Параметры командной строки перекрывают значения из конфига, поэтому основные настройки удобно держать в файле, а разовые поправки передавать в команде:

```bash
python crawler.py --config config.yaml --max-pages 10 --output results.csv
python crawler.py --help
```

### Запуск по конфигу

```bash
python demo.py
```

`demo.py` читает `config.yaml`, обходит сайт, печатает итог и сохраняет HTML-отчёт в `report.html`.

### Использование из кода

```python
import asyncio

from crawler import AdvancedCrawler


async def main():
    crawler = AdvancedCrawler.from_config("config.yaml")

    await crawler.crawl()

    stats = crawler.get_stats()
    print(f"Обработано: {stats['total_pages']} страниц")
    print(f"Успешно: {stats['successful']}")
    print(f"Ошибок: {stats['failed']}")

    crawler.export_to_html_report("report.html")
    await crawler.close()


asyncio.run(main())
```

В Jupyter цикл событий уже запущен, поэтому вместо `asyncio.run(main())` пишут `await main()`.

Стартовые адреса и параметры обхода можно передать и напрямую, они перекроют конфиг:

```python
crawler = AdvancedCrawler()
await crawler.crawl(["https://quotes.toscrape.com/"], max_pages=20, same_domain_only=True)
```

Обход по `sitemap.xml`:

```python
results = await crawler.crawl_sitemap("https://example.com/sitemap.xml", max_pages=50)
```

Базовый `AsyncCrawler` без конфига:

```python
from crawler import AsyncCrawler, JSONStorage

crawler = AsyncCrawler(
    max_concurrent=5,
    max_depth=2,
    requests_per_second=2,
    respect_robots=True,
    max_retries=2,
    storage=JSONStorage("results.json"),
)
results = await crawler.crawl(["https://quotes.toscrape.com/"], max_pages=30)
await crawler.close()
```

## Руководство по конфигурации

Конфиг состоит из пяти разделов. Указывать можно только то, что отличается от значений по умолчанию. Неизвестные разделы и параметры не применяются, а в лог пишется предупреждение, так опечатка не потеряется молча.

```yaml
start_urls:
  - https://quotes.toscrape.com/

crawler:
  max_concurrent: 5
  max_depth: 2
  max_pages: 30
  rate_limit: 2
  per_domain_limit: 2
  respect_robots: true
  user_agent: AsyncCrawler/1.0
  min_delay: 0.0
  jitter: 0.1
  max_retries: 2

filters:
  same_domain_only: true
  exclude_patterns:
    - /login
  include_patterns: []

storage:
  type: json
  path: results.json

logging:
  log_file: crawler.log
  console_level: WARNING
  file_level: DEBUG
  max_bytes: 1000000
  backup_count: 3
```

### `start_urls`

Список стартовых адресов. По умолчанию пустой, тогда адреса нужно передать в `crawl()` или через `--urls`.

### `crawler`

| Параметр | По умолчанию | Описание |
|---|---|---|
| `max_concurrent` | `10` | Сколько страниц загружается одновременно |
| `max_depth` | `2` | Глубина обхода от стартовых адресов |
| `max_pages` | `100` | Максимум страниц за обход |
| `rate_limit` | нет ограничения | Запросов в секунду к одному домену |
| `per_domain_limit` | как `max_concurrent` | Одновременных запросов к одному домену |
| `respect_robots` | `false` | Соблюдать `robots.txt` и `Crawl-delay` |
| `user_agent` | `AsyncCrawler/1.0` | Заголовок `User-Agent` и имя для правил `robots.txt` |
| `min_delay` | `0.0` | Минимальная пауза между запросами к домену, сек |
| `jitter` | `0.0` | Случайная добавка к паузе, сек |
| `max_retries` | `0` | Повторов при временных и сетевых ошибках |

### `filters`

| Параметр | По умолчанию | Описание |
|---|---|---|
| `same_domain_only` | `false` | Не переходить на домены, которых нет среди стартовых |
| `exclude_patterns` | `[]` | Регулярные выражения, подходящие адреса пропускаются |
| `include_patterns` | `[]` | Если список не пуст, обходятся только подходящие адреса |

### `storage`

| Параметр | По умолчанию | Описание |
|---|---|---|
| `type` | нет | `json`, `csv` или `sqlite`, без типа результаты не сохраняются |
| `path` | нет | Путь к файлу результатов |

Каждая запись содержит поля `url`, `title`, `text`, `links`, `metadata`, `crawled_at`, `status_code`, `content_type`. В JSON и CSV списки и словари записываются как JSON, в SQLite данные лежат в таблице `pages` с уникальным `url` и индексами по статусу и дате.

### `logging`

| Параметр | По умолчанию | Описание |
|---|---|---|
| `log_file` | `crawler.log` | Файл лога, `null` отключает запись в файл |
| `console_level` | `INFO` | Уровень сообщений на экране |
| `file_level` | `DEBUG` | Уровень сообщений в файле |
| `max_bytes` | `1000000` | Размер файла, после которого начинается новый |
| `backup_count` | `3` | Сколько старых файлов хранить |

## Описание API

### Итоговый краулер

**`AdvancedCrawler(config: dict | None = None)`** объединяет все компоненты. Наследует `AsyncCrawler`.

| Метод | Описание |
|---|---|
| `AdvancedCrawler.from_config(path)` | Создать краулер по YAML файлу |
| `await crawl(start_urls=None, max_pages=None, **filters)` | Обход, недостающие параметры берутся из конфига. Возвращает словарь `{url: данные страницы}` |
| `await crawl_sitemap(sitemap_url, max_pages=100, **filters)` | Обход адресов из `sitemap.xml` |
| `get_stats()` | Сводка: `total_pages`, `successful`, `failed`, `blocked`, `success_rate`, `duration`, `avg_speed`, `status_codes`, `top_domains`, `errors_by_type` |
| `export_to_json(filename)` | Сохранить статистику в JSON |
| `export_to_html_report(filename)` | Сохранить HTML-отчёт с карточками, таблицами и диаграммами |
| `get_error_stats()` | Ошибки по типам, число повторов, среднее время повтора |
| `await close()` | Дописать буфер на диск с повторами, закрыть хранилище и HTTP-сессию. Если запись так и не удалась, бросает `StorageError` |

**`AsyncCrawler(...)`** базовый краулер. Основные параметры: `max_concurrent`, `max_depth`, `per_domain_limit`, `requests_per_second`, `respect_robots`, `user_agent`, `min_delay`, `jitter`, `max_retries`, `backoff_base`, `retry_strategy`, `circuit_breaker`, `storage`, `save_retries`, `connect_timeout`, `read_timeout`, `total_timeout`, `timeout_growth`. Методы: `crawl`, `crawl_sitemap`, `fetch_url`, `fetch_urls`, `fetch_and_parse`, `get_error_stats`, `close`.

### Компоненты

| Класс | Назначение |
|---|---|
| `HTMLParser` | `await parse_html(html, url)` возвращает `url`, `title`, `text`, `links`, `metadata`, `images`, `headings`, `tables`, `lists` |
| `CrawlerQueue` | Очередь с приоритетами и защитой от повторов: `add_url`, `get_next`, `mark_processed`, `mark_failed`, `get_stats` |
| `SemaphoreManager` | Общий лимит и лимит на домен: `async with acquire(url)`, `get_active` |
| `RateLimiter` | Ограничение частоты запросов на домен с паузой и разбросом: `await acquire(domain)`, `get_stats` |
| `RobotsParser` | Загрузка и кэш `robots.txt`: `await fetch_robots(url)`, `can_fetch`, `get_crawl_delay` |
| `RetryStrategy` | Повторы с экспоненциальной задержкой: `await execute_with_retry(coro, *args)`, `get_stats` |
| `CircuitBreaker` | Временная блокировка домена при большой доле ошибок: `allow_request`, `record_success`, `record_failure`, `get_state` |
| `SitemapParser` | `await fetch_sitemap(url)` возвращает список адресов, индексные карты обходятся рекурсивно |
| `CrawlerStats` | Статистика обхода: `record_success`, `record_failure`, `record_blocked`, `get_stats`, `export_to_json`, `export_to_html_report` |
| `DataStorage` | Абстрактное хранилище с методами `save(data)` и `close()` |
| `JSONStorage(path, pretty=False)` | JSON Lines или массив при `pretty=True` |
| `CSVStorage(path)` | CSV в кодировке `utf-8-sig`, открывается в Excel |
| `SQLiteStorage(path)` | База SQLite через `aiosqlite` |

### Ошибки

`CrawlerError` базовый класс. Наследники: `TransientError` (таймауты, 429, 5xx, повторяются), `PermanentError` (4xx, не повторяются), `NetworkError` (сбои соединения, повторяются), `ParseError` (ошибки разбора), `CircuitOpenError` (домен временно заблокирован), `StorageError` (запись в хранилище не удалась после всех повторов). Функция `classify_error(error, url)` превращает исключение `aiohttp` или `asyncio` в один из этих классов.

### Функции

| Функция | Описание |
|---|---|
| `load_config(path)` | Прочитать YAML и наложить его на `DEFAULT_CONFIG` |
| `create_storage(settings)` | Создать хранилище по разделу `storage` |
| `setup_logging(log_file, console_level, file_level, max_bytes, backup_count)` | Логирование на экран и в файл с ротацией |

## Мониторинг и логи

Во время обхода раз в секунду печатается строка прогресса:

```
[████████████████░░░░]  80% (8/10) | Скорость: 2.7 стр/сек | Осталось: ~1 сек | Активных задач: 0 | В очереди: 41 | Ошибок: 0 | Заблокировано: 0 | Запросов: 2.7 req/sec | Средняя задержка: 0.50 сек
```

Процент считается от меньшего из двух чисел, лимита страниц или найденных адресов, поэтому маленький сайт заканчивается на 100%. Подробный лог с полной датой пишется в `crawler.log`, при достижении `max_bytes` файл переименовывается в `crawler.log.1` и начинается новый.

## Производительность

Тесты в разделе «Тестирование производительности» ноутбука. Сайт смоделирован: каждая страница отвечает за 0.05 сек, разбор HTML настоящий. Так сравнение не зависит от сети и не нагружает чужие сайты. Замеры на MacBook Pro, Python 3.14.

**Сравнение с синхронным краулером, 100 страниц**

| Краулер | Время |
|---|---|
| Синхронный | 5.39 сек |
| Асинхронный, 20 задач | 0.49 сек |

Ускорение в 11 раз: синхронный ждёт ответы по очереди, асинхронный ждёт 20 ответов одновременно.

**Масштабируемость и память, 20 задач**

| Страниц | Время | Страниц в секунду | Пик памяти |
|---|---|---|---|
| 100 | 0.47 сек | 212 | 0.9 МБ |
| 500 | 1.53 сек | 326 | 2.8 МБ |
| 1000 | 2.86 сек | 349 | 4.9 МБ |

Память растёт линейно, около 5 КБ на страницу, это результаты обхода в `processed_urls`. Скорость растёт с объёмом, потому что разгон в начале обхода занимает меньшую долю времени.

**Влияние числа одновременных задач, 1000 страниц**

| Задач | Время | Страниц в секунду |
|---|---|---|
| 5 | 10.81 сек | 93 |
| 20 | 2.85 сек | 351 |
| 50 | 1.31 сек | 762 |

**Узкие места.** Профиль `cProfile` показывает, что около 70% времени краулер ждёт событий ввода-вывода, а разбор HTML занимает единицы процентов. Значит, краулер ограничен ожиданием сети, и главный рычаг скорости это `max_concurrent`. На настоящих сайтах его стоит держать умеренным вместе с `rate_limit`, чтобы не перегружать сервер.

Найдено и исправлено одно узкое место. Когда очередь пуста, а другие страницы ещё загружаются, воркер раньше спал фиксированные 0.1 сек и пропускал появление новых ссылок. Теперь `CrawlerQueue.get_next(timeout=0.1)` ждёт появления адреса и просыпается сразу. Это ускорило небольшие обходы примерно на 20% (100 страниц за 0.44 сек вместо 0.54 при тех же условиях), на 1000 страниц выигрыш около 5%, потому что там очередь почти всегда полна.

## Тесты

Тесты лежат в `crawler.ipynb`, в разделах «Тестирование N дня», и работают без сети на подменённых сессиях и выдуманных сайтах. Они проверяют разбор HTML, очередь и глубину, фильтры, отсутствие повторов, ограничение частоты, `robots.txt`, повторы и Circuit Breaker, хранилища и обработку ошибок сохранения. Тесты первого дня обращаются к настоящим сайтам и зависят от сети. Запуск: открыть ноутбук и выполнить Restart, затем Run All.
