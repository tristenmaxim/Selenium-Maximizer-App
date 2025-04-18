# Selenium TikTok Data Automator

Автоматизированный скрипт для загрузки данных TikTok из Maximizer.io, их обработки и публикации в Google Drive и Google Sheets.

## Функциональность

Скрипт выполняет следующие операции:
- Авторизуется на сайте maximizer.io
- Скачивает отчеты по рекламе TikTok за последние 3 дня
- Предварительно обрабатывает и очищает данные
- Архивирует предыдущие отчеты в Google Drive
- Загружает новые данные в Google Drive
- Создает Google Sheet с исходными данными и сводной таблицей
- Форматирует таблицы для лучшей визуализации

## Требования

- Python 3.6+
- Selenium WebDriver
- Pandas
- Google API клиенты (Drive, Sheets)
- Учетные данные сервисного аккаунта Google

## Настройка

1. Установите необходимые зависимости:
```
pip install selenium pandas webdriver-manager gspread google-api-python-client
```

2. Создайте сервисный аккаунт Google и загрузите JSON ключ
3. Настройте константы в начале скрипта (EMAIL, PASSWORD, пути к файлам и ID папок Google Drive)

## Использование

Запустите скрипт:
```
python Unification-script.py
```
