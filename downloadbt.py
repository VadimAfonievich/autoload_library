# todo: скачать все zip архивы и разархивировать их в 1 папку в csv формате
import re
import requests
import zipfile
import io
import os
from datetime import timedelta, datetime
from os.path import isfile


class TimeFrame:
    one_sec = "1s"
    one_min = "1m"
    three_min = "3m"
    five_min = "5m"
    fifteen_min = "15m"
    thirty_min = "30m"
    one_hour = "1h"
    two_hour = "2h"
    four_hour = "4h"
    six_hour = "6h"
    eight_hour = "8h"
    twelve_hour = "12h"
    one_day = "1d"


class DownloadBT:
    """качает csv файлы за необходимый диапазон дат, пару и таймфрейм"""
    @classmethod
    def download(cls, start_date, end_date, pair, timeframe, verbose=True, auto_update=True):
        """скачивает архив по ссылке и разархивирует в папку"""
        d = end_date - start_date
        for i in range(d.days + 1):
            file_name = f"{pair}/{timeframe}/{pair}-{timeframe}-{start_date + timedelta(i):%Y-%m-%d}"
            if not isfile(f"data/csv_files/{file_name}.csv"):
                if verbose:
                    print(f"Файла c timeframe: {timeframe} за {start_date + timedelta(i):%Y-%m-%d} "
                          f"нет в папке! Сейчас скачаю!")

                link = f"https://s3-ap-northeast-1.amazonaws.com/data.binance.vision/data/spot/daily/" \
                       f"klines/{file_name}.zip"
                r = requests.get(link)

                if r.status_code == 404 and verbose:
                    print(f"Файла с timeframe: {timeframe} за {start_date + timedelta(i):%Y-%m-%d} еще нет на сайте.")
                    break
                z = zipfile.ZipFile(io.BytesIO(r.content))
                z.extractall(f"data/csv_files/{pair}/{timeframe}")

                if verbose:
                    print(f"Скачан файл за {start_date + timedelta(i):%Y-%m-%d} с таймфреймом: {timeframe}")
            else:
                if verbose:
                    print(f"Файл за {start_date + timedelta(i):%Y-%m-%d} уже имеется!")
        if verbose:
            print(f"Все имеющиеся на сайте файлы с запрашиваемым таймфреймом и диапазон дат скачаны!")

    @classmethod
    def update(cls, pair, timeframe, verbose=True):
        """обновляет данные по указанной паре и таймфрейму, от последних имеющихся на диске до самых новых"""
        directory = f'data/csv_files/{pair}/{timeframe}'
        root, dirs, files = next(os.walk(directory))
        if len(files) == 0:
            if verbose:
                print("Файлов нет! Качаю за предыдущие 7 дней!")

        r = re.search(r"\d\d\d\d-\d\d-\d\d", max(files))
        start_day = datetime.strptime(r.group(0), "%Y-%m-%d")

        cls.download(start_day, datetime.now(), "BTCBUSD", TimeFrame.one_day)

# todo: сделать обноление абсолютно всех файлов по парам (pair)
    @classmethod
    def update_all(cls, verbose=True):
        """обновляет все данные по всем парам и таймфреймам"""
        all_pairs_dir = f'data/csv_files'

        for pair in os.listdir(all_pairs_dir):
            all_pairs = f'data/csv_files/{pair}'
            for timeframes in os.listdir(all_pairs):
                cls.update(pair, timeframes)
        if verbose:
            print("Все файлы по всем парам и таймфреймам обновлены.")



"""Примеры вызова методов класса"""
# DownloadBT.download(datetime(2022, 11, 5), datetime.now(), "BTCBUSD", TimeFrame.thirty_min)
# DownloadBT.update("BTCBUSD", TimeFrame.thirty_min)
# DownloadBT.update_all()
