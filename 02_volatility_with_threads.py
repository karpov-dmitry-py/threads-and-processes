# -*- coding: utf-8 -*-

# Задача: вычислить 3 тикера с максимальной и 3 тикера с минимальной волатильностью в МНОГОПОТОЧНОМ стиле
#
# Бумаги с нулевой волатильностью вывести отдельно.
# Результаты вывести на консоль в виде:
#   Максимальная волатильность:
#       ТИКЕР1 - ХХХ.ХХ %
#       ТИКЕР2 - ХХХ.ХХ %
#       ТИКЕР3 - ХХХ.ХХ %
#   Минимальная волатильность:
#       ТИКЕР4 - ХХХ.ХХ %
#       ТИКЕР5 - ХХХ.ХХ %
#       ТИКЕР6 - ХХХ.ХХ %
#   Нулевая волатильность:
#       ТИКЕР7, ТИКЕР8, ТИКЕР9, ТИКЕР10, ТИКЕР11, ТИКЕР12
# Волатильности указывать в порядке убывания. Тикеры с нулевой волатильностью упорядочить по имени.

import os.path
import time
import threading

class Parser(threading.Thread):

    def __init__(self, source, stats, lock, encoding='utf8', *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.source = source
        self.stats = stats
        self.lock = lock
        self.encoding = encoding
        self.prices = []

    def run(self):

        with open(self.source, encoding=self.encoding) as source:

            headers_line_index = 1

            for line_index, line in enumerate(source):

                if line_index <= headers_line_index:
                    continue

                try:
                    ticker, date, price, qty = line.strip().split(',')
                except BaseException as exc:
                    print(f'Ошибка чтения данных в строке <{line}>, описание ошибки: {exc.args}')
                    continue

                self.prices.append(float(price))

        if not self.prices:
            return

        self.prices.sort()
        min = self.prices[0]
        max = self.prices[-1]
        avg = (max + min) / 2
        result = (max - min) / avg * 100 if avg else 0
        with self.lock:
            self.stats.append((ticker, result))

def time_track(func):
    def surrogate(*args, **kwargs):
        started_at = time.time()

        result = func(*args, **kwargs)

        ended_at = time.time()
        elapsed = round(ended_at - started_at, 4)
        print(f'Функция работала {elapsed} секунд(ы)')
        return result
    return surrogate

def filter_values(seq, value_wanted=0):
    key, value = seq
    return value == value_wanted

def get_limit_values(seq, limit, skip_zeros=False):

    if not skip_zeros:
        return seq[:limit]

    result = []

    for stats in seq:

        ticker, stat = stats

        if stat == 0:
            continue

        if len(result) >= limit:
            break

        result.append(stats)

    return result

def print_stats(header, stats):
    print(header)
    for ticker, stat in stats:
        print(f'\t{ticker} - {stat:6.2f} %')

@time_track
def main():

    path = os.path.join(os.path.dirname(__file__), 'trades')
    path = os.path.normpath(path)
    stats = []
    files = []
    limit = 3

    for dirpath, dirnames, filenames in os.walk(path):
        for file in filenames:
            file_full_path = os.path.join(dirpath, file)
            files.append(file_full_path)

    lock = threading.RLock()
    parsers = [Parser(file, stats, lock) for file in files]

    for parser in parsers:
        parser.start()

    for parser in parsers:
        parser.join()

    if not stats:
        print('Нет данных для анализа.')
        return

    stats = sorted(stats, key = lambda x: x[1])
    mins = get_limit_values(stats, limit, True)
    mins.sort(key=lambda x: x[1], reverse=True)

    stats.reverse()
    maxs = get_limit_values(stats, limit)

    stats = sorted(stats, key=lambda x: x[0])
    zeros = filter(filter_values, stats)

    print_stats('Максимальная волатильность', maxs)
    print_stats('Минимальная волатильность', mins)
    print('Нулевая волатильность')
    for ticker, stat in zeros:
        print(f'\t{ticker}', sep=' ,')

if __name__ == '__main__':
    main()