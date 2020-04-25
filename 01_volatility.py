# -*- coding: utf-8 -*-


# Описание предметной области:
#
# При торгах на бирже совершаются сделки - один купил, второй продал.
# Покупают и продают ценные бумаги (акции, облигации, фьючерсы, етс). Ценные бумаги - это по сути долговые расписки.
# Ценные бумаги выпускаются партиями, от десятка до несколько миллионов штук.
# Каждая такая партия (выпуск) имеет свой торговый код на бирже - тикер - https://goo.gl/MJQ5Lq
# Все бумаги из этой партии (выпуска) одинаковы в цене, поэтому говорят о цене одной бумаги.
# У разных выпусков бумаг - разные цены, которые могут отличаться в сотни и тысячи раз.
# Каждая биржевая сделка характеризуется:
#   тикер ценнной бумаги
#   время сделки
#   цена сделки
#   обьем сделки (сколько ценных бумаг было куплено)
#
# В ходе торгов цены сделок могут со временем расти и понижаться. Величина изменения цен называтея волатильностью.
# Например, если бумага №1 торговалась с ценами 11, 11, 12, 11, 12, 11, 11, 11 - то она мало волатильна.
# А если у бумаги №2 цены сделок были: 20, 15, 23, 56, 100, 50, 3, 10 - то такая бумага имеет большую волатильность.
# Волатильность можно считать разными способами, мы будем считать сильно упрощенным способом -
# отклонение в процентах от средней цены за торговую сессию:
#   средняя цена = (максимальная цена + минимальная цена) / 2
#   волатильность = ((максимальная цена - минимальная цена) / средняя цена) * 100%
# Например для бумаги №1:
#   average_price = (12 + 11) / 2 = 11.5
#   volatility = ((12 - 11) / average_price) * 100 = 8.7%
# Для бумаги №2:
#   average_price = (100 + 3) / 2 = 51.5
#   volatility = ((100 - 3) / average_price) * 100 = 188.34%
#
# Задача: вычислить 3 тикера с максимальной и 3 тикера с минимальной волатильностью.
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

# В каждом файле в папке trades содержится данные по сделакам по одному тикеру, разделенные запятыми.
#   Первая строка - название колонок:
#       SECID - тикер
#       TRADETIME - время сделки
#       PRICE - цена сделки
#       QUANTITY - количество бумаг в этой сделке
#   Все последующие строки в файле - данные о сделках
#

import os.path
import time

class Parser:

    def __init__(self, source, encoding='utf8'):
        self.source = source
        self.encoding = encoding
        self.prices = []
        self.result = None

    def norm_path(self, path, source):
        result = os.path.join(path, source)
        return os.path.normpath(result)

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
        return (ticker, result)

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

    path = os.path.join(os.path.dirname(__file__), r'trades')
    path = os.path.normpath(path)
    stats = []
    limit = 3

    for dirpath, dirnames, filenames in os.walk(path):
        for file in filenames:
            file_full_path = os.path.join(dirpath, file)
            parser = Parser(file_full_path)
            result = parser.run()

            if result:
                stats.append(result)

    if not stats:
        print('Нет данных для анализа.')
        return

    stats = sorted(stats, key = lambda x: x[1])
    mins = get_limit_values(stats, limit, True)
    mins.sort(key=lambda x: x[1], reverse=True) # ранее была неверная сортировка

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