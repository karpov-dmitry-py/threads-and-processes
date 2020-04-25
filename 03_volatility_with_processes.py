# -*- coding: utf-8 -*-

# Задача: вычислить 3 тикера с максимальной и 3 тикера с минимальной волатильностью в МНОГОПРОЦЕССНОМ стиле
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
#

import os.path
import time
from multiprocessing import Process, Pipe

class Parser(Process):

    def __init__(self, files, connection, encoding='utf8', *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.files = files
        self.connection = connection
        self.encoding = encoding

    def run(self):

        results = []

        for file in self.files:

            with open(file, encoding=self.encoding) as source:

                prices = []

                headers_line_index = 1

                for line_index, line in enumerate(source):

                    if line_index <= headers_line_index:
                        continue

                    try:
                        ticker, date, price, qty = line.strip().split(',')
                    except BaseException as exc:
                        print(f'Ошибка чтения данных в строке <{line}>, описание ошибки: {exc.args}')
                        continue

                    prices.append(float(price))

                if not prices:
                    continue

                prices.sort()
                min = prices[0]
                max = prices[-1]
                avg = (max + min) / 2
                file_result = (max - min) / avg * 100 if avg else 0
                results.append((ticker, file_result))

        if not results:
            return

        self.connection.send(results)
        self.connection.close()

def time_track(func):
    def surrogate(*args, **kwargs):
        started_at = time.time()
        result = func(*args, **kwargs)
        ended_at = time.time()
        elapsed = round(ended_at - started_at, 4)
        print(f'Длительность выполнения: {elapsed} сек.')
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
    files = []
    pipes = []
    parsers = []
    stats_limit = 3
    max_processes_limit = 10

    for dirpath, dirnames, filenames in os.walk(path):
        for file in filenames:
            file_full_path = os.path.join(dirpath, file)
            files.append(file_full_path)

    if not files:
        print('Нет данных для анализа.')
        return

    # разобьем файлы на партии с учетом нужного нам максимального кол-ва процессов и передадим каждому процессу свою партию файлов на обработку.
    total_files = len(files)
    num_files_per_process = total_files // max_processes_limit if total_files > max_processes_limit else total_files

    start = 0
    end = num_files_per_process

    if total_files <= max_processes_limit:
        max_processes_limit = 1

    for i in range(max_processes_limit):

        if i == max_processes_limit-1: # последний процесс получит свою партию файлов + все оставшиеся файлы до конца списка файлов
            end = total_files-1

        files_chunk = files[start:end]
        parent_end_conn, child_end_conn = Pipe()
        pipes.append(parent_end_conn)

        parser = Parser(files_chunk, child_end_conn)
        parsers.append(parser)

        start += num_files_per_process
        end += num_files_per_process

    for parser in parsers:
        parser.start()

    for pipe in pipes:
        stats.extend(pipe.recv())

    for parser in parsers:
        parser.join()

    if not stats:
        print('Нет данных для анализа.')
        return

    stats = sorted(stats, key = lambda x: x[1])
    mins = get_limit_values(stats, stats_limit, True)
    mins.sort(key=lambda x: x[1], reverse=True)

    stats.reverse()
    maxs = get_limit_values(stats, stats_limit)

    stats = sorted(stats, key=lambda x: x[0])
    zeros = filter(filter_values, stats)

    print_stats('Максимальная волатильность', maxs)
    print_stats('Минимальная волатильность', mins)
    print('Нулевая волатильность')
    for ticker, stat in zeros:
        print(f'\t{ticker}', sep=' ,')

if __name__ == '__main__':
    main()