import re
import json
import logging
import gzip
import os
from time import time
import shutil
import concurrent.futures


# Estimate time of program execution
def time_dec(original_func):
    def wrapper(*args):
        start = time()
        res = original_func(*args)
        end = time()
        dif = end - start
        print(f'function {original_func.__name__} executed in {dif} second')  # visualize process
        info = f'function {original_func.__name__} executed in {dif} second'
        return res

    return wrapper


# Creating logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)
foramtter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
file_handler = logging.FileHandler('nginx_data.log')
file_handler.setFormatter(foramtter)
logger.addHandler(file_handler)


# supplemental class to store functions and data
class UrlStat:
    _all_time_counter = 0  # Whole time of urls in nginx-access-ui.log-20170630
    _all_url_counter = 0  # Number of url's in nginx-access-ui.log-20170630

    def __init__(self, url, req_time):
        self.url = url
        self.all_time = 0
        self.time = req_time  # total request_time for a given URL, absolute value
        self.samples = []  # samples fot a given URL [0.33, 0.11....0,9]
        self.freq = 0

    # Collecting time samples for median calculating,
    # Count total execution time of all given URLs
    def add_time(self, req_time):
        self.samples.append(req_time)
        UrlStat._all_time_counter += req_time

    # median of request_time for a given URL
    def time_med(self):
        self.samples.sort()
        if len(self.samples) % 2 == 1:
            return self.samples[len(self.samples) // 2]
        else:
            return 0.5 * (self.samples[len(self.samples) // 2 - 1] + self.samples[len(self.samples) // 2])

    # maximum time of execution for given url
    def time_max(self):
        return max(self.samples)

    # Relative frequency of URL
    def freq_rel(self):
        return (self.freq / self._all_url_counter) * 100

    # Count times URL occurs in nginx-access-ui.log-20170630
    def count_freq(self):
        self.freq += 1
        UrlStat._all_url_counter += 1

    # total request_time for a given URL,
    # relative to the total request_time of all
    # requests
    def time_perc(self):
        return float(self.time / self._all_time_counter) * 100

    # average request_time for a given URL
    def time_avg(self):
        return self.time / self.freq


@time_dec
def log_analyzer(file_nginx):
    with gzip.open(file_nginx, 'rt') as info_nginx:
        url_time_pattern = re.compile(".+?(GET|POST|PUT|DELETE|HEAD|CONNECT|OPTIONS|TRACE)"
                                      "(?P<url_short>(.+?))(\?|HTTP).+ (?P<exec_time>[\d.]+)")
        # Example:
        # 1.196.116.32 - - # [29 / Jun / 2017: 03:50: 22 + 0300] "GET /api/v2/banner/25019354 HTTP/1.1" #200 927
        # "-" "Lynx/2.8.8dev.9 libwww-FM/2.14 SSL-MM/1.4.1 GNUTLS/2.10.5"
        # "-" "1498697422-2190034393-4708-9752759" "dc7161be3" 0.390

        # Searched Groups are:
        # 1) url_short == /api/v2/banner/25019354 -url_short
        # 2) exec_time == 0.390

        url_vals = {}  # /api/v2/slot/4822/groups : UrlStat
        # UrlStat : 0.390(exec_time), 1(frequency)
        for idx, line in enumerate(info_nginx):
            url_srch = re.search(url_time_pattern, line)
            time_srch = re.search(url_time_pattern, line)
            if url_srch is None:
                logger.warning(f'Tired to execute Line idx = {idx}; '
                               f'Line :{line}; url_short not found')
                continue
            url_short = url_srch.group('url_short')  # /api/v2/banner/25019354
            exc_time = float(time_srch.group('exec_time'))  # 0.390

            if url_short not in url_vals:
                us = UrlStat(url_short, exc_time)
                us.add_time(exc_time)
                us.count_freq()
                url_vals[url_short] = us
            else:
                url_stat = url_vals[url_short]
                url_stat.add_time(exc_time)
                url_stat.count_freq()
        return url_vals


@time_dec
def build_report(url_vals, log_path):
    # generate json data
    data = []
    for url, stats in url_vals.items():
        data.append({"count": stats.freq,
                     "time_avg": "%.3f" % stats.time_avg(),
                     "time_max": "%.3f" % stats.time_max(),
                     "time_sum": stats.time,
                     "url": url,
                     "time_med": "%.3f" % stats.time_med(),
                     "time_perc": "%.4f" % stats.time_perc(),
                     "count_perc": "%.5f" % stats.freq_rel()})

    data.sort(key=lambda item: item["time_sum"], reverse=True)

    table_json_text = json.dumps(data)

    with open("report.html", 'r') as rtf:
        report_text = rtf.read()
        report_text = report_text.replace("$table_json", table_json_text)

    os.makedirs(os.path.dirname(log_path), exist_ok=True)  # create intermediate directories
    with open(report_path, "w") as rf:
        rf.write(report_text)


def write_to_file(logs):
    if os.path.isfile('logs_result_threaded_new.txt'):

        with open(r'logs_result_threaded_new.txt', 'a') as lg_thread:
            for log in logs:
                lg_thread.write(log)
                lg_thread.write('\n')
                # just to make sure data is written correctly
            lg_thread.write(
                '__This line determines, that data from next data file has been written or end of the file__')
            lg_thread.write('\n')
    else:
        with open(r'logs_result_threaded_new.txt', 'w') as lg_thread:
            for log in logs:
                lg_thread.write(log)
                lg_thread.write('\n')
                # just to make sure data is written correctly
            lg_thread.write(
                '__This line determines, that data from next data file has been written or end of the file__')
            lg_thread.write('\n')


def concurrent_execute(path_lst):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        result = [executor.submit(log_analyzer, log_file) for log_file in path_lst]
        for f in concurrent.futures.as_completed(result):
            write_to_file(f.result())


path_lst = []
for i in range(10):
    path_lst.append(f'nginx-access-ui.log{i}.gz')
    shutil.copyfile('nginx-access-ui.log.gz', f'nginx-access-ui.log{i}.gz')

concurrent_execute(path_lst)


# path = [r'nginx-access-ui.log.gz', r'nginx-access-ui.log-2.gz', r'nginx-access-ui.log-3.gz']
# report_path = r'/home/driver220/log_reports/report_ver2.html'
# concurrent_execute(path)
# url_vals = log_analyzer(path)
# print(url_vals)
# build_report(url_vals, report_path)
