import Analysis.analysis as analysis
import Analysis.analysis_date as analysis_date

import sys
import datetime


def main():
    #jump_strategy()
    #after_release_overheat_strategy('2018-03-12')
    prerelease_strategy('2018-03-12')


def prerelease_strategy(date):
    old_stdout = sys.stdout

    file_name = 'prerelease_strategy.{day}'.format(day=datetime.datetime.now().strftime('%Y%m%d'))

    log_file = open('C:\\Users\\Jianbo\\{file}.log'.format(file=file_name), "w")

    start_date = date
    end_date = analysis_date.next_business_dates(n=5)

    while analysis_date.convert_str_to_date(start_date) < end_date:

        str = analysis.find_interesting_prerelease_ticker(analysis_date.convert_str_to_date(start_date))
        sys.stdout = log_file

        print('------------Pay attention to the following tickers on date = {date}----------------'
              .format(date=start_date))
        print(str)

        log_file.flush()
        print('\n')
        print('\n')
        sys.stdout = old_stdout

        start_date = analysis_date.convert_date_to_str(analysis_date.next_business_dates(datestr=start_date, n=1))
        print('\n')
        print('\n')

    log_file.close()


def after_release_overheat_strategy(date ):

    old_stdout = sys.stdout

    file_name = 'after_jump_strategy.{day}'.format(day=datetime.datetime.now().strftime('%Y%m%d'))

    log_file = open('C:\\Users\\Jianbo\\{file}.log'.format(file=file_name), "w")

    start_date = date
    end_date = analysis_date.next_business_dates(datestr=start_date, n=5)

    while analysis_date.convert_str_to_date(start_date) <= end_date:
        tickers = analysis.analysis_after_release_large_gap_ticker(start_date)

        sys.stdout = log_file

        print('------------Pay attention to the following tickers on date = {date}----------------'
              .format(date=start_date))
        print(tickers)
        for ticker in tickers:
            analysis.analysis_ticker_release_info(ticker)

        log_file.flush()
        print('\n')
        print('\n')
        sys.stdout = old_stdout

        start_date = analysis_date.convert_date_to_str(analysis_date.next_business_dates(datestr=start_date, n=1))
        print('\n')
        print('\n')

    log_file.close()


def jump_strategy():

    old_stdout = sys.stdout

    file_name = 'release_jump_strategy.{day}'.format(day=datetime.datetime.now().strftime('%Y%m%d'))

    log_file = open('C:\\Users\\Jianbo\\{file}.log'.format(file=file_name), "w")

    start_date = '2018-03-12'
    end_date = analysis_date.next_business_dates(datestr=start_date, n=7)

    while analysis_date.convert_str_to_date(start_date) <= end_date:
        tickers = analysis.analysis_release_ticker_earnings(start_date)

        sys.stdout = log_file

        print('------------Pay attention to the following tickers on date = {date}----------------'
              .format(date=start_date))
        print(tickers)
        for ticker in tickers:
            analysis.analysis_ticker_release_info(ticker)

        log_file.flush()
        print('\n')
        print('\n')
        sys.stdout = old_stdout

        start_date = analysis_date.convert_date_to_str(analysis_date.next_business_dates(datestr=start_date, n=1))
        print('\n')
        print('\n')

    log_file.close()










if __name__ == '__main__':
    main()