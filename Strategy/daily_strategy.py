import Analysis.analysis as analysis
import Analysis.analysis_date as analysis_date
import sys
import datetime


def big_plunge_strategy(date):

    old_stdout = sys.stdout

    file_name = 'big_plunge_strategy.{day}'.format(day=datetime.datetime.now().strftime('%Y%m%d'))

    log_file = open('C:\\Users\\Jianbo\\{file}.log'.format(file=file_name), "w")

    sys.stdout = log_file

    print('------------Pay attention to the following tickers on date = {date}----------------'
          .format(date=date))

    info = analysis.analysis_tickers_daily_return_ranks(date)

    for i in info:
        print(i)
        print('\n')

    sys.stdout = old_stdout

    log_file.close()


def main():
    date = analysis_date.next_business_dates()
    big_plunge_strategy(date)


if __name__ == '__main__':
    main()
