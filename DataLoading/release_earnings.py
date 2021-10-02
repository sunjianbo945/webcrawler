from selenium import webdriver
from Model.model import StockEarningData
from DataLoading.crud import merge_into_stock_earings as merge_earning
from Analysis.analysis_date import *
import logging


def load_daily_earnings_info(date):

    row_length = 101

    offset = 0

    browser = None

    success = True

    try:
        while row_length > 100:
            if offset == 0:
                url = "https://finance.yahoo.com/calendar/earnings?day={date}".format(date=date)
            else:
                url = "https://finance.yahoo.com/calendar/earnings?day={date}".format(date=date) + \
                      "&offset={offset}&size=100".format(offset=offset)

            if browser is None:
                browser = webdriver.Chrome()

            browser.get(url)


            table = browser.find_element_by_xpath("//div[@id='fin-cal-table']/div/div/table")

            rows = table.find_elements_by_xpath('.//tr')

            row_length = len(rows)

            for row in rows[1:]:
                ticker = row.find_element_by_xpath('.//td[1]').text
                time = row.find_element_by_xpath('.//td[3]').text
                expect_eps = row.find_element_by_xpath('.//td[4]').text
                actual_eps = row.find_element_by_xpath('.//td[5]').text
                surprise = row.find_element_by_xpath('.//td[6]').text

                print("{ticker},{time},{expect_eps},{actual_eps},{surprise}".format(ticker=ticker
                                                                                    ,time=time
                                                                                    ,expect_eps=expect_eps,
                                                                                    actual_eps=actual_eps,
                                                                                    surprise=surprise))

                stock_earning = StockEarningData(ticker, date, time, expect_eps, actual_eps, surprise)
                merge_earning(stock_earning)


            offset+=100
    except Exception as e:
        print(e)
        print(date + " does not have data")
        if offset == 0:
            success = False
    finally:
        browser.quit()
        return success


def main():

    logger = logging.getLogger('release_earnings')
    hdlr = logging.FileHandler('release_earnings.log')
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)
    logger.setLevel(logging.WARNING)

    start_date = '2019-02-20'

    end_date = next_business_dates(n=225)

    while convert_str_to_date(start_date) < end_date:
        print(start_date)
        attend = 0

        while not load_daily_earnings_info(start_date) and attend < 3:
            attend += 1

        if attend >= 3:
            logger.error('tried more than 3 time for date {d}'.format(d=start_date))
            print('tried more than 3 time for date {d}'.format(d=start_date))
        else:
            print(start_date + " is complete")

        start_date = convert_date_to_str(next_business_dates(datestr=start_date, n=1))


if __name__ == '__main__':
    main()