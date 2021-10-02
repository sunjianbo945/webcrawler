import datetime

from Model.model import ValuationMeasures, StockPriceHistory, ShareStatistics, StockProfitability, \
    StockIncomeStatment, StockBalanceSheet, StockCashFlowStatement, StockDividendsAndSplits, StockFundamentalStats, \
    StockBasicInfo

import time
from DataLoading.crud import select_all_loading_minute_tickers as select_minute_ticker, merge_into_stock_basic_info, \
    select_all_fundamental_tickers
from DataLoading.crud import merge_many_into_stock_min_data as merge_many_data
from DataLoading.crud import merge_many_into_stock_fundamental_data as merge_many_fundamental_data
from DataLoading.crud import select_all_tickers
from selenium import webdriver
import logging


'''
Yahoo Finance modules table summary : 
assetProfile
financialData
defaultKeyStatistics
calendarEvents
incomeStatementHistory
cashflowStatementHistory
balanceSheetHistory
'''

logger = logging.getLogger('yahoo_page')
hdlr = logging.FileHandler('C:\\Users\\js799\\yahoo_page.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.WARNING)


def get_ticker_stats(ticker):


    url = "https://query2.finance.yahoo.com/v10/finance/quoteSummary/{ticker}?formatted=true&crumb=8ldhetOu7RJ&" \
          "lang=en-US&region=US&modules=defaultKeyStatistics%2CfinancialData%2CcalendarEvents&corsDomain=finance.yahoo.com"\
        .format(ticker=ticker)

    summary_detial_url = "https://query2.finance.yahoo.com/v10/finance/quoteSummary/" \
                         "{ticker}?formatted=true&crumb=8ldhetOu7RJ&lang=en-US&region=US&" \
                         "modules=summaryDetail&corsDomain=finance.yahoo.com".format(ticker=ticker)

    headers = {'User-Agent': 'Mozilla/5.0'}

    htmlfile = requests.get(url, headers=headers)

    data_statistics = htmlfile.json()

    htmlfile = requests.get(summary_detial_url, headers=headers)

    data_summary = htmlfile.json()

    if data_statistics['quoteSummary']['error'] is not None:
        return None

    default_key_statistics = data_statistics['quoteSummary']['result'][0]['defaultKeyStatistics']

    financial_data = data_statistics['quoteSummary']['result'][0]['financialData']

    summary_detail = data_summary['quoteSummary']['result'][0]['summaryDetail']


    valuation_measures = ValuationMeasures(ticker=ticker,
                                           market_cap=summary_detail.get('marketCap'),
                                           enterprise_value=default_key_statistics.get('enterpriseValue'),
                           trailing_pe=summary_detail.get('trailingPE'),
                           forward_pe=summary_detail.get('forwardPE'),
                           peg_ratio_5y=default_key_statistics.get('pegRatio'),
                           price_to_sale=summary_detail.get('priceToSalesTrailing12Months'),
                           price_to_book=default_key_statistics.get('priceToBook'),
                           enterprise_revenue=default_key_statistics.get('enterpriseToRevenue'),
                           enterprise_ebitda=default_key_statistics.get('enterpriseToEbitda'))

    stock_price_history = StockPriceHistory(ticker=ticker,
                                            beta=default_key_statistics.get('beta'),
                                            high_52_week=summary_detail.get('fiftyTwoWeekHigh'),
                                            low_52_week=summary_detail.get('fiftyTwoWeekLow')
                                            )

    share_stats = ShareStatistics(ticker=ticker,
                                  avg_volume_3m=summary_detail.get('averageVolume'),
                                  avg_volume_10d=summary_detail.get('averageVolume10days'),
                                  share_outstanding=default_key_statistics.get('sharesOutstanding'),
                                  hold_insiders=default_key_statistics.get('heldPercentInsiders'),
                                  hold_inst=default_key_statistics.get('heldPercentInstitutions'),
                                  shares_short=default_key_statistics.get('sharesShort'),
                                  short_ratio=default_key_statistics.get('shortRatio'),
                                  shares_short_prev_m=default_key_statistics.get('sharesShortPriorMonth'))

    stock_profitability = StockProfitability(ticker=ticker,
                                             profit_margin=financial_data.get('profitMargins'),
                                             operating_margin=financial_data.get('operatingMargins'),
                                             ret_asset=financial_data.get('returnOnAssets'),
                                             ret_equity=financial_data.get('returnOnEquity'))

    stock_income_statement = StockIncomeStatment(ticker=ticker, revenue=financial_data.get('totalRevenue'),
                                                 revenue_per_share=financial_data.get('revenuePerShare'),
                                                 quarterly_revenue_growth=financial_data.get('revenueGrowth'),
                                                 gross_profit=financial_data.get('grossProfits'),
                                                 ebitda=financial_data.get('ebitda'),
                                                 net_income_avi_to_common=default_key_statistics.get('netIncomeToCommon'),
                                                 trailing_eps=default_key_statistics.get('trailingEps'),
                                                 forward_eps=default_key_statistics.get('forwardEps'),
                                                 quarterly_earnings_growth=default_key_statistics.get('earningsQuarterlyGrowth'))

    stock_balance_sheet = StockBalanceSheet(ticker=ticker,
                                            total_cash=financial_data.get('totalCash'),
                                            total_cash_per_share=financial_data.get('totalCashPerShare'),
                                            total_debt=financial_data.get('totalDebt'),
                                            total_debt_per_equity=financial_data.get('debtToEquity'),
                                            current_ratio=financial_data.get('currentRatio'),
                                            book_value_per_share=default_key_statistics.get('bookValue'))

    cash_flow_statement = StockCashFlowStatement(ticker=ticker,
                                                 operating_cash_flow=financial_data.get('operatingCashflow'),
                                                 levered_free_cash_flow=financial_data.get('freeCashflow'))

    stock_dividend_split = StockDividendsAndSplits(ticker=ticker,
                                                   forward_dividend_rate=summary_detail.get('dividendRate'),
                                                   forward_dividend_yield=summary_detail.get('dividendYield'),
                                                   trailing_dividend_rate=summary_detail.get('trailingAnnualDividendRate'),
                                                   trailing_dividend_yield=summary_detail.get('trailingAnnualDividendYield'),
                                                   avg_dividend_yield_5y=summary_detail.get('fiveYearAvgDividendYield'),
                                                   payout_ratio=summary_detail.get('payoutRatio'),
                                                   dividend_date=data_statistics['quoteSummary']['result'][0]['calendarEvents'].get('dividendDate'),
                                                   ex_dividend_date=summary_detail.get('exDividendDate'))

    return StockFundamentalStats(valuation_measures=valuation_measures,
                                 stock_price_history=stock_price_history,
                                 share_stats=share_stats,
                                 stock_profitability=stock_profitability,
                                 stock_income_statement=stock_income_statement,
                                 stock_balance_sheet=stock_balance_sheet,
                                 cash_flow_statement=cash_flow_statement,
                                 stock_dividend_split=stock_dividend_split)




def get_ticker_previous_min_data(position, cur_data, data_list):
    if cur_data is not None:
        return cur_data

    try:
        for i in range(position-1, -1, -1):
            ret_data = data_list[i]
            if ret_data is not None:
                return ret_data
    except IndexError as e:
        print(e)
        print("position = {0}".format(position))
        raise

    return 0


def load_daily_minute_ticker_data():

    logger = logging.getLogger('yahoo_page_stock_min_data')
    hdlr = logging.FileHandler('C:\\Users\\Jianbo\\yahoo_page_stock_min_data.log')
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)
    logger.setLevel(logging.WARNING)

    tickers = select_minute_ticker()
    program_start_time = time.time()
    for ticker in tickers:
        try:
            if ticker == 'BCR':
                continue
            print('Start loading ticker = {0}'.format(ticker))
            start_time = time.time()
            data = get_ticker_min_data(ticker)
            merge_many_data(data)
            print('finish loading ticker = {0},'
                  ' and {1} data has been loaded'
                  ' and it totally takes {2}'.format(ticker, len(data), time.strftime("%H:%M:%S", time.gmtime(
                time.time() - start_time))))
        except:
            logging.info('ticker = {ticker} is bad'.format(ticker=ticker))
    print('The whole load_daily_minute_ticker_data program takes {0}'.format(time.strftime("%H:%M:%S", time.gmtime(
        time.time() - program_start_time))))
    # logger.log('The whole load_daily_minute_ticker_data program takes {0}'.format(time.strftime("%H:%M:%S", time.gmtime(
    #     time.time() - program_start_time))))


def load_ticker_fundamental_data(date=datetime.datetime.today().strftime('%Y-%m-%d')):

    logger = logging.getLogger('yahoo_page_fundamental')
    hdlr = logging.FileHandler('C:\\Users\\js799\\yahoo_page_fundamental.log')
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)
    logger.setLevel(logging.WARNING)

    tickers = select_all_fundamental_tickers(date)
    program_start_time = time.time()
    for ticker in tickers:
        print('Start loading ticker = {0}'.format(ticker))
        start_time = time.time()
        try:
            data = get_ticker_stats(ticker)
            merge_many_fundamental_data(data)
            print('finish loading ticker = {0},'
               ' and it totally takes {1}'.format(ticker, time.strftime("%H:%M:%S", time.gmtime(time.time() - start_time))))
        except:
            print('ticker = {ticker} load failed'.format(ticker=ticker))
            #logger.log('ticker = {ticker} load failed'.format(ticker=ticker))

    print('The whole load_ticker_fundamental_data program takes {0}'.format(time.strftime("%H:%M:%S", time.gmtime(
        time.time() - program_start_time))))
    # logger.log('The whole load_ticker_fundamental_data program takes {0}'.format(time.strftime("%H:%M:%S", time.gmtime(
    #     time.time() - program_start_time))))


def load_ticker_sector_info(ticker=None):

    if ticker is None:
        return None

    browser = None

    success = True

    try:

        url = "https://finance.yahoo.com/quote/{ticker}/profile?p={ticker}".format(ticker=ticker)

        if browser is None:
            browser = webdriver.Chrome()

        browser.get(url)

        company_name = browser.find_element_by_xpath("//div[@id='Col1-0-Profile-Proxy']/section/div/div").text

        company_name = company_name.split('\n')[0]

        table = browser.find_element_by_xpath("//div[@id='Col1-0-Profile-Proxy']/section/div/div/div")

        parts = table.find_elements_by_xpath('.//p')

        sector_info = parts[1].text.split('\n')

        sector = sector_info[0].split(':')[1]

        sub_sector = sector_info[1].split(':')[1]

        stocker = StockBasicInfo(ticker, company_name, sector, sub_sector)
        merge_into_stock_basic_info(stocker)
    except :
        print(ticker + " does not have good data")
        return None
    finally:
        if browser is not None:
            browser.quit()
        return success

def load_all_tickers_sector_info():
    tickers = select_all_tickers()
    for ticker in tickers:
        print(ticker + ' starts')
        good = load_ticker_sector_info(ticker)
        if good is None:
            logger.info('{ticker} does not work'.format(ticker=ticker))
        print(ticker + ' ends')

def main():

    # parser = argparse.ArgumentParser()
    #
    # parser.add_argument('-g', action='store', dest='goal',
    #                     help='the goal of this run')
    #
    # results = parser.parse_args()
    #
    # if results.goal == 'return':
    #     load_daily_minute_ticker_data()
    # else:
    #     load_ticker_fundamental_data()


    #load_ticker_fundamental_data()

    load_all_tickers_sector_info()


if __name__ == '__main__':
    main()
