import time
import logging
from DataLoading.crud import select_all_loading_minute_tickers as select_minute_ticker
import requests
from DataLoading.crud import merge_many_into_stock_min_data as merge_many_data
from Model.model import StockMinData


def load_daily_minute_ticker_data():

    # logger = logging.getLogger('yahoo_page_stock_min_data')
    # hdlr = logging.FileHandler('yahoo_page_stock_min_data.log')
    # formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    # hdlr.setFormatter(formatter)
    # logger.addHandler(hdlr)
    # logger.setLevel(logging.WARNING)

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


def get_ticker_min_data(ticker):
    url = "https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range=1d&includePrePost=false&interval=1" \
          "m&corsDomain=finance.yahoo.com&.tsrc=finance".format(ticker=ticker)

    headers = {'User-Agent': 'Mozilla/5.0'}
    html_file = requests.get(url, headers=headers)

    data = html_file.json()

    time_stamps = data['chart']['result'][0].get('timestamp')
    if time_stamps is None:
        return []

    high_datas=data['chart']['result'][0].get('indicators')["quote"][0]["high"]

    low_datas = data['chart']['result'][0].get('indicators')["quote"][0]["low"]

    open_datas = data['chart']['result'][0].get('indicators')["quote"][0]["open"]

    close_datas = data['chart']['result'][0].get('indicators')["quote"][0]["close"]

    volume_datas = data['chart']['result'][0]['indicators']["quote"][0]["volume"]

    if len(time_stamps) ==0 and len(close_datas) == 0:
        raise Exception("time stampe size is {0} and close price size is {1}".format(len(time_stamps), len(close_datas)))

    # return [StockMinData(ticker=ticker, datetime=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time_stamps[i])),
    #     close=get_ticker_previous_min_data(i, close_datas[i], close_datas),
    #     volume=get_ticker_previous_min_data(i, volume_datas[i], volume_datas)) for i in range(len(time_stamps))]

    return [StockMinData(ticker=ticker, datetime=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time_stamps[i])),
                         high=high_datas[i],low=low_datas[i],open= open_datas[i],
                         close=close_datas[i],
                         volume=volume_datas[i]) for i in
            range(len(time_stamps))]


def main():
    load_daily_minute_ticker_data()


if __name__=='__main__':
    main()