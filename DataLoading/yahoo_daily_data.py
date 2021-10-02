from pandas_datareader import data
from pandas_datareader._utils import RemoteDataError
import requests
from Model.model import StockDailyData
from DataLoading.crud import merge_many_into_stock_daily_data as merge_many, select_all_tickers
from DataLoading.crud import merge_many_into_fx_daily_data as merge_fx_many
from DataLoading.crud import select_all_loading_tickers, select_all_fx_currency
import time
import Analysis.analysis_date as analysis_date


def load_daily_instrument_price(tickers, start_date, end_date, data_source='yahoo'):

    """Define the instruments to download. We would like to see Apple, Microsoft and the S&P500 index.
    Reference : https://www.learndatasci.com/tutorials/python-finance-part-yahoo-finance-api-pandas-matplotlib/
    """
    # In case, we query too fast and yahoo refuse the request. remain list will put the ticker do not load correctly
    remain = ['a']
    attend = -1
    while remain:
        if attend > 50:
            break

        remain.clear()
        for ticker in tickers:
            # yahoo do not support '.' ticker.  We need to replace '.' to '-'
            if data_source == 'yahoo':
                query_ticker = ticker.replace('.', '-')

            start_time = time.time()
            print('--------------------------------- Start load {0} -----------------------------'.format(ticker))
            # User pandas_reader.data.DataReader to load the desired data. As simple as that.
            try:
                daily_data = data.DataReader([query_ticker], data_source, start_date, end_date)

                #daily_data = panel_data.minor_xs(ticker)
                filtered_df = daily_data[daily_data['Volume'].notnull()]

                temp = [StockDailyData(ticker=ticker, date=index.date(), open=row["Open"][0], high=row["High"][0],
                                       low=row["Low"][0], close=row["Close"][0], adj_close=row["Adj Close"][0],
                                       volume=row["Volume"][0])
                        for index, row in filtered_df.iterrows()]

                merge_many(temp)

                print('finish load ticker = {0}. Total spend {1}'.format(ticker, time.strftime("%H:%M:%S", time.gmtime(
                                                                             time.time()-start_time))))
            except RemoteDataError as er:
                print(er)
                print(ticker + ' does not have any thing -------------------------------- ')
                remain.append(ticker)
            except ConnectionError as er:
                print(er)
                print(ticker + ' does not have any thing -------------------------------- ')
                remain.append(ticker)
            except Exception as er:
                print(er)
                print(ticker + ' does not have any thing -------------------------------- ')

        tickers = remain.copy()
        attend += 1

    return True


def load_daily_currency_data(fx_id, fx_currency, start_time_stamp, end_time_stamp):
    url = "https://query1.finance.yahoo.com/v8/finance/chart/{currency}=X?symbol=EURUSD%3DX&period1={p1}&" \
          "period2={p2}&interval=1d&includePrePost=true&events=div%7Csplit%7Cearn&corsDomain=finance.yahoo.com"\
        .format(currency=fx_currency, p1=start_time_stamp, p2=end_time_stamp)

    def get_ticker_previous_min_data(position, cur_data, data_list):
        if cur_data is not None:
            return cur_data

        try:
            for i in range(position - 1, -1, -1):
                ret_data = data_list[i]
                if ret_data is not None:
                    return ret_data
        except IndexError as e:
            print(e)
            print("position = {0}".format(position))
            raise

        return 0

    headers = {'User-Agent': 'Mozilla/5.0'}

    htmlfile = requests.get(url, headers=headers)

    fx_currency_data = htmlfile.json()

    time_stamps = fx_currency_data['chart']['result'][0].get('timestamp')
    if time is None:
        return None

    close_rate = fx_currency_data['chart']['result'][0]['indicators']['adjclose'][0]['adjclose']

    return [(fx_id, time.strftime("%Y-%m-%d", time.localtime(time_stamps[i])),
             get_ticker_previous_min_data(i, close_rate[i], close_rate)) for i in range(len(time_stamps))]


def main(tickers=[]):
    # start_date = analysis_date.next_business_dates()
    start_date = analysis_date.convert_str_to_date('2007-01-01')
    # end_date = analysis_date.next_business_dates(n=1)
    end_date = analysis_date.convert_str_to_date()
    if not tickers:
        tickers = select_all_loading_tickers(start_date, end_date)
        #tickers = select_all_tickers()

    load_daily_instrument_price(tickers, start_date, end_date)


def load_fx_currenty(start_date, end_date):
    # load fx_currency part
    fx_currencys = select_all_fx_currency(start_date, end_date)

    start_date_ux_timestampe = int((time.mktime(time.strptime("{0} 00:00:00".format(start_date), "%Y-%m-%d %H:%M:%S"))))
    end_date_ux_timestampe = int(time.mktime(time.strptime("{0} 00:00:00".format(end_date), "%Y-%m-%d %H:%M:%S")))

    for fx in fx_currencys:
        print('--------------------------------- Start load {0} -----------------------------'.format(fx[1]+fx[2]))
        temp = load_daily_currency_data(fx[0], '{0}{1}'.format(fx[1], fx[2]), start_date_ux_timestampe, end_date_ux_timestampe)
        merge_fx_many(temp)
        print('--------------------------------- Finish load {0} -----------------------------'.format(fx[1]+fx[2]))



if __name__ == '__main__':
    main()
