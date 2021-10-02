import Analysis.sql as sql
import json
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import  Analysis.analysis_date as ana_date
import DataLoading.yahoo_daily_data as daily_data
import DataLoading.yahoo_page as page


def get_market_weight():
    df = sql.find_all_tickers_cap()
    df['weight'] = df['market_cap']*1.0/df['market_cap'].sum()
    #sort data
    df = df.set_index('ticker').sort_values(by='weight')

    rank = df.weight.cumsum()

    df_fq = df[rank.apply(lambda x: x < 0.25)]
    #df_fq.index.values
    df_lq = df[rank.apply(lambda x:x > 0.75)]
    return df_fq, df_lq


def get_tickers_return(tickers, start_date, end_date):

    df = sql.get_portfolio_daily_time_series(tickers, start_date, end_date)
    if df is None:
        raise Exception('The DataFrame from get_tickers_return is None')

    num_days = (df.index.values[-1] - df.index.values[0]).days
    year_faction = num_days/365.0
    ret_series = (df.pct_change(len(df.index)-1)*(1/year_faction)).iloc[-1]

    return ret_series


def get_tickers_lowest_opp():
    df = sql.find_ticker_low_position()
    if df is None:
        raise Exception('The DataFrame from get_tickers_return is None')

    df['diff'] = df['close'] - df['lowest']
    df['per_diff'] = df['diff'] / df['lowest']
    df = df.sort_values('per_diff')
    return df


def run_portfolio():
    with open('portfolio.ini') as json_data_file:
        data = json.load(json_data_file)

    portfolio = data.get('portfolio')
    start_date = data.get('start_date')
    end_date = data.get('end_date')

    portfolio_time_series = get_tickers_return(portfolio.keys(), start_date, end_date)
    print(portfolio_time_series)
    print(portfolio_time_series.get('AAPL'))
    return portfolio_time_series


def get_ticker_daily_returns_data(ticker, dates=""):

    df = pd.DataFrame()
    for i in dates:
        ret_df = sql.find_ticker_daily_returns(ticker, i)
        df = df.append(ret_df, ignore_index=True)

    return df


def analysis_ticker_return_data(ticker):
    dates = ['2017-12-27', '2017-12-28', '2017-12-29', '2018-01-02', '2018-01-03', '2018-01-04', '2018-01-05',
             '2018-01-08', '2018-01-09', '2018-01-10', '2018-01-11', '2018-01-12']

    data = get_ticker_daily_returns_data(ticker, dates)
    mean_10 = data.rolling(window=60).mean()
    return get_ticker_daily_returns_data(ticker,dates)


def analysis_ticker_release_info(ticker):
    earnings = sql.find_stock_earings(ticker)
    if earnings is None:
        return None

    ticker_info = []
    #daily_data.get_data([ticker], ana_date.next_business_dates(n=-1000), ana_date.next_business_dates())

    liquid = sql.find_liquid_ticker(ticker)

    if liquid is None:
        return None

    try:
        for e in earnings:
            release_date = e[1]
            time = e[2]

            if 'before' in time.lower():
                pre_date = ana_date.next_business_dates(datestr=ana_date.convert_date_to_str(release_date),n=-1)
                release_day = ana_date.next_business_dates(datestr=ana_date.convert_date_to_str(release_date))

                pre_stock_daily_data = sql.find_ticker_daily_data(ticker, ana_date.convert_date_to_str(pre_date))
                release_data_stock_daily_data = sql.find_ticker_daily_data(ticker, ana_date.convert_date_to_str(release_day))
                if release_data_stock_daily_data is None:
                    continue

                release_open = release_data_stock_daily_data[2]
                release_high = release_data_stock_daily_data[3]
                release_low = release_data_stock_daily_data[4]

                diff_open = (release_open - pre_stock_daily_data[5]) / (pre_stock_daily_data[5])
                diff_high = (release_high - pre_stock_daily_data[5]) / (pre_stock_daily_data[5])
                diff_low = (release_low - pre_stock_daily_data[5]) / (pre_stock_daily_data[5])

                print('ticker = {ticker}, release date is {date}, {time} , EPS change from {eps1} to {eps2} by {sup},'
                      ' jump from {price1} to {price2} by {percentage}, '
                      'release price range is from {p1} to {p2} and change from {per1} to {per2}'
                      .format(ticker=ticker, date=sql.convert_date_to_str(release_date), time=time,price1=pre_stock_daily_data[5],
                              eps1=e[3], eps2=e[4], sup=e[5],
                              price2=release_data_stock_daily_data[2],
                              percentage=str(diff_open), p1=str(release_low), p2=str(release_high),
                              per1=str(diff_low), per2=str(diff_high)))
                ticker_info.append((ticker, diff_open, diff_low, diff_high,release_date))
            elif 'after' in time.lower():
                next_date = ana_date.next_business_dates(datestr=sql.convert_date_to_str(release_date), n=1)
                release_day = ana_date.next_business_dates(datestr=sql.convert_date_to_str(release_date))

                nex_stock_daily_data = sql.find_ticker_daily_data(ticker, ana_date.convert_date_to_str(next_date))
                release_data_stock_daily_data = sql.find_ticker_daily_data(ticker,
                                                                           ana_date.convert_date_to_str(release_day))
                if nex_stock_daily_data is None:
                    continue

                next_open = nex_stock_daily_data[2]
                next_high = nex_stock_daily_data[3]
                next_low = nex_stock_daily_data[4]

                diff_open = (next_open - release_data_stock_daily_data[5]) / (release_data_stock_daily_data[5])
                diff_high = (next_high - release_data_stock_daily_data[5]) / (release_data_stock_daily_data[5])
                diff_low = (next_low - release_data_stock_daily_data[5]) / (release_data_stock_daily_data[5])

                print('ticker = {ticker}, release date is {date}, {time} , EPS change from {eps1} to {eps2} by {sup},'
                      'jump from {price1} to {price2} by {percentage}, '
                      'release price range is from {p1} to {p2} and change from {per1} to {per2}'
                      .format(ticker=ticker, date=sql.convert_date_to_str(release_date), time=time,
                              eps1=e[3], eps2=e[4],sup=e[5],
                              price1=release_data_stock_daily_data[5],
                              price2=next_open,
                              percentage=str(diff_open), p1=str(next_low), p2=str(next_high),
                              per1=str(diff_low), per2=str(diff_high)))
                ticker_info.append((ticker, diff_open, diff_low, diff_high,release_date))
            else:
                continue

        return ticker_info
    except:
        print('ticker is bad')
        return None


def find_interesting_big_jump_ticker(ticker,jump):
    ticker_info = analysis_ticker_release_info(ticker)

    open_interest = 0

    if ticker_info is not None and len(ticker_info) != 0:
        for info in ticker_info:
            #price jump more then 1%
            if info[1] > jump:
                open_interest += 1
    else:
        return None

    if open_interest / len(ticker_info) > 0.7:
        return ticker
    else:
        return None


def find_interesting_large_gap_ticker(ticker,jump):
    ticker_info = analysis_ticker_release_info(ticker)

    open_interest = 0

    if ticker_info is not None and len(ticker_info) != 0:
        for info in ticker_info:
            #price jump more then 1%
            if info[3]-info[2] > jump:
                open_interest += 1
    else:
        return None

    if open_interest / len(ticker_info) > 0.7:
        return ticker
    else:
        return None


def analysis_after_release_large_gap_ticker(date):
    tickers = sql.find_tickers_release_earnings_by_date(date)

    ret = []
    for ticker in tickers:
        good = find_interesting_large_gap_ticker(ticker, 0.08)
        if good is not None:
            ret.append(ticker)
    return ret


def find_interesting_prerelease_ticker(date):
    tickers = analysis_release_ticker_earnings(date)
    if len(tickers)==0:
        return None
    ret = []
    for ticker in tickers:
        good = analysis_ticker_before_release_behavior(ticker)
        if good is not None:
            ret.append(ticker)
    right = 1.0*len(ret)/len(tickers)

    return 'good tickers {tickers}, only {subset} follow two days pre trade, you are betting {per} is right'\
        .format(tickers=tickers, subset=ret, per=right)


def analysis_release_ticker_earnings(date):
    tickers = sql.find_tickers_release_earnings_by_date(date)

    ret = []
    for ticker in tickers:
        good = find_interesting_big_jump_ticker(ticker, 0.01)
        if good is not None:
            ret.append(ticker)
    return ret


def analysis_release_ticker_before_release_behavior(ticker, start, end, jump):

    info = sql.get_ticker_bench_return(ticker, start, end)

    if info is None or len(info) == 0:
        return None

    for i in info:
        if '^' in i[0]:
            bench_return = i[1]
        else:
            ticker_return = i[1]

    print('ticker = {ticker} from {d1} to {d2}, return is {r}, and sp500 has {br}, release jump is {jump}'
          .format(ticker=ticker,d1=start,d2=end,r=ticker_return,br=bench_return,jump=jump))
    if ticker_return < bench_return and ticker_return+jump < bench_return:
        return -1
    elif ticker_return > bench_return or ticker_return+jump > bench_return:
        return 1


def analysis_ticker_before_release_behavior(ticker):

    earnings = sql.find_stock_earings(ticker)
    if earnings is None or len(earnings)==0:
        return None


    count = 0
    total = 0
    ticker_info = analysis_ticker_release_info(ticker)

    if ticker_info is None or len(ticker_info)==0:
        return None

    map = {}

    for ticker_i in ticker_info:
        map[ticker_i[4]] = ticker_i

    for e in earnings:
        release_date = e[1]
        time = e[2]
        if e[5] is None:
            continue
        if 'before' in time.lower():
            end = ana_date.next_business_dates(datestr=ana_date.convert_date_to_str(release_date),n=-1)
            start = ana_date.next_business_dates(datestr=ana_date.convert_date_to_str(release_date), n=-2)
        elif 'after' in time.lower():
            end = ana_date.next_business_dates(datestr=ana_date.convert_date_to_str(release_date))
            start = ana_date.next_business_dates(datestr=ana_date.convert_date_to_str(release_date), n=-1)
        else:
            continue
        jump = map[release_date][1]
        good = analysis_release_ticker_before_release_behavior(ticker, start, end, jump)
        if good == -1:
            count -= 2
        elif good is not None:
            count += 1

        total += 1

    if total !=0 and 1.0*count/total >0.7:
        return ticker
    else:
        return None


def analysis_oil_inventories_uwt_relationship(ticker):
    info = sql.get_ticker_oil_inventories_relationships(ticker)

    size = len(info)
    count = 0
    for i in info:

        if i[0]>=i[1] and i[2]<=i[4]:
            count+=1
        elif i[0]<=i[1] and i[2]>=i[5]:
            count+=1

    print('{per} % of the time, we are betting right'.format(per=1.0*count/size))


def analysis_tickers_daily_return_ranks(date):

    return sql.find_all_tickers_daily_returns(date)


def analysis_all_sectors_performance(start_date,end_date):
    sectors = sql.find_all_sectors()
    if sectors is None:
        print('No sector in database')
        return None

    for sector in sectors:
        if 'index' in sector.lower():
            continue
        tickers = sql.find_top_tickers_by_sector(sector,'2018-03-06', start_date,end_date)
        portfolio_time_series = get_tickers_return(tickers, start_date, end_date)
        print(portfolio_time_series)
        print(portfolio_time_series.get('AAPL'))


def main():
    #run_portfolio()
    # ret = get_ticker_daily_returns_data('AAPL')
    # plt.hist(ret, normed=True, bins=30)
    # plt.ylabel('Probability')
    # plt.show()
    # print('asd')
    # tickers = analysis_release_ticker_earnings('2018-03-02')
    #
    # print('--------------------------------')
    # print(tickers)
    analysis_ticker_release_info('WUBA')
    #analysis_oil_inventories_uwt_relationship('OIL')
    #analysis_all_sectors_performance('2018-01-01','2018-03-07')
    #print(find_interesting_prerelease_ticker('2018-03-13'))

if __name__ == '__main__':
    main()