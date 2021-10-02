import psycopg2
from config import config
import pandas as pd
from Analysis.analysis_date import *


def get_portfolio_daily_time_series(tickers, start_date, end_date):
    if tickers is None or len(tickers) == 0:
        return None

    sql = "select a.date, a.close, a.ticker from stock_daily_data a where a.ticker in ('" + "','".join(tickers)\
          +"') and a.date between cast(%s as date) and cast(%s as date);"
    conn = None
    try:
        # read database configuration
        params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)

        df = pd.DataFrame()

        for chunk in pd.read_sql(sql, index_col='date', params=(start_date, end_date), con=conn, chunksize=5000):
            df = df.append(chunk)

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        return False
    finally:
        if conn is not None:
            conn.close()

    df = df.sort_index()

    data_group = df.groupby('ticker')
    sorted_dates = sorted(list(set(df.index.values)))
    ticker_dict = {'date': sorted_dates}
    for t in tickers:
        ticker_dict[t] = data_group.get_group(t).sort_index().close.values

    new_df = pd.DataFrame.from_dict(ticker_dict)
    new_df = new_df.set_index('date')
    return new_df


def find_all_tickers_cap():
    sql = "with temp(ticker, latest_date) as (select ticker,max(update_date) from stock_fundamental_statistics " \
          "group by ticker)select a.ticker, market_cap from stock_fundamental_statistics a, temp t where a.market_cap " \
          "is not null and a.update_date = t.latest_date and t.ticker = a.ticker"

    conn = None
    try:
        # read database configuration
        params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)

        df = pd.DataFrame()

        for chunk in pd.read_sql(sql, con=conn, chunksize=5000):
            df = df.append(chunk)

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        return False
    finally:
        if conn is not None:
            conn.close()

    return df


def find_ticker_low_position():
    sql = "with sdd_temp(ticker, latest_date) as (select ticker,max(update_date) from stock_fundamental_statistics " \
          "group by ticker), sfs_temp(ticker, latest_date) as (select ticker, max(date) from stock_daily_data " \
          "group by ticker)select a.ticker, a.low_52_week lowest, b.adj_close as close from stock_fundamental_statistics a, " \
          "sdd_temp t , sfs_temp t1 , stock_daily_data b where a.market_cap is not null and " \
          "a.update_date = t.latest_date and t.ticker = a.ticker and b.ticker = t1.ticker and " \
          "b.date=t1.latest_date and b.ticker=a.ticker"

    conn = None
    try:
        # read database configuration
        params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)

        df = pd.DataFrame()

        for chunk in pd.read_sql(sql, index_col='ticker', con=conn, chunksize=5000):
            df = df.append(chunk)

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        return False
    finally:
        if conn is not None:
            conn.close()

    return df


def find_ticker_min_data(ticker):
    sql = "select datetime,close from stock_minute_data where ticker ='{ticker}'".format(ticker=ticker)

    conn = None
    try:
        # read database configuration
        params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)

        df = pd.DataFrame()

        for chunk in pd.read_sql(sql, index_col='datetime', con=conn, chunksize=5000):
            df = df.append(chunk)

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        return False
    finally:
        if conn is not None:
            conn.close()

    return df


def find_ticker_daily_returns(ticker, date, format ="%Y-%m-%d" ):

    today = convert_str_to_date(datestr=date, format=format)
    p_date = convert_date_to_str(next_business_dates(datestr=date, format=format, n=-1))
    n_date = convert_date_to_str(next_business_dates(datestr=date, format=format, n=1))

    sql = "select (a.close-b.adj_close)/(b.adj_close)*100 as daily_return " \
          "from stock_minute_data a join stock_daily_data b on a.ticker = b.ticker " \
          "where a.ticker= '{ticker}' " \
          "and a.datetime between '{today}' and '{next_date}' " \
          "and b.date = '{pre_date}'".format(ticker=ticker, today=today, next_date=n_date, pre_date=p_date)

    #print(sql)
    conn = None
    try:
        # read database configuration
        params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)

        df = pd.DataFrame()

        for chunk in pd.read_sql(sql, con=conn, chunksize=5000):
            df = df.append(chunk)

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        return False
    finally:
        if conn is not None:
            conn.close()

    return df


def find_stock_earings(ticker):

    if ticker is None:
        return

    sql = """  select * from stock_earnings where ticker = '{ticker}'
    """.format(ticker=ticker)
    conn = None
    try:
        # read database configuration
        params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        cur.execute(sql)

        # commit the changes to the database
        earnings = cur.fetchall()

        # close communication with the database
        cur.close()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        return None
    finally:
        if conn is not None:
            conn.close()

    return earnings


def find_tickers_release_earnings_by_date(date):

    if date is None:
        return

    sql = """  select * from stock_earnings where release_date = cast('{date}' as date)
    """.format(date=date)
    conn = None
    try:
        # read database configuration
        params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        cur.execute(sql)

        # commit the changes to the database
        tickers = [i[0] for i in cur.fetchall()]

        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        return None
    finally:
        if conn is not None:
            conn.close()

    return tickers


def find_ticker_daily_data(ticker, datastr):

    if ticker is None:
        return

    sql = """  select * from stock_daily_data where ticker = '{ticker}' and date = cast('{date}' as date)
    """.format(ticker=ticker,date=datastr)
    conn = None
    try:
        # read database configuration
        params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        cur.execute(sql)

        # commit the changes to the database
        stock_daily_data = cur.fetchall()

        # close communication with the database
        cur.close()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        return None
    finally:
        if conn is not None:
            conn.close()

    if len(stock_daily_data) !=0:
        return stock_daily_data[0]

    return None


def find_liquid_ticker(ticker):
    if ticker is None:
        return

    sql = """  select ticker FROM stock_daily_data where ticker= '{ticker}' and date between cast('{d1}' as date)
     and cast('{d2}' as date) GROUP BY TICKER having avg(volume)>200000 and avg(close)>30
    """.format(ticker=ticker, d1=convert_date_to_str(next_business_dates(n=-30)), d2=convert_date_to_str(next_business_dates()))
    conn = None
    try:
        # read database configuration
        params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        cur.execute(sql)

        # commit the changes to the database
        stock_daily_data = cur.fetchall()


        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        return None
    finally:
        if conn is not None:
            conn.close()

    if len(stock_daily_data) !=0:
        return stock_daily_data[0]

    return None


def get_ticker_oil_inventories_relationships(ticker):

    sql = '''SELECT ACTUAL,EXPECT,OPEN,CLOSE ,HIGH,LOW FROM STOCK_DAILY_DATA A JOIN crude_oil_inventories B 
    ON A.DATE=B.DATE WHERE TICKER = '{ticker}' '''.format(ticker=ticker)

    conn = None
    try:
        # read database configuration
        params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        cur.execute(sql)

        info = cur.fetchall()

        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        raise
    finally:
        if conn is not None:
            conn.close()

    return info


def get_ticker_bench_return(ticker, start, end):
    sql = '''     SELECT a.ticker, b.close/a.open-1 FROM STOCK_DAILY_DATA a join stock_daily_data b on a.ticker=b.ticker
     WHERE a.TICKER = '{ticker}' AND a.DATE = cast('{d1}' as date) and b.date = cast('{d2}' as date) 
     union SELECT a.ticker, b.close/a.open-1 FROM STOCK_DAILY_DATA a join 
     stock_daily_data b on a.ticker=b.ticker WHERE a.TICKER like '%^G%' AND a.DATE = cast('{d1}' as date) 
     and b.date = cast('{d2}' as date) 
     '''.format(ticker=ticker,d1=start,d2=end)

    conn = None
    try:
        # read database configuration
        params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        cur.execute(sql)

        info = cur.fetchall()

        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        raise
    finally:
        if conn is not None:
            conn.close()

    return info


def find_all_tickers_daily_returns(date):
    sql = ''' SELECT A.TICKER, A.CLOSE/B.CLOSE-1 AS R ,c.sector,c.sub_sector
    FROM STOCK_DAILY_DATA A JOIN STOCK_DAILY_DATA B ON A.TICKER=B.TICKER
    join stock_basic_info c on a.ticker=c.ticker
    WHERE A.DATE=cast('{today}' as date) AND B.DATE=cast('{preday}' as date) AND B.CLOSE !=0
    AND A.TICKER IN (SELECT TICKER FROM STOCK_DAILY_DATA WHERE DATE BETWEEN '2018-02-07' AND '2018-03-07' 
    GROUP BY TICKER HAVING( AVG(VOLUME)>40000 ) AND AVG(CLOSE)>30) 
    ORDER BY R'''.format(today=date,preday=next_business_dates(datestr=convert_date_to_str(date), n=-1))

    conn = None
    try:
        # read database configuration
        params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        cur.execute(sql)

        info = cur.fetchall()

        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        raise
    finally:
        if conn is not None:
            conn.close()

    return info


def find_top_tickers_by_sector(sector, date, number=100, is_large=True):
    sql = '''select b.ticker,market_cap 
    from stock_fundamental_statistics a join stock_basic_info b on a.ticker=b.ticker and b.sector ='{sector}'
    where a.update_date = cast('{date}' as date)
     order by market_cap {order} limit {num}'''.format(sector=sector, date=date,
                                                       num=number,order=('ASC' if is_large else 'DESC'))

    conn = None
    try:
        # read database configuration
        params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        cur.execute(sql)

        info = cur.fetchall()

        # close communication with the database
        cur.close()

        return [i[0] for i in info]

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        raise
    finally:
        if conn is not None:
            conn.close()

    return None


def find_all_sectors():
    sql = ''' select sector,count(1) from stock_basic_info 
    where sector is not null and length(sector)!=0 group by sector'''

    conn = None
    try:
        # read database configuration
        params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        cur.execute(sql)

        info = cur.fetchall()

        # close communication with the database
        cur.close()

        return [i[0] for i in info]

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        raise
    finally:
        if conn is not None:
            conn.close()

    return None


if __name__ == '__main__':
    find_ticker_daily_returns('AAPL', '2018-02-23')