import psycopg2
from config import config
from pprint import pprint
import datetime


def merge_into_stock_basic_info(stock):
    """ insert a new vendor into the stock_basic_info table

    Args : stock an object of stock_basic_info class

    Return :
    """
    sql = """
         WITH temp (ticker,instr_name,sector,sub_sector ) as (
               values(%s,%s,%s,%s) 
                ),
                upsert as
                (
                    UPDATE stock_basic_info SET instr_name = temp.instr_name
                    , sector= trim(temp.sector) , 
                    sub_sector=trim(temp.sub_sector) 
                    From temp where stock_basic_info.ticker = temp.ticker 
                    RETURNING stock_basic_info.*
                )
                INSERT INTO stock_basic_info(ticker,instr_name,sector,sub_sector)
                SELECT ticker,instr_name,trim(sector),trim(sub_sector)
                FROM temp
                WHERE NOT EXISTS (SELECT 1 FROM upsert up WHERE up.ticker = temp.ticker );
    """
    conn = None
    try:
        # read database configuration
        params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        # datetime.datetime.today().strftime('%Y-%m-%d')
        cur.execute(sql, (stock.ticker, stock.security, stock.sector, stock.sub_sector))

        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        return False
    finally:
        if conn is not None:
            conn.close()

    return True


def delete_from_stock_basic_info(ticker):
    """ delete a row into the stock_basic_info table

        Args : stock ticker

        Return :
        """
    sql = """DELETE FROM stock_basic_info where ticker = '{ticker}';""".format(ticker=ticker)
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
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        return False
    finally:
        if conn is not None:
            conn.close()

    return True


def select_all_tickers():

    sql = """SELECT ticker FROM stock_basic_info where insert_date is not null order by 1"""
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

        tickers = cur.fetchall()

        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        raise
    finally:
        if conn is not None:
            conn.close()

    return (ticker[0] for ticker in tickers)


def select_all_fundamental_tickers(date):
    sql = """select distinct ticker from stock_fundamental_statistics where ticker not in
    (SELECT ticker FROM stock_fundamental_statistics 
    where update_date = cast('{date}' as date))""".format(date=date)
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

        tickers = cur.fetchall()

        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        raise
    finally:
        if conn is not None:
            conn.close()

    return [ticker[0] for ticker in tickers]


def select_all_loading_tickers(start_date, end_date):
    sql = """ select a.ticker from stock_basic_info a where a.ticker not in(
    SELECT distinct b.ticker FROM stock_daily_data b where b.date between %s and %s) """
    conn = None
    try:
        # read database configuration
        params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        cur.execute(sql, (start_date, end_date))

        tickers = cur.fetchall()

        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        raise
    finally:
        if conn is not None:
            conn.close()

    return [ticker[0] for ticker in tickers]


def select_all_fx_currency(start_date, end_date):
    sql = """ select a.fx_id, a.from_ccy, a.to_ccy from fx_basic_info a where a.ccy_id not in(
    SELECT distinct b.fx_id FROM fx_daily_rate b where b.date between %s and %s) """
    conn = None
    try:
        # read database configuration
        params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        cur.execute(sql, (start_date, end_date))

        tickers = cur.fetchall()

        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        raise
    finally:
        if conn is not None:
            conn.close()

    return tickers


def select_all_loading_minute_tickers():
    sql = """ select a.ticker from stock_basic_info a where a.ticker not in(
    SELECT distinct b.ticker FROM stock_minute_data b where b.datetime >= current_date and b.ticker <>'BCR')
     and a.insert_date is not null """

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

        tickers = cur.fetchall()

        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        raise
    finally:
        if conn is not None:
            conn.close()

    return [ticker[0] for ticker in tickers]


def merge_into_stock_daily_data(stock_daily_data):
    """ insert a new vendor into the stock_basic_info table

    Args : stock an object of stock_basic_info class

    Return :
    """
    sql = """  WITH temp (ticker,date,open,high,low, close, adj_close,volume) as (
               values(%s,%s,%s,%s,%s,%s,%s,%s) 
                ),
                upsert as
                (
                    UPDATE stock_daily_data SET OPEN = temp.open
                    , close= temp.close , 
                    high=temp.high ,low = temp.low
                    , volume = temp.volume From temp where stock_daily_data.ticker = temp.ticker 
                    and stock_daily_data.date=temp.date
                    RETURNING stock_daily_data.*
                )
                INSERT INTO stock_daily_data (ticker,date,open,high,low, close, adj_close,volume)
                SELECT ticker,date,open,high,low, close, adj_close,volume
                FROM temp
                WHERE NOT EXISTS (SELECT 1 FROM upsert up WHERE up.ticker = temp.ticker and up.date=temp.date);
    """
    conn = None
    try:
        # read database configuration
        params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        cur.execute(sql, (stock_daily_data.ticker, stock_daily_data.date,
                          round(stock_daily_data.open, 2), round(stock_daily_data.high, 2),
                          round(stock_daily_data.low, 2), round(stock_daily_data.close, 2),
                          round(stock_daily_data.adj_close, 2), stock_daily_data.volume))

        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        raise
    finally:
        if conn is not None:
            conn.close()

    return True


def merge_many_into_stock_daily_data(stock_daily_datas):
    """ insert a new vendor into the stock_daily_data table

    Args : stock an object of stock_daily_data class

    Return :
    """
    sql = """  WITH temp (ticker,date,open,high,low, close, adj_close,volume) as (
               values(%s,%s,%s,%s,%s,%s,%s,%s) 
                ),
                upsert as
                (
                    UPDATE stock_daily_data SET OPEN = temp.open
                    , close= temp.close , 
                    high=temp.high ,low = temp.low
                    , volume = cast(temp.volume as integer) From temp where stock_daily_data.ticker = temp.ticker 
                    and stock_daily_data.date=temp.date
                    RETURNING stock_daily_data.*
                )
                INSERT INTO stock_daily_data (ticker,date,open,high,low, close, adj_close,volume)
                SELECT ticker,date,open,high,low, close, adj_close,cast(temp.volume as integer)
                FROM temp
                WHERE NOT EXISTS (SELECT 1 FROM upsert up WHERE up.ticker = temp.ticker and up.date=temp.date);
    """
    conn = None
    try:
        # read database configuration
        params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        param_list = [(stock_daily_data.ticker, stock_daily_data.date,
                 round(stock_daily_data.open, 2), round(stock_daily_data.high, 2),
                 round(stock_daily_data.low, 2), round(stock_daily_data.close, 2),
                 round(stock_daily_data.adj_close, 2),round(stock_daily_data.volume, 0)
                 #round(stock_daily_data.volume, 0) if len(str(round(stock_daily_data.volume,0))) < 9 else 99999999
                )
                for stock_daily_data in stock_daily_datas]

        cur.executemany(sql, param_list)

        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        pprint(param_list)
        raise
    finally:
        if conn is not None:
            conn.close()

    return True


def merge_many_into_fx_daily_data(fx_rates):
    """ insert a new vendor into the stock_basic_info table

    Args : stock an object of stock_basic_info class

    Return :
    """
    sql = """  WITH temp (ccy_id, date, value) as (
               values(%s,%s,%s) 
                ),
                upsert as
                (
                    UPDATE fx_daily_rate SET value = cast(temp.value as double precision)
                    From temp where fx_daily_rate.ccy_id = cast(temp.ccy_id as integer) 
                    and fx_daily_rate.date=cast(temp.date as date)
                    RETURNING fx_daily_rate.*
                )
                INSERT INTO fx_daily_rate 
                SELECT cast(ccy_id as integer), cast(date as date), cast(value as double precision)
                FROM temp
                WHERE NOT EXISTS (SELECT 1 FROM upsert up WHERE up.ccy_id = cast(temp.ccy_id as integer)
                 and up.date=cast(temp.date as date));
    """
    conn = None
    try:
        # read database configuration
        params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        param_list = [(fx_rate[0], fx_rate[1],
                       round(fx_rate[2], 2)) if fx_rate[2] is not None else None
                      for fx_rate in fx_rates]

        cur.executemany(sql, param_list)

        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        pprint(param_list)
        raise
    finally:
        if conn is not None:
            conn.close()

    return True


def merge_many_into_stock_min_data(stock_min_datas):
    """ insert a new vendor into the stock_basic_info table

    Args : stock an object of stock_basic_info class

    Return :
    """
    sql = """  WITH temp (ticker,datetime,high,low,open,close,volume) as (
               values(%s,
               to_timestamp(%s,'YYYY-MM-DD hh24:mi:ss'),
               cast(%s as double precision),
               cast(%s as double precision),
               cast(%s as double precision),
               cast(%s as double precision),
               cast(%s as bigint)
               ) 
                ),
                upsert as
                (
                    UPDATE stock_minute_data SET close= temp.close ,open=temp.open ,high= temp.high ,low= temp.low 
                    , volume = temp.volume From temp where stock_minute_data.ticker = temp.ticker 
                    and stock_minute_data.datetime=temp.datetime
                    RETURNING stock_minute_data.*
                )
                INSERT INTO stock_minute_data (ticker,datetime,high,low,open,close,volume)
                SELECT ticker,datetime,high,low,open,close,volume
                FROM temp
                WHERE NOT EXISTS (SELECT 1 FROM upsert up WHERE up.ticker = temp.ticker and up.datetime=temp.datetime);
    """
    conn = None
    try:
        # read database configuration
        params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        # create a new cursor
        cur = conn.cursor()

        # execute the INSERT statement
        param_list = [(stock_min_data.ticker, stock_min_data.datetime,
                       round(stock_min_data.high, 2) if stock_min_data.high is not None else None,
                       round(stock_min_data.low, 2) if stock_min_data.low is not None else None,
                       round(stock_min_data.open, 2) if stock_min_data.open is not None else None,
                       round(stock_min_data.close, 2) if stock_min_data.close is not None else None,
                       round(stock_min_data.volume, 0) if stock_min_data.volume is not None else None)
                      for stock_min_data in stock_min_datas]

        cur.executemany(sql, param_list)

        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        pprint(param_list)
        raise
    finally:
        if conn is not None:
            conn.close()

    return True


def merge_many_into_stock_fundamental_data(stock_fundamental_statistics):

    if stock_fundamental_statistics is None:
        return False

    sql = """WITH t (ticker,market_cap,enterprise_value,trailing_pe,forward_pe,peg_ratio_5y,price_to_sale,
    price_to_book,enterprise_revenue,enterprise_ebitda,beta,high_52_week,low_52_week,avg_volume_3m,avg_volume_10d,
    share_outstanding,hold_insiders,hold_inst,shares_short,short_ratio,shares_short_prev_m,profit_margin,operating_margin,
    return_on_asset,return_on_equity,revenue,revenue_per_share,quarterly_revenue_growth,gross_profit,ebitda,
    net_income_avi_to_common,trailing_eps,forward_eps,quarterly_earning_growth,total_cash,total_cash_per_share,
    total_debt,total_debt_per_equity,current_ratio,book_value_per_share,operating_cash_flow,levered_free_cash_flow,
    forward_dividend_yield,forward_dividend_rate,trailing_dividend_yield,trailing_dividend_rate,avg_dividend_yield_5y,
    payout_ratio,dividend_date,ex_dividend_date,update_date) as (values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)),
       upsert as
       (
           UPDATE stock_fundamental_statistics 
           SET market_cap= cast(t.market_cap as bigint),
           enterprise_value=cast(t.enterprise_value as bigint),
           trailing_pe=cast(t.trailing_pe as double precision),
           forward_pe=cast(t.forward_pe as double precision),
           peg_ratio_5y=cast(t.peg_ratio_5y as double precision),
           price_to_sale=cast(t.price_to_sale as double precision),
           price_to_book=cast(t.price_to_book as double precision),
           enterprise_revenue=cast(t.enterprise_revenue as double precision),
           enterprise_ebitda=cast(t.enterprise_ebitda as double precision),
           beta=cast(t.beta as double precision),
           high_52_week=cast(t.high_52_week as double precision),
           low_52_week=cast(t.low_52_week as double precision),
           avg_volume_3m=cast(t.avg_volume_3m as bigint),
           avg_volume_10d=cast(t.avg_volume_10d as bigint),
           share_outstanding=cast(t.share_outstanding as bigint),
           hold_insiders=cast(t.hold_insiders as bigint),
           hold_inst=cast(t.hold_inst as bigint),
           shares_short=cast(t.shares_short as bigint),
           short_ratio=cast(t.short_ratio as double precision),
           shares_short_prev_m=cast(t.shares_short_prev_m as bigint),
           profit_margin=cast(t.profit_margin as double precision),
           operating_margin=cast(t.operating_margin as double precision),
           return_on_asset=cast(t.return_on_asset as double precision),
           return_on_equity=cast(t.return_on_equity as double precision),
           revenue=cast(t.revenue as bigint),
           revenue_per_share=cast(t.revenue_per_share as double precision),
           quarterly_revenue_growth=cast(t.quarterly_revenue_growth as double precision),
           total_cash=cast(t.total_cash as bigint),
           total_cash_per_share=cast(t.total_cash_per_share as double precision),
           total_debt=cast(t.total_debt as bigint),
           total_debt_per_equity=cast(t.total_debt_per_equity as double precision),
           current_ratio=cast(t.current_ratio as double precision),
           book_value_per_share=cast(t.book_value_per_share as double precision),
           operating_cash_flow=cast(t.operating_cash_flow as bigint),
           levered_free_cash_flow=cast(t.levered_free_cash_flow as bigint),
           forward_dividend_yield=cast(t.forward_dividend_yield as double precision),
           forward_dividend_rate=cast(t.forward_dividend_rate as double precision),
           trailing_dividend_yield=cast(t.trailing_dividend_yield as double precision),
           trailing_dividend_rate=cast(t.trailing_dividend_rate as double precision),
           avg_dividend_yield_5y=cast(t.avg_dividend_yield_5y as double precision),
           payout_ratio=cast(t.payout_ratio as double precision),
           dividend_date=cast(t.dividend_date as date),
           ex_dividend_date=cast(t.ex_dividend_date as date) 
           From t where stock_fundamental_statistics.ticker = t.ticker 
           and stock_fundamental_statistics.update_date = cast(t.update_date as date) 
           RETURNING stock_fundamental_statistics.*
       )INSERT INTO stock_fundamental_statistics(ticker,market_cap,enterprise_value,trailing_pe,forward_pe,
       peg_ratio_5y,price_to_sale,price_to_book,enterprise_revenue,enterprise_ebitda,beta,high_52_week,low_52_week,avg_volume_3m,avg_volume_10d,
    share_outstanding,hold_insiders,hold_inst,shares_short,short_ratio,shares_short_prev_m,profit_margin,operating_margin,
    return_on_asset,return_on_equity,revenue,revenue_per_share,quarterly_revenue_growth,gross_profit,ebitda,
    net_income_avi_to_common,trailing_eps,forward_eps,quarterly_earning_growth,total_cash,total_cash_per_share,
    total_debt,total_debt_per_equity,current_ratio,book_value_per_share,operating_cash_flow,levered_free_cash_flow,
    forward_dividend_yield,forward_dividend_rate,trailing_dividend_yield,trailing_dividend_rate,avg_dividend_yield_5y,
    payout_ratio,dividend_date,ex_dividend_date,update_date
    )
    SELECT ticker,
    cast(market_cap as bigint),
    cast(enterprise_value as bigint),
    cast(trailing_pe as double precision),
    cast(forward_pe as double precision),
    cast(peg_ratio_5y as double precision),
    cast(price_to_sale as double precision),
    cast(price_to_book as double precision),
    cast(enterprise_revenue as double precision),
    cast(enterprise_ebitda as double precision),
    cast(beta as double precision),
    cast(high_52_week as double precision),
    cast(low_52_week as double precision),
    cast(avg_volume_3m as bigint),
    cast(avg_volume_10d as bigint),
    cast(share_outstanding as bigint),
    cast(hold_insiders as bigint),
    cast(hold_inst as bigint),
    cast(shares_short as bigint),
    cast(short_ratio as double precision),
    cast(shares_short_prev_m as bigint),
    cast(profit_margin as double precision),
    cast(operating_margin as double precision),
    cast(return_on_asset as double precision),
    cast(return_on_equity as double precision),
    cast(revenue as bigint),
    cast(revenue_per_share as double precision),
    cast(quarterly_revenue_growth as double precision),
    cast(gross_profit as bigint),
    cast(ebitda as bigint),
    cast(net_income_avi_to_common as bigint),
    cast(trailing_eps as double precision),
    cast(forward_eps as double precision),
    cast(quarterly_earning_growth as double precision),
    cast(total_cash as bigint),
    cast(total_cash_per_share as double precision),
    cast(total_debt as bigint),
    cast(total_debt_per_equity as double precision),
    cast(current_ratio as double precision),
    cast(book_value_per_share as double precision),
    cast(operating_cash_flow as bigint),
    cast(levered_free_cash_flow as bigint),
    cast(forward_dividend_yield as double precision),
    cast(forward_dividend_rate as double precision),
    cast(trailing_dividend_yield as double precision),
    cast(trailing_dividend_rate as double precision),
    cast(avg_dividend_yield_5y as double precision),
    cast(payout_ratio as double precision),
    cast(dividend_date as date),
    cast(ex_dividend_date as date),
    cast(update_date as date) 
    FROM t WHERE NOT EXISTS (SELECT 1 FROM upsert up WHERE up.ticker = t.ticker and up.update_date=cast(t.update_date as date) );"""

    def get_data(json, field):
        if json is None or len(json) == 0:
            return None
        return json.get(field)

    conn = None
    try:
        # read database configuration
        params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        # create a new cursor
        cur = conn.cursor()

        # execute the INSERT statement
        param = (stock_fundamental_statistics.valuation_measures.ticker,
                      get_data(stock_fundamental_statistics.valuation_measures.market_cap, 'raw'),
                      get_data(stock_fundamental_statistics.valuation_measures.enterprise_value, 'raw'),
                      get_data(stock_fundamental_statistics.valuation_measures.trailing_pe, 'raw'),
                      get_data(stock_fundamental_statistics.valuation_measures.forward_pe, 'raw'),
                      get_data(stock_fundamental_statistics.valuation_measures.peg_ratio_5y, 'raw'),
                      get_data(stock_fundamental_statistics.valuation_measures.price_to_sale, 'raw'),
                      get_data(stock_fundamental_statistics.valuation_measures.price_to_book, 'raw'),
                      get_data(stock_fundamental_statistics.valuation_measures.enterprise_revenue, 'raw'),
                      get_data(stock_fundamental_statistics.valuation_measures.enterprise_ebitda, 'raw'),
                      get_data(stock_fundamental_statistics.stock_price_history.beta, 'raw'),
                      get_data(stock_fundamental_statistics.stock_price_history.high_52_week, 'raw'),
                      get_data(stock_fundamental_statistics.stock_price_history.low_52_week, 'raw'),
                      get_data(stock_fundamental_statistics.share_stats.avg_volume_3m, 'raw'),
                      get_data(stock_fundamental_statistics.share_stats.avg_volume_10d, 'raw'),
                      get_data(stock_fundamental_statistics.share_stats.share_outstanding, 'raw'),
                      get_data(stock_fundamental_statistics.share_stats.hold_insiders, 'raw'),
                      get_data(stock_fundamental_statistics.share_stats.hold_inst, 'raw'),
                      get_data(stock_fundamental_statistics.share_stats.shares_short, 'raw'),
                      get_data(stock_fundamental_statistics.share_stats.short_ratio, 'raw'),
                      get_data(stock_fundamental_statistics.share_stats.shares_short_prev_m, 'raw'),
                      get_data(stock_fundamental_statistics.stock_profitability.profit_margin, 'raw'),
                      get_data(stock_fundamental_statistics.stock_profitability.operating_margin, 'raw'),
                      get_data(stock_fundamental_statistics.stock_profitability.ret_asset, 'raw'),
                      get_data(stock_fundamental_statistics.stock_profitability.ret_equity, 'raw'),
                      get_data(stock_fundamental_statistics.stock_income_statement.revenue, 'raw'),
                      get_data(stock_fundamental_statistics.stock_income_statement.revenue_per_share, 'raw'),
                      get_data(stock_fundamental_statistics.stock_income_statement.quarterly_revenue_growth, 'raw'),
                      get_data(stock_fundamental_statistics.stock_income_statement.gross_profit, 'raw'),
                      get_data(stock_fundamental_statistics.stock_income_statement.ebitda, 'raw'),
                      get_data(stock_fundamental_statistics.stock_income_statement.net_income_avi_to_common, 'raw'),
                      get_data(stock_fundamental_statistics.stock_income_statement.trailing_eps, 'raw'),
                      get_data(stock_fundamental_statistics.stock_income_statement.forward_eps, 'raw'),
                      get_data(stock_fundamental_statistics.stock_income_statement.quarterly_earning_growth, 'raw'),
                      get_data(stock_fundamental_statistics.stock_balance_sheet.total_cash, 'raw'),
                      get_data(stock_fundamental_statistics.stock_balance_sheet.total_cash_per_share, 'raw'),
                      get_data(stock_fundamental_statistics.stock_balance_sheet.total_debt, 'raw'),
                      get_data(stock_fundamental_statistics.stock_balance_sheet.total_debt_per_equity, 'raw'),
                      get_data(stock_fundamental_statistics.stock_balance_sheet.current_ratio, 'raw'),
                      get_data(stock_fundamental_statistics.stock_balance_sheet.book_value_per_share, 'raw'),
                      get_data(stock_fundamental_statistics.cash_flow_statement.operating_cash_flow, 'raw'),
                      get_data(stock_fundamental_statistics.cash_flow_statement.levered_free_cash_flow, 'raw'),
                      get_data(stock_fundamental_statistics.stock_dividend_split.forward_dividend_yield, 'raw'),
                      get_data(stock_fundamental_statistics.stock_dividend_split.forward_dividend_rate, 'raw'),
                      get_data(stock_fundamental_statistics.stock_dividend_split.trailing_dividend_yield, 'raw'),
                      get_data(stock_fundamental_statistics.stock_dividend_split.trailing_dividend_rate, 'raw'),
                      get_data(stock_fundamental_statistics.stock_dividend_split.avg_dividend_yield_5y, 'raw'),
                      get_data(stock_fundamental_statistics.stock_dividend_split.payout_ratio, 'raw'),
                      get_data(stock_fundamental_statistics.stock_dividend_split.dividend_date, 'fmt'),
                      get_data(stock_fundamental_statistics.stock_dividend_split.ex_dividend_date, 'fmt'),
                      datetime.datetime.today().strftime('%Y-%m-%d')
                      )

        cur.execute(sql, param)

        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        pprint(param)
        raise
    finally:
        if conn is not None:
            conn.close()

    return True


def merge_into_stock_earings(stock_earing):

    if stock_earing is None:
        return

    sql = """  WITH temp (ticker,release_date,time,expect_eps,actual_eps,surprise) as (
               values(%s,%s,%s,%s,%s,%s) 
                ),
                upsert as
                (
                    UPDATE stock_earnings SET expect_eps = cast( temp.expect_eps as double precision) 
                    , actual_eps= cast(temp.actual_eps as double precision) , 
                    surprise= cast(temp.surprise as double precision)   
                    From temp where stock_earnings.ticker = temp.ticker  
                    and stock_earnings.release_date = cast(temp.release_date as date)
                    and stock_earnings.release_date=cast(stock_earnings.release_date as date)
                    RETURNING stock_earnings.*
                )
                INSERT INTO stock_earnings (ticker,release_date,time,expect_eps,actual_eps,surprise)
                SELECT ticker, cast(release_date as date),time, 
                cast(expect_eps as double precision),
                cast(actual_eps as double precision),
                cast(surprise as double precision) 
                FROM temp
                WHERE NOT EXISTS (SELECT 1 FROM upsert up WHERE up.ticker = temp.ticker 
                and up.release_date=cast(temp.release_date as date));
    """
    conn = None
    try:
        # read database configuration
        params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        cur.execute(sql, (stock_earing.ticker, stock_earing.release_date,
                          stock_earing.time, stock_earing.expect_eps,
                          stock_earing.actual_eps, stock_earing.surprise))

        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        return False
    finally:
        if conn is not None:
            conn.close()

    return True


def merge_into_crude_oil_inventories(date, actual, expect):

    if date is None or actual is None or expect is None:
        return

    sql = """  WITH temp (date, actual, expect) as (
               values(%s,%s,%s) 
                ),
                upsert as
                (
                    UPDATE crude_oil_inventories SET actual = cast( temp.actual as double precision) 
                    , expect= cast(temp.expect as double precision) 
                    From temp where crude_oil_inventories.date = cast(temp.date as date) 
                    RETURNING crude_oil_inventories.*
                )
                INSERT INTO crude_oil_inventories (date, actual, expect)
                SELECT cast(date as date),
                cast(actual as double precision),
                cast(expect as double precision)
                FROM temp
                WHERE NOT EXISTS (SELECT 1 FROM upsert up WHERE up.date=cast(temp.date as date));
    """
    conn = None
    try:
        # read database configuration
        params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        cur.execute(sql, (date,actual,expect))

        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        return False
    finally:
        if conn is not None:
            conn.close()

    return True


