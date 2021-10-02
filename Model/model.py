class StockBasicInfo:

    def __init__(self, ticker, security, sector, sub_sector):
        self.ticker = ticker
        self.security = security
        self.sector = sector
        self.sub_sector = sub_sector


class StockDailyData:

    def __init__(self, ticker, date, open, high, low, close, adj_close, volume):
        self.ticker = ticker
        self.date = date
        self.open = open
        self.high = high
        self.low = low
        self.close = close
        self.adj_close = adj_close
        self.volume = volume


class StockMinData:

    def __init__(self, ticker, datetime, high, low, open, close, volume):
        self.ticker = ticker
        self.datetime = datetime
        self.high = high
        self.low = low
        self.open = open
        self.close = close
        self.volume = volume


class ValuationMeasures:

    def __init__(self, ticker, market_cap, enterprise_value, trailing_pe, forward_pe, peg_ratio_5y, price_to_sale,
                 price_to_book, enterprise_revenue, enterprise_ebitda ):
        self.ticker = ticker
        self.market_cap = market_cap
        self.enterprise_value = enterprise_value
        self.trailing_pe = trailing_pe
        self.forward_pe = forward_pe
        self.peg_ratio_5y = peg_ratio_5y
        self.price_to_sale = price_to_sale
        self.price_to_book = price_to_book
        self.enterprise_revenue = enterprise_revenue
        self.enterprise_ebitda = enterprise_ebitda


class StockPriceHistory:
    def __init__(self, ticker, beta, high_52_week, low_52_week):
        self.ticker = ticker
        self.beta = beta
        self.high_52_week = high_52_week
        self.low_52_week = low_52_week


class ShareStatistics:
    def __init__(self, ticker, avg_volume_3m, avg_volume_10d, share_outstanding, hold_insiders, hold_inst, shares_short,
                 short_ratio, shares_short_prev_m):
        self.ticker = ticker
        self.avg_volume_3m = avg_volume_3m
        self.avg_volume_10d = avg_volume_10d
        self.share_outstanding = share_outstanding
        self.hold_insiders = hold_insiders
        self.hold_inst = hold_inst
        self.shares_short = shares_short
        self.short_ratio = short_ratio
        self.shares_short_prev_m = shares_short_prev_m


class StockProfitability:
    def __init__(self, ticker, profit_margin, operating_margin, ret_asset, ret_equity):
        self.ticker = ticker
        self.profit_margin = profit_margin
        self.operating_margin = operating_margin
        self.ret_asset = ret_asset
        self.ret_equity = ret_equity


class StockIncomeStatment:

    def __init__(self, ticker, revenue, revenue_per_share, quarterly_revenue_growth, gross_profit, ebitda,
                 net_income_avi_to_common, trailing_eps, forward_eps, quarterly_earnings_growth):
        self.ticker = ticker
        self.revenue = revenue
        self.revenue_per_share = revenue_per_share
        self.quarterly_revenue_growth = quarterly_revenue_growth
        self.gross_profit = gross_profit
        self.ebitda = ebitda
        self.net_income_avi_to_common = net_income_avi_to_common
        self.trailing_eps = trailing_eps
        self.forward_eps = forward_eps
        self.quarterly_earning_growth = quarterly_earnings_growth


class StockBalanceSheet:
    def __init__(self, ticker, total_cash, total_cash_per_share, total_debt, total_debt_per_equity,
                 current_ratio, book_value_per_share):
        self.ticker = ticker
        self.total_cash = total_cash
        self.total_cash_per_share = total_cash_per_share
        self.total_debt = total_debt
        self.total_debt_per_equity = total_debt_per_equity
        self.current_ratio = current_ratio
        self.book_value_per_share = book_value_per_share


class StockCashFlowStatement:
    def __init__(self, ticker, operating_cash_flow, levered_free_cash_flow):
        self.ticker = ticker
        self.operating_cash_flow = operating_cash_flow
        self.levered_free_cash_flow = levered_free_cash_flow


class StockDividendsAndSplits:
    def __init__(self, ticker, forward_dividend_yield, forward_dividend_rate, trailing_dividend_yield,
                 trailing_dividend_rate, avg_dividend_yield_5y, payout_ratio, dividend_date, ex_dividend_date):
        self.ticker = ticker
        self.forward_dividend_yield = forward_dividend_yield
        self.forward_dividend_rate = forward_dividend_rate
        self.trailing_dividend_yield = trailing_dividend_yield
        self.trailing_dividend_rate = trailing_dividend_rate
        self.avg_dividend_yield_5y = avg_dividend_yield_5y
        self.payout_ratio = payout_ratio
        self.dividend_date = dividend_date
        self.ex_dividend_date = ex_dividend_date


class StockFundamentalStats:

    def __init__(self, valuation_measures, stock_price_history, share_stats, stock_profitability, stock_income_statement,
                 stock_balance_sheet, cash_flow_statement, stock_dividend_split):
        self.valuation_measures = valuation_measures
        self.stock_price_history=stock_price_history
        self.share_stats = share_stats
        self.stock_profitability = stock_profitability
        self.stock_income_statement = stock_income_statement
        self.stock_balance_sheet = stock_balance_sheet
        self.cash_flow_statement = cash_flow_statement
        self.stock_dividend_split = stock_dividend_split


class Portfolio:

    def __init__(self, ticker, weight=0, money=0):
        self.ticker = ticker
        self.weight = weight
        self.money = money


class StockEarningData:

    def __init__(self, ticker, release_date, time, expect_eps, actual_eps , surprise):
        self.ticker = ticker
        self.release_date = release_date
        self.time = time
        self.expect_eps = expect_eps if expect_eps != '-' else None
        self.actual_eps = actual_eps if actual_eps != '-' else None
        self.surprise = surprise if surprise != '-' else None
