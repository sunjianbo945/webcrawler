"""
Scrape a table from wikipedia using python. Allows for cells spanning multiple rows and/or columns. Outputs csv files for
each table
"""

from bs4 import BeautifulSoup
from urllib.request import urlopen
from DataLoading.crud import merge_into_stock_basic_info as insert
from Model.model import StockBasicInfo


def scrap_sp500():
    wiki = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    page = urlopen(wiki)
    soup = BeautifulSoup(page)

    tables = soup.findAll("table", {"class": "wikitable"})

    # show tables
    table = tables[0]

    # preinit list of lists
    rows = table.findAll("tr")
    row_lengths = [len(r.findAll(['th', 'td'])) for r in rows]
    ncols = max(row_lengths)
    nrows = len(rows)
    data = []
    for i in range(nrows):
        rowD = []
        for j in range(ncols):
            rowD.append('')
        data.append(rowD)

    # process html
    for i in range(len(rows)):
        row = rows[i]
        rowD = []
        cells = row.findAll(["td", "th"])
        for j in range(len(cells)):
            cell = cells[j]

            # lots of cells span cols and rows so lets deal with that
            cspan = int(cell.get('colspan', 1))
            rspan = int(cell.get('rowspan', 1))
            for k in range(rspan):
                for l in range(cspan):
                    data[i + k][j + l] += cell.text

        data.append(rowD)

    for i in range(1, nrows):
        stock = StockBasicInfo(ticker=data[i][1], security=data[i][0], sector=data[i][3], sub_sector=data[i][4])
        insert(stock)


if __name__ == '__main__':
    scrap_sp500()
