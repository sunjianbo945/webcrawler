import requests
from pyquery import PyQuery as pq

# AAAAAAAAA
# BBBBBBBBB
# CCCCCCCCC
def main():
    url = 'https://finance.yahoo.com/quote/BA?p=BA'
    headers = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.101 Safari/537.36",
        "accept-language": "en-US,en;q=0.9",
        "accept": "*/*",
        "accept-encoding": "gzip, deflate, br"
    }

    response = requests.get(url, headers=headers)
    html = response.content
    doc = pq(html)
    summaries = doc.find("div#quote-summary")
    rows = summaries.find("tr")

    for row in rows:
        print('{} = {}'.format(pq(row).find('td').eq(0).text(), pq(row).find('td').eq(1).text()))


if __name__ == '__main__':
    main()
