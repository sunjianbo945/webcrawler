from selenium import webdriver
import time
import datetime

from DataLoading.crud import merge_into_crude_oil_inventories


def convert(value):
    if value:
        # determine multiplier
        multiplier = 1
        if value.endswith('K'):
            multiplier = 1000
            value = value[0:len(value)-1] # strip multiplier character
        elif value.endswith('M'):
            multiplier = 1000000
            value = value[0:len(value)-1] # strip multiplier character

        # convert value to float, multiply, then convert the result to int
        return int(float(value) * multiplier)

    else:
        return 0


def load_data():
    url = 'https://www.investing.com/economic-calendar/eia-crude-oil-inventories-75'
    browser = webdriver.Chrome()
    browser.get(url)
    button = browser.find_element_by_xpath("//div[@id='eventTabDiv_history_0']/div[@id='showMoreHistory75']")
    button.click()

    for i in range(0, 50):
        button.click()
        time.sleep(1)

    table = browser.find_element_by_xpath("//div[@id='eventTabDiv_history_0']/table/tbody")
    rows = table.find_elements_by_xpath('.//tr')

    for row in rows[1:]:
        cols = row.find_elements_by_tag_name('td')
        date = datetime.datetime.strptime(cols[0].text, '%b %d, %Y').strftime('%Y-%m-%d')
        actual = convert(cols[2].text)
        exp = convert(cols[3].text)

        merge_into_crude_oil_inventories(date, actual, exp)

    if browser is not None:
        browser.quit()


def main():
    load_data()


if __name__ == '__main__':
    main()