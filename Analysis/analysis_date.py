from datetime import datetime
from datetime import timedelta


def convert_str_to_date(datestr="", format="%Y-%m-%d"):
    '''
    This function will convert the input date string to date object based on the format
    input :
        datestr : for exmaple '2018-01-01'
        format : for example  "%Y-%m-%d"
    output :
        date object
    no input => will be return today
    '''
    if not datestr:
        return datetime.today().date()
    return datetime.strptime(datestr, format).date()


def next_business_dates(datestr="", format="%Y-%m-%d", n=0):
    '''
     This function will find the the next n business date for given the input date string and format
     input :
         datestr : for exmaple '2018-01-01'
         format : for example  "%Y-%m-%d"
         n : for example 3, next 3 business day. n can be negative meaning previous n days
     output :
         date object
     no input => will be return yesterday's date
     '''

    date = convert_str_to_date(datestr=datestr, format=format)

    if n > 0:
        delta = 1
    elif n < 0:
        delta = -1
    else:
        return date

    for i in range(0, abs(n)):
        date = date + timedelta(days=delta)
        while date.isoweekday() > 5:
            date = date + timedelta(days=delta)


    return date


def convert_date_to_str(date, format="%Y-%m-%d"):
    return date.strftime(format)


