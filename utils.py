

import requests
import datetime
import xml.etree.ElementTree as ET

def contains_number(s: str) -> bool:
    if False in [c.isdigit() for c in s]:
        return False
    return True

class Rates(object):
    DATE = 'date'
    KZT_CODE = 'KZT'
    rss_url = "https://nationalbank.kz/rss/rates_all.xml"
    latest_rates = None
    # supported_currencies = None

    def __init__(self):
        """
        """
        self.latest_rates = self._fetch_rates()

    def _fetch_rates(self):
        """
        Получает XML с сайта
        """
        if self.latest_rates:
            return self.latest_rates
        response = requests.get(self.rss_url)
        rss = ET.fromstring(response.text)
        date = datetime.datetime.strptime(
            response.headers['date'], '%a, %d %b %Y %H:%M:%S %Z')
        return {'rss': rss, 'date': date}

    def _parse_rates_from_rss(self, rss):
        """
        Парсит полученный XML
        """
        rates = dict([
            (item.find('title').text,
                float(item.find('description').text)/float(item.find('quant').text))
            for item in rss.iter('item')
            ])
        # add KZT
        rates[self.KZT_CODE] = 1
        return rates

    def get_exchange_rates(self):
        """
        Загружает данные о курсах валют по отношению к казахскому тенге с сайта казахского банка
        """
        fetched_data = self._fetch_rates()
        rates = self._parse_rates_from_rss(fetched_data['rss'])
        # res_date = fetched_data['date'].strftime('%Y-%m-%d')
        # return {'rates': rates, 'date': res_date}
        return rates
