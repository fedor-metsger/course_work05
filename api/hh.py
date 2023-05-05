
import requests
from api.api import Engine
from vacancy.hh import HHVacancy
from DB.db_manager import DBManager

PAGE_SIZE = 100

class HH(Engine):
    def __init__(self, site: str, dbm: DBManager):
        Engine.__init__(self, dbm)
        self.site = site

    def __str__(self):
        return f'HH({self.data})'

    def __download_vacancies_page(self, kw: str, employers: list, pg: int):
        """
        Производит загрузку одной страницы данных с сайта hh.ru
        :param site: сайт
        :param kw: ключевые слова
        :param pg: номер страницы
        :return:
        """
        url = "https://api.hh.ru/vacancies"
        params = {
            'host': self.site,
            "text": kw,
            # "area": [1, 2],
            "employer_id": tuple(employers),
            "currency": "RUR",
            "only_with_salary": True,
            "page": pg,
            "per_page": PAGE_SIZE
        }
        count, pages = 0, 0
        try:
            response = requests.get(url, params=params)
            if response.status_code != 200:
                print("Ошибка при запросе данных с сайта HeadHunter: Status code ", response.status_code)
                return None, None

            pages = response.json()["pages"]
            for i in response.json()["items"]:
                # print(i["id"])
                company = company_id = descr = salary_from = salary_to = currency = city = None
                if isinstance(i["employer"], dict):
                    company_id = i["employer"]["id"]
                    company = i["employer"]["name"]
                if isinstance(i["snippet"], dict): descr = i["snippet"]["responsibility"]
                if isinstance(i["area"], dict): city = i["area"]["name"]
                if isinstance(i["salary"], dict):
                    salary_from = i["salary"]["from"]
                    salary_to = i["salary"]["to"]
                    currency = i["salary"]["currency"]

                vac = HHVacancy(i["id"], i["name"], company, f'https://hh.ru/vacancy/{i["id"]}',
                                descr, salary_from, salary_to, currency, company_id, self.site)
                self.data.add_vacancy(vac)
                count += 1
        except Exception as e:
            print("Ошибка при запросе данных с сайта HeadHunter:", repr(e))
            return None, None

        return count, pages

    def download_vacancies(self, kw='', employers=[]) -> bool:
        """
        Производит загрузку данных с сайта hh.ru
        :param site: сайт
        :param kw: ключевые слова
        :return:
        """
        if not isinstance(kw, str):
            raise ValueError("Ключевые слова должны задаваться строкой")
        if not isinstance(employers, list):
            raise ValueError("Работодатели должны задаваться списком")

        # print(f'Выполняется загрузка вакансий с сайта HeadHunter c ключевыми словами "{kw}"')

        count, pg = 0, 0
        while True:
            ret, ret_pages = self.__download_vacancies_page(kw, employers, pg)
            if not ret: return False
            if self.data == None:
                print(f"Нет вакансий с такими ключевыми словами")
                return False
            pg += 1
            count += ret
            if pg == ret_pages:
                print(f'Загружено {count} вакансий')
                return True
            if count >= 100:
                print(f'Загрузка временно ограничена 100 вакансий. Загружено {count} вакансий')
                return True

    def save_vacancies(self):
        dict = self.data.to_dict()
        self.dbm.dump(dict)
        print(f'Сохранено {len(dict)} вакансий')

