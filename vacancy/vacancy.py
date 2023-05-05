
from utils import Rates
class Vacancy:
    """
    Класс для работы с одной вакансией
    """

    curr_rate = {}
    rates = None

    def __init__(self, id: str, name: str, company:str, url: str,
                 descr: str, sal_fr:int, sal_to: int, curr: str, company_id: str):
        if not isinstance(id, str):
            raise TypeError("ID вакансии должно быть строкой")
        if not isinstance(name, str):
            raise TypeError("Название вакансии должно быть строкой")
        if not isinstance(company, str):
            raise TypeError("Название компании должно быть строкой")
        if not isinstance(url, str):
            raise TypeError("URL вакансии должен быть строкой")
        if id == "":
            raise ValueError("ID вакансии должно быть непустой строкой")
        if name == "":
            raise ValueError("Название вакансии должно быть непустой строкой")
        if company == "":
            raise ValueError("Название компании должно быть непустой строкой")
        if url == "":
            raise ValueError("URL вакансии должен быть непустой строкой")
        if company_id == "":
            raise ValueError("ID компании должно быть непустой строкой")
        self.id = id
        self.name = name
        self.url = url
        self.descr = descr
        self.sal_fr = sal_fr
        self.sal_to = sal_to
        self.curr = curr
        self.company = company
        self.company_id = company_id

    def __str__(self):
        return f'Vacancy("{self.name}", "{self.sal_fr}" "{self.curr}", "{self.company}")'

    def to_dict(self):
        return {"id": self.id, "name": self.name, "company": self.company, "url": self.url,
                "descr": self.descr, "sal_fr": self.sal_fr, "sal_to": self.sal_to, "curr": self.curr,
                "company_id": self.company_id}

    @classmethod
    def get_curr_rate(self, curr: str) -> float:
        """
        Получает курс валюты к рублю. Пока заглушка
        :return:
        """
        if curr == "BYR": curr = "BYN"
        if curr in self.curr_rate: return self.curr_rate[curr]
        if not Vacancy.rates: Vacancy.rates = Rates().get_exchange_rates()
        if curr == "KZT": self.curr_rate[curr] = Vacancy.rates["RUB"]
        else: self.curr_rate[curr] = Vacancy.rates["RUB"] / Vacancy.rates[curr]
        return self.curr_rate[curr]

    @classmethod
    def _convert_curr(self, sal: int, curr: str):
        """
        Если валюта не рубль, конвертирует сумму
        :param sal: сумма в валюте
        :param curr: название валюты
        :return:
        """
        if sal is None or sal == 0: return 0
        sal_ret = int(sal / Vacancy.get_curr_rate(curr)) if curr.lower() not in {"rur", "rub"} else sal
        return sal_ret

    def __gt__(self, other) -> bool:
        """
        Сравнивает два класса Vacancy. Сравнение идёт по минимальной заработной плате
        :param other:
        :return:
        """
        if not isinstance(other, Vacancy):
            raise TypeError("Вакансию можно сравнивать только с вакансией")
        self_sal_fr = self._convert_curr(self.sal_fr, self.curr)
        other_sal_fr = self._convert_curr(other.sal_fr, other.curr)
        return self_sal_fr > other_sal_fr

    def __ge__(self, other) -> bool:
        """
        Сравнивает два класса Vacancy. Сравнение идёт по минимальной заработной плате
        :param other:
        :return:
        """
        if not isinstance(other, Vacancy):
            raise TypeError("Вакансию можно сравнивать только с вакансией")
        self_sal_fr = self._convert_curr(self.sal_fr, self.curr)
        other_sal_fr = self._convert_curr(other.sal_fr, other.curr)
        return self_sal_fr >= other_sal_fr

    def __lt__(self, other) -> bool:
        """
        Сравнивает два класса Vacancy. Сравнение идёт по минимальной заработной плате
        :param other:
        :return:
        """
        if not isinstance(other, Vacancy):
            raise TypeError("Вакансию можно сравнивать только с вакансией")
        self_sal_fr = self._convert_curr(self.sal_fr, self.curr)
        other_sal_fr = self._convert_curr(other.sal_fr, other.curr)
        return self_sal_fr < other_sal_fr

    def __le__(self, other) -> bool:
        """
        Сравнивает два класса Vacancy. Сравнение идёт по минимальной заработной плате
        :param other:
        :return:
        """
        if not isinstance(other, Vacancy):
            raise TypeError("Вакансию можно сравнивать только с вакансией")
        self_sal_fr = self._convert_curr(self.sal_fr, self.curr)
        other_sal_fr = self._convert_curr(other.sal_fr, other.curr)
        return self_sal_fr <= other_sal_fr

    def __eq__(self, other) -> bool:
        """
        Сравнивает два класса Vacancy. Сравнение идёт по минимальной заработной плате
        :param other:
        :return:
        """
        if not isinstance(other, Vacancy):
            raise TypeError("Вакансию можно сравнивать только с вакансией")
        self_sal_fr = self._convert_curr(self.sal_fr, self.curr)
        other_sal_fr = self._convert_curr(other.sal_fr, other.curr)
        return self_sal_fr == other_sal_fr

    def __ne__(self, other) -> bool:
        """
        Сравнивает два класса Vacancy. Сравнение идёт по минимальной заработной плате
        :param other:
        :return:
        """
        if not isinstance(other, Vacancy):
            raise TypeError("Вакансию можно сравнивать только с вакансией")
        self_sal_fr = self._convert_curr(self.sal_fr, self.curr)
        other_sal_fr = self._convert_curr(other.sal_fr, other.curr)
        return self_sal_fr != other_sal_fr

    @property
    def converted_sal_to(self):
        """
        Возвращает сконвертированную максимальную зарплату
        :return:
        """
        return self._convert_curr(self.sal_to, self.curr)

    def match_kw(self, kwds: str) -> bool:
        """
        Проверяет, содержит ли вакансия ключевые слова
        :param kw:
        :return:
        """
        for w in kwds.lower().split():
            if self.descr and w in self.descr.lower() or \
                w in self.name.lower() or w in self.company.lower(): return True
        return False


class VacancyCollection():
    """
    Класс для работы с коллекцией вакансий
    """
    def __init__(self):
        self.__vacancies = {}

    def __str__(self):
        return f'VacancyCollection({len(self.__vacancies)} vacancies)'

    @property
    def vacancies(self):
        return self.__vacancies

    def add_vacancy(self, vac: Vacancy) -> bool:
        """
        Добавляет вакансию в коллекцию
        :param vac:
        :return:
        """
        if not isinstance(vac, Vacancy):
            raise TypeError("Можно добавлять только элементы типа Vacancy")
        if vac.id in self.__vacancies:
            raise ValueError("Вакансия с таким ID уже загружена")
        # self.__vacancies[vac.id] = vac.to_dict()
        self.__vacancies[vac.id] = vac

    def clear_vacancies(self):
        """
        Очищает коллекцию
        :return:
        """
        self.__vacancies = {}

    def to_dict(self):
        """
        Возвращает содержимое коллекции в виде словаря
        :return:
        """
        data = {}
        for i in self.__vacancies:
            data[i] = self.__vacancies[i].to_dict()
        return data

    def select_vacancies(self):
        """
        возвращает необходимое количество вакансий отсортированных и отобранных
        в соответствии с пользовательским выбором
        :param us:
        :return:
        """
        data = []
        if us.stype == "FROM_SAL":
            data = sorted(self.__vacancies.values())
        if us.stype == "TO_SAL":
            data = sorted(self.__vacancies.values(), key=lambda v: v.converted_sal_to)

        if us.keyword and us.keyword != "":
            for i in range(len(data) - 1, -1, -1):
                if not data[i].match_kw(us.keyword):
                    del data[i]

        return data[-us.quantity:]

