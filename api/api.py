
from abc import ABC, abstractmethod
from vacancy.vacancy import Vacancy, VacancyCollection
from DB.db_manager import DBManager

class Engine(ABC):

    def __init__(self, dbm: DBManager):
        self.data = VacancyCollection()
        self.dbm = dbm

    @abstractmethod
    def download_vacancies(self, kw: str) -> bool:
        """
        Осуществляет загрузку данных с сайта.
        Данные загружаются по ключевым словам
        :param kw: ключевые слова через пробел
        :return:
        """
        pass

    @abstractmethod
    def save_vacancies(self):
        """
        Осуществляет сохранение данных в файл
        :param:
        :return:
        """
        pass




