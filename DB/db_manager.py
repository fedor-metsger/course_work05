
import psycopg2

class DBManager:
    """
    Класс для работы с БД
    """
    def __init__(self, db_host: str, db_name: str, user_name: str, user_password: str):
        self.__conn = psycopg2.connect(host=db_host, database=db_name, user=user_name, password=user_password)

    def create_db(self):
        try:
            with self.__conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE employer (
                        employer_id varchar(20) PRIMARY KEY,
                        title VARCHAR(50) NOT NULL
                    );       
                """)
                cur.execute("""
                    CREATE TABLE vacancy (
                        vacancy_id VARCHAR(20) PRIMARY KEY,
                        title VARCHAR(100) NOT NULL,
                        company_id VARCHAR(20) REFERENCES employer(employer_id),
                        url VARCHAR(100) NOT NULL,
                        descr TEXT,
                        sal_fr INT,
                        sal_to INT,
                        curr VARCHAR(5)
                    );       
                """)
            self.__conn.commit()
        except Exception as e:
            print(f'Ошибка при создании таблиц БД: "{str(e)}""')
            return False
        return True

    def add_employer(self, id: str, title: str) -> str:
        with self.__conn.cursor() as cur:
            cur.execute("""
                INSERT INTO employer (employer_id, title) VALUES (%s, %s) RETURNING employer_id
            """, (id, title))
            id = cur.fetchone()[0]
        self.__conn.commit()
        return id

    def get_employer(self, id: str) -> dict:
        res = None
        with self.__conn.cursor() as cur:
            cur.execute("SELECT * FROM employer WHERE employer_id = %s", (id,))
            res = cur.fetchone()
        if isinstance(res, tuple):
            return res

    def get_tables(self) -> list:
        res = None
        with self.__conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM pg_tables
                 WHERE schemaname = 'public'
            """, (id))
            res = cur.fetchall()
        if isinstance(res, list):
            return res

    def del_vacancy(self, id: str):
        with self.__conn.cursor() as cur:
            cur.execute("DELETE FROM vacancy WHERE vacancy_id = %s", (id,))

    def add_vacancy(self, id: str, name: str, company_id: str, company: str,
                    url: str, descr: str, sal_fr: int, sal_to: int, curr: str, site: str) -> str:
        with self.__conn.cursor() as cur:
            self.del_vacancy(id)
            if not self.get_employer(company_id):
                self.add_employer(company_id, company)
            cur.execute("""
                INSERT INTO vacancy (vacancy_id, title, company_id, url, descr, sal_fr, sal_to, curr)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING vacancy_id
            """, (id, name, company_id, url, descr, sal_fr, sal_to, curr))
            id = cur.fetchone()[0]
        return id

    def dump(self, data: dict):
        """
        Записывает данные в БД
        """
        for v in data.values():
            self.add_vacancy(**v)
        self.__conn.commit()

    def get_companies_and_vacancies_count(self) -> list:
        """
        Получает список всех компаний и количество вакансий у каждой компании.
        """
        with self.__conn.cursor() as cur:
            cur.execute("""
                SELECT e.title, v.cnt
                  FROM employer e,
                        (SELECT company_id, COUNT(vacancy_id) cnt
                          FROM vacancy
                         GROUP BY company_id) v
                 WHERE e.employer_id = v.company_id;""")
            res = cur.fetchall()
        return res

    def get_all_vacancies(self) -> list:
        """
        Получает список всех компаний и количество вакансий у каждой компании.
        """
        with self.__conn.cursor() as cur:
            cur.execute("""
                SELECT e.title, v.title, v.sal_fr, v.sal_to, v.url
                  FROM employer e, vacancy v
                 WHERE e.employer_id = v.company_id;""")
            res = cur.fetchall()
        return res

    def get_avg_salary(self) -> list:
        """
        Получает среднюю зарплату по вакансиям.
        """
        with self.__conn.cursor() as cur:
            cur.execute("""
                SELECT ROUND(AVG(v.sal), 2)
                  FROM (
                        SELECT sal_fr sal
                          FROM vacancy
                         WHERE sal_fr IS NOT NULL
                           AND sal_to IS NULL
                         UNION
                        SELECT sal_to sal
                          FROM vacancy
                         WHERE sal_fr IS NULL
                           AND sal_to IS NOT NULL
                         UNION
                        SELECT (sal_fr + sal_to) / 2 sal
                          FROM vacancy
                         WHERE sal_fr IS NOT NULL
                           AND sal_to IS NOT NULL
                ) v;""")
            res = cur.fetchall()
        return res

    def get_vacancies_with_higher_salary(self) -> list:
        """
        Получает список всех вакансий, у которых зарплата выше средней по всем вакансиям.
        """
        with self.__conn.cursor() as cur:
            cur.execute("""
                SELECT e.title, v.title, v.sal
                  FROM (
                        SELECT title, company_id, sal_fr sal
                          FROM vacancy
                         WHERE sal_fr IS NOT NULL
                           AND sal_to IS NULL
                         UNION
                        SELECT title, company_id, sal_to sal
                          FROM vacancy
                         WHERE sal_fr IS NULL
                           AND sal_to IS NOT NULL
                         UNION
                        SELECT title, company_id, (sal_fr + sal_to) / 2 sal
                          FROM vacancy
                         WHERE sal_fr IS NOT NULL
                           AND sal_to IS NOT NULL
                ) v
                  JOIN employer e 
                    ON e.employer_id = v.company_id
                 WHERE v.sal > (
                        SELECT AVG(v.sal)
                          FROM (
                                SELECT sal_fr sal
                                  FROM vacancy
                                 WHERE sal_fr IS NOT NULL
                                   AND sal_to IS NULL
                                 UNION
                                SELECT sal_to sal
                                  FROM vacancy
                                 WHERE sal_fr IS NULL
                                   AND sal_to IS NOT NULL
                                 UNION
                                SELECT (sal_fr + sal_to) / 2 sal
                                  FROM vacancy
                                 WHERE sal_fr IS NOT NULL
                                   AND sal_to IS NOT NULL
                        ) v
                )
                 ORDER BY v.sal;""")
            res = cur.fetchall()
        return res

    def get_vacancies_with_keyword(self, kw: str) -> list:
        """
        Получает список всех вакансий, в названии которых содержатся ключевое слово.
        """
        with self.__conn.cursor() as cur:
            cur.execute("""
                SELECT e.title, v.title
                  FROM vacancy v
                  JOIN employer e 
                    ON e.employer_id = v.company_id
                 WHERE LOWER(v.title) LIKE %s;""", (f'%{kw.lower()}%',))
            res = cur.fetchall()
        return res

def main():
    dbm = DBManager("192.168.1.43", "hh", "postgres", "postgres")
    # dbm.create_db()
    # print(dbm.get_tables())
    # dbm.add_employer("1", "Рога и Копыта")
    print(dbm.get_employer("2"))
    # dbm.add_vacancy("1", "Погромист", "2", "url://", "", 1000, 20000, "RUR", "Майкрософт")
    dbm.dump({"id": "3"})

if __name__ == "__main__":
    main()