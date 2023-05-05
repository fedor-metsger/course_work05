
from api.hh import HH
from DB.db_manager import DBManager
import pandas as pd

DB_CONNECTION_FILENAME = "db_config.txt"

SELECTED_COMPANIES = ["4712518", "906391", "668994", "2000762", "1111946",
                      "2136954", "1276080", "4596113", "115", "598471"]

def read_connection_data():
    """
    Читает из конфигурационного файла параметры подключения к БД.
    Должны быть в первой строке через пробел:
    <имя или адрес хоста> <имя БД> <имя пользователя> <пароль>
    :return:
    """
    db_host = db_name = user_name = user_password = None
    try:
        with open(DB_CONNECTION_FILENAME, "r", encoding="utf-8") as cf:
            db_host, db_name, user_name, user_password = cf.readline().split()
    except:
        print(f"Ошибка при чтении файла с параметрами БД: {DB_CONNECTION_FILENAME}")
    return db_host, db_name, user_name, user_password

def main():
    """
    main()
    :return:
    """
    db_host, db_name, user_name, user_password = read_connection_data()

    if not user_password:
        print(f'В файле {DB_CONNECTION_FILENAME} в первой строке через пробел должны быть записаны:')
        print(f'host database user password')
        return

    dbm = DBManager(db_host, db_name, user_name, user_password)
    while True:
        inp = input("""Выберите действие (1):
    0. Создать таблицы.
    1. Загрузить вакансии с сайта в БД.
    2. Получить список всех компаний и количество вакансий у каждой компании.
    3. Получить список всех вакансий с указанием названия компании, названия вакансии и зарплаты и ссылки на вакансию.
    4. Получить среднюю зарплату по вакансиям.
    5. Получить список всех вакансий, у которых зарплата выше средней по всем вакансиям.
    6. Получить список всех вакансий, в названии которых содержится ключевое слово. 
    7. Завершить работу программы.
    = > """)

        if inp not in {'0', '1', '2', '3', '4', '5', '6', '7', ''}:
            print("Неправильный ввод. Попробуйте ещё.")
            continue

        if inp == '1' or inp == '':
            print(f'Выполняется загрузка вакансий с сайта Head Hunter\n' \
                  f'по выбранным компаниям: {SELECTED_COMPANIES}\n' \
                  f'в БД {db_name}@{db_host}')
            api = HH("hh.ru", dbm)
            if api.download_vacancies(employers=SELECTED_COMPANIES):
                api.save_vacancies()
        elif inp == '2':
            res = dbm.get_companies_and_vacancies_count()
            if res:
                df = pd.DataFrame(res, columns=['Название компании', 'Количество вакансий'])
                print(df)
            else: print("Запрос вернул пустой результат")
            input("Нажмите любую клавишу...")
        elif inp == '3':
            res = dbm.get_all_vacancies()
            if res:
                df = pd.DataFrame(res, columns=['Название компании', 'Название вакансии',
                                                'Зарлата от', 'Зарплата до', 'URL'])
                pd.set_option('display.max_rows', 100)
                pd.set_option('display.max_columns', 200)
                pd.set_option('display.width', 200)
                print(df)
            else:
                print("Запрос вернул пустой результат")
            input("Нажмите любую клавишу...")
        elif inp == '4':
            res = dbm.get_avg_salary()
            if res:
                df = pd.DataFrame(res, columns=['Средняя заработная плата'])
                print(df)
            else:
                print("Запрос вернул пустой результат")
            input("Нажмите любую клавишу...")
        elif inp == '5':
            res = dbm.get_vacancies_with_higher_salary()
            if res:
                df = pd.DataFrame(res, columns=['Название компании', 'Название вакансии',
                                                'Зарлата (средняя между "от" и "до")'])
                pd.set_option('display.max_rows', 100)
                pd.set_option('display.max_columns', 200)
                pd.set_option('display.width', 200)
                print(df)
            else:
                print("Запрос вернул пустой результат")
            input("Нажмите любую клавишу...")
        elif inp == '6':
            kw = input("Введите ключевое слово: ")
            res = dbm.get_vacancies_with_keyword(kw)
            if res:
                df = pd.DataFrame(res, columns=['Название компании', 'Название вакансии'])
                pd.set_option('display.max_rows', 100)
                pd.set_option('display.max_columns', 200)
                pd.set_option('display.width', 200)
                print(df)
            else:
                print("Запрос вернул пустой результат")
            input("Нажмите любую клавишу...")
        elif inp == '0':
            if dbm.create_db():
                input("Таблицы созданы, нажмите любую клавишу...")
        elif inp == '7':
            return

if __name__ == "__main__":
    main()

