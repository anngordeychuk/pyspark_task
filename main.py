import pandas as pd

from tasks import *
from utils import get_db_config, get_pg_engine, get_or_create_spark_session

menu = """
1. Вывести количество фильмов в каждой категории, отсортировать по убыванию.
2. Вывести 10 актеров, чьи фильмы больше всего арендовали, отсортировать по убыванию.
3. Вывести категорию фильмов, на которую потратили больше всего денег.
4. Вывести названия фильмов, которых нет в inventory.
5. Вывести топ 3 актеров, которые больше всего появлялись в фильмах в категории “Children”.
Если у нескольких актеров одинаковое кол-во фильмов, вывести всех.
6. Вывести города с количеством активных и неактивных клиентов (активный — customer.active = 1).
Отсортировать по количеству неактивных клиентов по убыванию.
7. Вывести категорию фильмов, у которой самое большое кол-во часов
cуммарной аренды в городах (customer.address_id в этом city),
и которые начинаются на букву “a”. Тоже самое сделать для городов в которых есть символ “-”.
0. Выйти из меню.
"""

if __name__ == '__main__':

    db_config = get_db_config()
    engine = get_pg_engine(db_config)
    spark = get_or_create_spark_session()

    pd_film = pd.read_sql('select * from film', engine).dropna(axis='columns', how='all')
    film = spark.createDataFrame(pd_film)

    pd_film_category = pd.read_sql('select * from film_category', engine)
    f_c = spark.createDataFrame(pd_film_category)

    pd_category = pd.read_sql('select * from category', engine)
    category = spark.createDataFrame(pd_category)

    pd_film_actor = pd.read_sql('select * from film_actor', engine)
    film_actor = spark.createDataFrame(pd_film_actor)

    pd_actor = pd.read_sql('select * from actor', engine)
    actor = spark.createDataFrame(pd_actor)

    pd_inv = pd.read_sql('select * from inventory', engine)
    inventory = spark.createDataFrame(pd_inv)

    pd_city = pd.read_sql('select * from city', engine)
    city = spark.createDataFrame(pd_city)

    pd_address = pd.read_sql('select * from address', engine)
    address = spark.createDataFrame(pd_address)

    pd_customer = pd.read_sql('select * from customer', engine)
    customer = spark.createDataFrame(pd_customer)

    pd_store = pd.read_sql('select * from store', engine)
    store = spark.createDataFrame(pd_store)

    num_of_task = None
    print(menu)

    while True:

        num_of_task = int(input('Введите номер задания: '))

        if num_of_task == 1:
            count_of_films_based_on_category(f_c, category)
        elif num_of_task == 2:
            top_actors(film, film_actor, actor)
        elif num_of_task == 3:
            top_film_category(film, f_c, category)
        elif num_of_task == 4:
            films_not_exist_in_inventory(film, inventory)
        elif num_of_task == 5:
            top_children_actors(actor, film_actor, f_c, category)
        elif num_of_task == 6:
            analysis_of_cities(city, address, customer)
        elif num_of_task == 7:
            top_rental_onA(film, f_c, category, inventory, store, address, city, customer)
            top_rental(film, f_c, category, inventory, store, address, city, customer)
        elif num_of_task == 0:
            break
        else:
            print("Try again, enter the required task")
