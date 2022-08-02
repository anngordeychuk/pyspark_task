from tasks import *
from utils import get_db_config, get_or_create_spark_session, create_dataframe

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
    spark = get_or_create_spark_session()

    tables = (
        'film',
        'film_category',
        'category',
        'film_actor',
        'actor',
        'inventory',
        'city',
        'address',
        'customer',
        'store',
    )

    dataframes = {table: create_dataframe(spark, db_config, table) for table in tables}
    dataframes['film'].dropna(how='all')

    num_of_task = None
    print(menu)

    while True:

        num_of_task = int(input('Введите номер задания: '))

        if num_of_task == 1:
            count_of_films_based_on_category(dataframes['film_category'], dataframes['category'],)
        elif num_of_task == 2:
            top_actors(dataframes['film'], dataframes['film_actor'], dataframes['actor'],)
        elif num_of_task == 3:
            top_film_category(dataframes['film'], dataframes['film_category'], dataframes['category'])
        elif num_of_task == 4:
            films_not_exist_in_inventory(dataframes['film'], dataframes['inventory'])
        elif num_of_task == 5:
            top_children_actors(dataframes['actor'], dataframes['film_actor'], dataframes['film_category'], dataframes['category'])
        elif num_of_task == 6:
            analysis_of_cities(dataframes['city'], dataframes['address'], dataframes['customer'])
        elif num_of_task == 7:
            top_rental_onA(dataframes['film'], dataframes['film_category'], dataframes['category'], dataframes['inventory'],
                           dataframes['store'], dataframes['address'], dataframes['city'], dataframes['customer'])
            top_rental(dataframes['film'], dataframes['film_category'], dataframes['category'], dataframes['inventory'], dataframes['store'],
                       dataframes['address'], dataframes['city'], dataframes['customer'])
        elif num_of_task == 0:
            break
        else:
            print("Try again, enter the required task")

# import pyspark.pandas as pd
# engine = get_pg_engine(db_config)
# f_c = create_dataframe(engine, spark, 'select * from film_category')
# category = create_dataframe(engine, spark, 'select * from category')
# film_actor = create_dataframe(engine, spark, 'select * from film_actor')
# actor = create_dataframe(engine, spark, 'select * from actor')
# inventory = create_dataframe(engine, spark, 'select * from inventory')
# city = create_dataframe(engine, spark, 'select * from city')
# address = create_dataframe(engine, spark, 'select * from address')
# customer = create_dataframe(engine, spark, 'select * from customer')
# store = create_dataframe(engine, spark, 'select * from store')
