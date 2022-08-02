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

    film = create_dataframe(spark, db_config, 'film')
    film.dropna(how='all')

    f_c = create_dataframe(spark, db_config, 'film_category')
    category = create_dataframe(spark, db_config, 'category')
    film_actor = create_dataframe(spark, db_config, 'film_actor')
    actor = create_dataframe(spark, db_config, 'actor')
    inventory = create_dataframe(spark, db_config, 'inventory')
    city = create_dataframe(spark, db_config, 'city')
    address = create_dataframe(spark, db_config, 'address')
    customer = create_dataframe(spark, db_config, 'customer')
    store = create_dataframe(spark, db_config, 'store')

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
