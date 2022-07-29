import pandas as pd
from pyspark.sql import SparkSession
from sqlalchemy import create_engine

from pyspark.sql.functions import *

appName = "PySpark PostgreSQL Example - via psycopg2"
master = "local"
pg_config = {
    'host': 'localhost',
    'dbname': 'pagila',
    'user': 'postgres',
    'password': '1234',
    'port': '5432',
}


menu = """
1. Вывести количество фильмов в каждой категории, отсортировать по убыванию.
2. Вывести 10 актеров, чьи фильмы большего всего арендовали, отсортировать по убыванию.
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
    spark = SparkSession.builder.master(master).appName(appName).getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    engine = create_engine(
        f"postgresql+psycopg2://{pg_config['user']}:{pg_config['password']}@"
        f"{pg_config['host']}:{pg_config['port']}/{pg_config['dbname']}?client_encoding=utf8",
    )

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

    num_of_task = None
    print(menu)
    while True:

        num_of_task = int(input('Введите номер задания: '))

        if num_of_task == 1:
            fc = f_c.join(category, f_c.category_id == category.category_id, "inner")
            fc.groupBy(category.category_id, category.name).count() \
                .select(col('name').alias('category_name'), col('count').alias('count_of_films')) \
                .sort(col('count_of_films').desc()).show()
        elif num_of_task == 2:
            film.join(film_actor).where(film['film_id'] == film_actor['film_id']) \
                .join(actor).where(film_actor['actor_id'] == actor['actor_id']) \
                .groupBy(actor['actor_id'], actor['first_name'], actor['last_name']) \
                .sum('rental_duration') \
                .select(concat('first_name', 'last_name').alias('actor_name'), 'sum(rental_duration)') \
                .withColumnRenamed('sum(rental_duration)', 'rental_sum') \
                .sort(col('rental_sum').desc()) \
                .show(10)
        elif num_of_task == 3:
            print(film.join(f_c).where(film["film_id"] == f_c["film_id"])
                  .join(category).where(f_c["category_id"] == category["category_id"])
                  .groupBy("name").sum("replacement_cost")
                  .withColumnRenamed("sum(replacement_cost)", "cost").sort(col("cost").desc()).collect()[0].name)
        elif num_of_task == 4:
            film.join(inventory, "film_id", "left_anti").select("title").distinct().sort("film_id").show()
        elif num_of_task == 5:
            actor.join(film_actor).where(actor["actor_id"] == film_actor["actor_id"]) \
                .join(f_c).where(film_actor["film_id"] == f_c["film_id"]) \
                .join(category).where(f_c["category_id"] == category["category_id"]) \
                .filter(category["name"] == 'Children') \
                .groupBy(actor["actor_id"], actor["first_name"], actor["last_name"]) \
                .count().sort(col('count').desc()).select(concat('first_name', 'last_name').alias('actor_name')).show(4)
        elif num_of_task == 6:
            city.join(address, city.city_id == address.city_id, "inner") \
                .join(customer, address.address_id == customer.address_id, "inner") \
                .groupBy('city').agg(
                count(when(col('active') == 1, True)).alias('active'),
                count(when(col('active') == 0, True)).alias('non-active'),
            ).sort(col('active').desc()).show(5)
        elif num_of_task == 7:
            pd_store = pd.read_sql('select * from store', engine)
            store = spark.createDataFrame(pd_store)

            print(film.join(f_c, film['film_id'] == f_c['film_id'], "inner")
                  .join(category, f_c['category_id'] == category['category_id'], "inner")
                  .join(inventory, inventory['film_id'] == film['film_id'], "inner")
                  .join(store, store['store_id'] == inventory['store_id'], "inner")
                  .join(address, address['address_id'] == store['address_id'], "inner")
                  .join(city, city['city_id'] == address['city_id'], "inner")
                  .join(customer, customer['store_id'] == store['store_id'], "inner")
                  .where(film['title'].like("A%"))
                  .groupBy(category['name'], city['city']).count().select(category['name'].alias('category_name'), 'count')
                  .sort(col('count').desc()).collect()[0].category_name)

            film.join(f_c, film['film_id'] == f_c['film_id'], "inner") \
                .join(category, f_c['category_id'] == category['category_id'], "inner") \
                .join(inventory, inventory['film_id'] == film['film_id'], "inner") \
                .join(store, store['store_id'] == inventory['store_id'], "inner") \
                .join(address, address['address_id'] == store['address_id'], "inner") \
                .join(city, city['city_id'] == address['city_id'], "inner") \
                .join(customer, customer['store_id'] == store['store_id'], "inner") \
                .where(film['title'].like("%-%")) \
                .groupBy(category['name'], city['city']).count().select(category['name'].alias('category_name'), 'count') \
                .sort(col('count').desc()).show()
        elif num_of_task == 0:
            break
        else:
            print("Try again, enter the required task")
