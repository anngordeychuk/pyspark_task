from pyspark.sql.functions import DataFrame, col, concat, count, when


def count_of_films_based_on_category(film_category_df: DataFrame, category_df: DataFrame):
    """Вывести количество фильмов в каждой категории, отсортировать по убыванию."""
    fc = film_category_df.join(
        category_df,
        film_category_df.category_id == category_df.category_id,
        "inner",
    )
    fc.groupBy(
        category_df.category_id,
        category_df.name,
    ).count().select(
        col('name').alias('category_name'),
        col('count').alias('count_of_films'),
    ).sort(
        col('count_of_films').desc(),
    ).show()


def top_actors(film_df: DataFrame, film_actor_df: DataFrame, actor_df: DataFrame):
    """Вывести 10 актеров, чьи фильмы больше всего арендовали, отсортировать по убыванию."""
    film_df.join(
        film_actor_df,
    ).where(
        film_df['film_id'] == film_actor_df['film_id'],
    ).join(
        actor_df,
    ).where(
        film_actor_df['actor_id'] == actor_df['actor_id']
    ).groupBy(
        actor_df['actor_id'],
        actor_df['first_name'],
        actor_df['last_name'],
    ).sum(
        'rental_duration'
    ).select(
        concat('first_name', 'last_name').alias('actor_name'),
        'sum(rental_duration)',
    ).withColumnRenamed(
        'sum(rental_duration)',
        'rental_sum'
    ).sort(
        col('rental_sum').desc()
    ).show(10)


def top_film_category(film_df: DataFrame, film_category_df: DataFrame, category_df: DataFrame):
    """Вывести категорию фильмов, на которую потратили больше всего денег."""
    print(
    film_df.join(
        film_category_df
    ).where(
        film_df['film_id'] == film_category_df['film_id']
    ).join(
        category_df
    ).where(
    film_category_df['category_id'] == category_df['category_id']
    ).groupBy(
        'name'
    ).sum(
        'replacement_cost'
    ).withColumnRenamed(
        'sum(replacement_cost)', 'cost'
    ).sort(
        col('cost').desc()
    ).collect()[0].name)

def films_not_exist_in_inventory(film_df: DataFrame, inventory_df:DataFrame):
    """Вывести названия фильмов, которых нет в inventory."""
    film_df.join(
        inventory_df, 'film_id', 'left_anti'
    ).select(
        'title'
    ).distinct(
    ).sort(
        'film_id'
    ).show()


def top_children_actors(actor_df: DataFrame, film_actor_df: DataFrame, film_category_df:DataFrame, category_df: DataFrame):
    """Вывести топ 3 актеров, которые больше всего появлялись в фильмах в категории “Children”.
    Если у нескольких актеров одинаковое кол-во фильмов, вывести всех."""
    actor_df.join(
        film_actor_df
    ).where(
        actor_df['actor_id'] == film_actor_df['actor_id']
    ).join(
        film_category_df
    ).where(
        film_actor_df['film_id'] == film_category_df['film_id']
    ).join(
        category_df
    ).where(
        film_category_df['category_id'] == category_df['category_id']
    ).filter(
        category_df["name"] == 'Children'
    ).groupBy(
        actor_df['actor_id'],
        actor_df['first_name'],
        actor_df['last_name']
    ).count(
    ).sort(
        col('count').desc()
    ).select(
        concat('first_name', 'last_name').alias('actor_name')
    ).show(4)


def analysis_of_cities(city_df: DataFrame, address_df: DataFrame, customer_df: DataFrame):
    """Вывести города с количеством активных и неактивных клиентов (активный — customer.active = 1).
    Отсортировать по количеству неактивных клиентов по убыванию."""
    city_df.join(
        address_df, city_df.city_id == address_df.city_id, 'inner'
    ).join(
        customer_df, address_df.address_id == customer_df.address_id, 'inner'
    ).groupBy(
        'city'
    ).agg(
        count(when(col('active') == 1, True)).alias('active'),
        count(when(col('active') == 0, True)).alias('non-active'),
    ).sort(
        col('active').desc()
    ).show(5)

def top_rental_onA(film_df: DataFrame, film_category_df: DataFrame, category_df: DataFrame, inventory_df: DataFrame,
           store_df: DataFrame, address_df: DataFrame, city_df: DataFrame, customer_df: DataFrame):
    """Вывести категорию фильмов, у которой самое большое кол-во часов
    cуммарной аренды в городах (customer.address_id в этом city),
    и которые начинаются на букву “a”. Тоже самое сделать для городов в которых есть символ “-”."""
    print(
    film_df.join(
        film_category_df, film_df['film_id'] == film_category_df['film_id'], 'inner'
    ).join(
        category_df, film_category_df['category_id'] == category_df['category_id'], 'inner'
    ).join(
        inventory_df, inventory_df['film_id'] == film_df['film_id'], 'inner'
    ).join(
        store_df, store_df['store_id'] == inventory_df['store_id'], 'inner'
    ).join(
        address_df, address_df['address_id'] == store_df['address_id'], 'inner'
    ).join(
        city_df, city_df['city_id'] == address_df['city_id'], 'inner'
    ).join(
        customer_df, customer_df['store_id'] == store_df['store_id'], 'inner'
    ).where(
        film_df['title'].like("A%")
    ).groupBy(
        category_df['name'],
        city_df['city']
    ).count(
    ).select(
        category_df['name'].alias('category_name'), 'count'
    ).sort(
        col('count').desc()
    ).collect(
    )[0].category_name)

def top_rental(film_df: DataFrame, film_category_df: DataFrame, category_df: DataFrame, inventory_df: DataFrame,
           store_df: DataFrame, address_df: DataFrame, city_df: DataFrame, customer_df: DataFrame):
    film_df.join(
        film_category_df, film_df['film_id'] == film_category_df['film_id'], 'inner'
    ).join(
        category_df, film_category_df['category_id'] == category_df['category_id'], 'inner'
    ).join(
        inventory_df, inventory_df['film_id'] == film_df['film_id'], 'inner'
    ).join(
        store_df, store_df['store_id'] == inventory_df['store_id'], 'inner'
    ).join(
        address_df, address_df['address_id'] == store_df['address_id'], 'inner'
    ).join(
        city_df, city_df['city_id'] == address_df['city_id'], 'inner'
    ).join(
        customer_df, customer_df['store_id'] == store_df['store_id'], 'inner'
    ).where(
        film_df['title'].like("%-%")
    ).groupBy(
        category_df['name'],
        city_df['city']
    ).count(
    ).select(
        category_df['name'].alias('category_name'), 'count'
    ).sort(
        col('count').desc()
    ).show()

