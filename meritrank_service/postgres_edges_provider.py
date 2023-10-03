import psycopg2

from meritrank_service.log import LOGGER


def get_edges_data(postgres_url):
    connection = None
    out_dict = {}
    count = 0
    try:
        connection = psycopg2.connect(postgres_url)
        cursor = connection.cursor()
        cursor.execute("SELECT src, dst, amount FROM edges")
        rows = cursor.fetchall()
        for src, dst, amount in rows:
            out_dict.setdefault(src, {})[dst] = {"weight": amount}
            count = count + 1

    except (Exception, psycopg2.Error) as error:
        LOGGER.error(f"Error while connecting to PostgreSQL {error}")

    finally:
        # Close the database connection
        if connection:
            cursor.close()
            connection.close()
        LOGGER.info("Got %i edges from DB", count)

    return out_dict
