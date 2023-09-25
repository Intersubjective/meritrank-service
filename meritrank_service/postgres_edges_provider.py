import psycopg2


def get_edges_data(postgres_url):
    connection = None
    out_dict = {}
    try:
        connection = psycopg2.connect(postgres_url)
        cursor = connection.cursor()
        cursor.execute("SELECT src, dst, amount FROM edges")
        rows = cursor.fetchall()
        for src, dst, amount in rows:
            out_dict.setdefault(src, {})[dst] = {"weight": amount}

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)

    finally:
        # Close the database connection
        if connection:
            cursor.close()
            connection.close()

    return out_dict
