import psycopg2
from psycopg2 import Error

"""
Test to see if we can connect to the postgresql database using the postgresql
python client library.
"""

def connect_to_db():
    connection = None

    try:
    # Connect to an existing database
        connection = psycopg2.connect(
            user='ge_user', 
            password='pG9tC4qI5lE3sB2e',
            host='localhost', 
            port='5432', 
            database='expectations_store'
        )

    # Create a cursor to perform database operations
        cursor = connection.cursor()
        # Print PostgreSQL details
        print("PostgreSQL server information")
        print(connection.get_dsn_parameters(), "\n")
        # Executing a SQL query
        cursor.execute("SELECT version();")
        # Fetch result
        record = cursor.fetchone()
        print("You are connected to - ", record, "\n")

    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)

    finally:
        if (connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")

if __name__ == '__main__':
    connect_to_db()