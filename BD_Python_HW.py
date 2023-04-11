import psycopg2


def clear_tables(conn):
    with conn.cursor() as cur:
        cur.execute("""
        TRUNCATE TABLE telNumbers;
        """)
        cur.execute("""
        TRUNCATE TABLE clients CASCADE;
        """)
        cur.execute("""
        ALTER SEQUENCE clients_id_seq RESTART WITH 1;
        """)
        cur.execute("""
        ALTER SEQUENCE telNumbers_id_seq RESTART WITH 1;
        """)
    conn.commit()


def create_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS clients(
        id SERIAL PRIMARY KEY,
        first_name VARCHAR(40),
        second_name VARCHAR(40),
        email VARCHAR(40)
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS telNumbers(
        id SERIAL PRIMARY KEY,
        client_id INTEGER NOT NULL REFERENCES clients(id),
        tel_number VARCHAR(20)
        );
        """)
    conn.commit()


def add_client(conn, first_name, second_name, email, phones=None):
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO clients(first_name, second_name, email) VALUES(%s, %s, %s);
        """, (first_name, second_name, email))
        cur.execute("""
        SELECT id FROM clients WHERE email = %s
        """, (email,))
        client_id = cur.fetchone()[0]
        for phone in phones:
            cur.execute("""
            INSERT INTO telNumbers(client_id, tel_number) VALUES(%s, %s);
            """, (client_id, phone))
    conn.commit()


def add_phone(conn, client_id, phone):
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO telNumbers(client_id, tel_number) VALUES(%s, %s);
        """, (client_id, phone))
    conn.commit()
    pass


def change_client(conn, client_id, first_name=None, second_name=None, email=None, phones=None):
    client_characteristics = {'first_name': first_name, 'second_name': second_name, 'email': email}
    with conn.cursor() as cur:
        for key, value in client_characteristics.items():
            if not isinstance(value, type(None)):
                cur.execute("""
                UPDATE clients SET %s = '%s' WHERE id = %s;
                """ % (key, value, client_id))
        if not isinstance(phones, type(None)):
            cur.execute("""
                DELETE FROM telNumbers WHERE client_id = %s;
                """, (client_id, ))
            for phone in phones:
                add_phone(conn, client_id, phone)
    conn.commit()
    pass


def delete_phone(conn, client_id, phone):
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM telNumbers WHERE client_id = %s AND tel_number = %s;
            """, (client_id, phone))
    conn.commit()
    pass


def delete_client(conn, client_id):
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM telNumbers WHERE client_id = %s;
            """, (client_id, ))
        cur.execute("""
            DELETE FROM clients WHERE id = %s;
            """, (client_id, ))
    conn.commit()


def find_client(conn, first_name=None, second_name=None, email=None, phone=None):
    client_characteristics = {'first_name': first_name, 'second_name': second_name, 'email': email, 'tel_number': phone}
    with conn.cursor() as cur:
        for key, value in client_characteristics.items():
            if not isinstance(value, type(None)):
                cur.execute("""
                SELECT first_name, second_name, email, tel_number FROM clients c
                LEFT JOIN telNumbers t ON t.client_id = c.id
                WHERE %s = '%s'
                """ % (key, value))
                print(cur.fetchall())


def print_tables(conn):
    with conn.cursor() as cur:
        cur.execute("""
        SELECT * FROM clients;
        """)
        print(cur.fetchall())
    with conn.cursor() as cur:
        cur.execute("""
        SELECT * FROM telNumbers;
        """)
        print(cur.fetchall())


with psycopg2.connect(database="DB_Python_HW", user="postgres", password="_Gfgfvfvf2") as conn:
    create_db(conn)
    clear_tables(conn)

    add_client(conn, "Ivan", "Ivanov", "II@mail.ru", ("+7911", "+7112"))
    add_client(conn, "Misha", "Mishin", "MM@rambler.ru", ("+911", "+103", "+900"))
    add_client(conn, "Ilia", "Iliich", "IIl@yandex.com", ("+1917", ))
    print_tables(conn)
    print()

    add_phone(conn, 1, '+1')
    print_tables(conn)
    print()

    change_client(conn, 1, first_name="Petr", email="PI@mail.ru", phones=("+2", "+3", "+4"))
    print_tables(conn)
    print()

    delete_phone(conn, 1, "+2")
    print_tables(conn)
    print()

    delete_client(conn, 2)
    print_tables(conn)
    print()

    find_client(conn, phone="+4")
    find_client(conn, first_name="Ilia")

conn.close()
