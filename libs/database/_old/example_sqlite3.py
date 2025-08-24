import sqlite3

connection = sqlite3.connect("store_transaction.db")
cursor = connection.cursor()

# # Получаем список всех таблиц
# cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
# tables = cursor.fetchall()

# # Удаляем каждую таблицу
# for table in tables:
#     table_name = table[0]
#     cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
#     print(f"Таблица удалена: {table_name}")

command1 = """CREATE TABLE IF NOT EXISTS
stores(store_id INTEGER PRIMARY KEY, location TEXT)"""

cursor.execute(command1)

command2 = """CREATE TABLE IF NOT EXISTS
purchases(purchase_id INTEGER PRIMARY KEY, store_id INTEGER, total_cost FLOAT,
FOREIGN KEY(store_id) REFERENCES stores(store_id))"""

cursor.execute(command2)

cursor.execute("INSERT INTO stores VALUES (21, 'Minneapolis, MN')")
cursor.execute("INSERT INTO stores VALUES (95, 'Chicago, IL')")
cursor.execute("INSERT INTO stores VALUES (64, 'Iowa City, IA')")

cursor.execute("INSERT INTO purchases VALUES (54, 21, 15.49)")
cursor.execute("INSERT INTO purchases VALUES (23, 64, 21.12)")

cursor.execute("SELECT * FROM purchases")
result = cursor.fetchall()
print(result)

cursor.execute("UPDATE purchases SET total_cost = 3.67 WHERE purchase_id = 54")
cursor.execute("SELECT * FROM purchases")
result = cursor.fetchall()
print(result)

cursor.execute("DELETE FROM purchases WHERE purchase_id = 54")
cursor.execute("SELECT * FROM purchases")
result = cursor.fetchall()
print(result)

connection.commit()
connection.close()

with cursor:
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    
    for table in tables:
        cursor.execute(f"DROP TABLE IF EXISTS {table[0]}")