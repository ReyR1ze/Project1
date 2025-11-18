
import sqlite3
import redis


conn = sqlite3.connect('nanoedu.db')
cursor = conn.cursor()


sql_query = """
SELECT r.region_name,
       COUNT(e.full_name) AS equipment_count,
       SUM(e.cost) AS total_cost
FROM table4_equipment e
JOIN sp_region r ON e.region_id = r.region_id
GROUP BY r.region_name;
"""
cursor.execute(sql_query)
rows = cursor.fetchall()


r = redis.Redis(host='localhost', port=6379, db=0)


for row in rows:
    region_name = row[0]
    equipment_count = row[1]
    total_cost = row[2] if row[2] is not None else 0.0
    r.hset(region_name, mapping={
        'equipment_count': equipment_count,
        'total_cost': total_cost
    })


for region in r.keys():
    print(f"Region: {region.decode('utf-8')}, Data: {r.hgetall(region)}")

cursor.close()
conn.close()
