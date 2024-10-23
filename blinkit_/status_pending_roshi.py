import pymysql

# Connect to the database
conn = pymysql.connect(
    host='localhost',
    user='root',
    password='actowiz',
    database='blinkit_'
)

cur = conn.cursor()
update_query="UPDATE `blinkit_links_roshi` SET `status`='pending'"
cur.execute(update_query)
conn.commit()
print("table updated successfully")
