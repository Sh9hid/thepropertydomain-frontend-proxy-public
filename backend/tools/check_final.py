import sqlite3
conn = sqlite3.connect('backend/leads.db')
c = conn.cursor()
c.execute('SELECT count(*) FROM leads')
print(f'Lead Count: {c.fetchone()[0]}')
c.execute('SELECT owner_name FROM leads')
rows = c.fetchall()
for row in rows:
    print(f"- {row[0]}")
conn.close()
