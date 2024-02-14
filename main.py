import pandas as pd
from my_token import user, password

#для чистого SQL
import psycopg2
import warnings
connection = psycopg2.connect(
    host="ep-ancient-glitter-a2nji5yk.eu-central-1.aws.neon.tech",
    database="mydb",
    user=user,
    password=password
    )

pd.set_option('display.max_columns', None)
warnings.filterwarnings('ignore')

#для SQLAlchemy
import sqlalchemy
from sqlalchemy import URL, create_engine, select, Table, text, asc, desc, distinct, func, cast
from sqlalchemy.orm import aliased
from sqlalchemy.schema import MetaData
connection_string = URL.create(
  'postgresql',
  username=user,
  password= password,
  host='ep-ancient-glitter-a2nji5yk.eu-central-1.aws.neon.tech',
  database='mydb'
)
engine = create_engine(connection_string)

metadata_mydb = MetaData(schema="cd")
metadata_mydb.reflect(bind=engine);


facilities = metadata_mydb.tables["cd.facilities"]
members = metadata_mydb.tables["cd.members"]
bookings = metadata_mydb.tables["cd.bookings"]


sql1 = '''
SELECT distinct surname
  FROM cd.members
  order by surname
  limit 10
'''
df1 = pd.read_sql(sql1, con=connection)  #Read SQL query or database table into a DataFrame.
print('Задание 1, чистый SQL')
print(df1)

#SQLAlchemy
sql_alchemy1=select(distinct(members.c.surname)).order_by(asc(members.c.surname)).limit(10)
with engine.connect() as conn:
    result = conn.execute(sql_alchemy1)
df_alchemy1=pd.DataFrame(result)
print('Задание 1, SQLAlchemy')
print(df_alchemy1)


sql2 = '''
SELECT firstname, surname, date(joindate)
  FROM cd.members m
  order by joindate DESC
  limit 1
'''
df2 = pd.read_sql(sql2, con=connection)
print('\nЗадание 2, чистый SQL')
print(df2)

#SQLAlchemy
sql_alchemy2=select(members.c.firstname, members.c.surname, func.date(members.c.joindate)).order_by(desc(members.c.joindate)).limit(1)
with engine.connect() as conn:
    result = conn.execute(sql_alchemy2)
df_alchemy2=pd.DataFrame(result)
print('Задание 2, SQLAlchemy')
print(df_alchemy2)

sql3 = '''
SELECT f."name", cast(b.starttime as time)
FROM cd.facilities f
join cd.bookings b on b.facid =f.facid
where f."name" like 'Tennis Court%' and date(b.starttime)='2012-09-21'
order by b.starttime
'''
df3 = pd.read_sql(sql3, con=connection)
print('\nЗадание 3, чистый SQL')
print(df3)

#SQLAlchemy
sql_alchemy3=select(facilities.c.name, bookings.c.starttime.cast(sqlalchemy.Time)).select_from(facilities.join(bookings)).where((facilities.c.name.like('Tennis Court%')) &
    (func.date(bookings.c.starttime) == '2012-09-21')).order_by(asc(bookings.c.starttime))
with engine.connect() as conn:
    result = conn.execute(sql_alchemy3)
df_alchemy3=pd.DataFrame(result)
print('Задание 3, SQLAlchemy')
print(df_alchemy3)

sql4 = '''
select m.surname, m.firstname, m2.surname as recomended_by_surname, m2.firstname as recomended_by_firstname
from cd.members m
left join cd.members m2 on m2.memid=m.recommendedby
order by m.surname, m.firstname, m2.surname, m2.firstname
limit 10
'''
df4 = pd.read_sql(sql4, con=connection)
print('\nЗадание 4, чистый SQL')
print(df4)

#SQLAlchemy
members1=aliased(members)
sql_alchemy4=select(members.c.surname, members.c.firstname, members1.c.surname.label('recomended_by_surname'), members1.c.firstname.label('recomended_by_firstname'))\
.select_from(members.join(members1, members.c.recommendedby == members1.c.memid, isouter=True))\
.order_by(asc(members.c.surname)).limit(10)
with engine.connect() as conn:
    result = conn.execute(sql_alchemy4)
df_alchemy4=pd.DataFrame(result)
print('Задание 4, SQLAlchemy')
print(df_alchemy4)

sql5 = '''
select facid, sum(slots) as "total slots"
from cd.bookings
where starttime between date('2012-09-01') and date('2012-10-01')
group by facid
order by "total slots"
'''
df5 = pd.read_sql(sql5, con=connection)
print('\nЗадание 5, чистый SQL')
print(df5)

#SQLAlchemy
sql_alchemy5=select(bookings.c.facid, func.sum(bookings.c.slots).label('total slots')).where((bookings.c.starttime.between('2012-09-01 00:00:00.000','2012-09-30 23:59:59.000'))).group_by(bookings.c.facid)\
.order_by(asc('total slots'))
with engine.connect() as conn:
    result = conn.execute(sql_alchemy5)
df_alchemy5=pd.DataFrame(result)
print('Задание 5, SQLAlchemy')
print(df_alchemy5)

sql6 = '''
select * from
(select m.firstname || ' ' || m.surname as member, f."name" as facility, (CASE
  when b.memid != 0 then f.membercost
  else f.guestcost END) as cost
from cd.bookings b
join cd.members m on b.memid=m.memid
join cd.facilities f on b.facid =f.facid
where date(b.starttime)='2012-09-14')
where cost>30
order by cost
'''
df6 = pd.read_sql(sql6, con=connection)
print('\nЗадание 6, чистый SQL')
print(df6)


