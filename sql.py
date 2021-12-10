insert_movie = ('''
   insert into petl2.movies values(%s,%s,%s,%s)
''')
test_CreateSchema=(''' create schema if not exists  petl2''')
test_CreateMoviesTable= ('''create table if not exists  petl2.Movies(
title  text[] ,
rated  text ,
released  date ,
runtime integer,
genre  text[],
director  text,
writers  text[],
actors  text[],
plot  text,
awards  text,
poster  text )''')


test_CreateTempTable=('''CREATE TEMP TABLE new_contacts(info json)''');
test_updateTable=(''' insert into petl2.Movies(title) values(%s)''')