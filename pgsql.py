import psycopg2
from config import pgsql_config
import json
from datetime import datetime, timedelta

def query(query, values=None):
    # Connect to your postgres DB
    cursor = connect()

    # Execute a query
    if (values):
        cursor.execute(query, values)
    else:
        cursor.execute(query)

        # return query results
        return cursor.fetchall()

# function is used to filter english movies

def movie_english():
        result_title=[]

        with open('/Users/yorkmac040/PycharmProjects/jaya/mohan/python-etl-p2/datasets/json/moviesdata.json', 'r') as g:
            data = json.loads(g.read())
            for i in data:

                if ("English" in i["Language"]):
                    #print(i["Title"])
                    result_title.append(i["Title"])
            return result_title
# function is used to filter columns without n/a
def movie_NO_na():
        result_na=movie_english()
        test_piece=[]
        with open('/Users/yorkmac040/PycharmProjects/jaya/mohan/python-etl-p2/datasets/json/moviesdata.json', 'r') as g:
            data = json.loads(g.read())
            for i in data:
                if(i["Title"]in result_na and i["Rated"] not in ["N/A"] and i["Released"] not in ["N/A"] and i["Runtime"] not in ["N/A"] and i["Genre"]  not in ["N/A"] and i["Director"] not in ["N/A"] and i["Writer"]  not in ["N/A"] and
i["Actors"]  not in ["N/A"] and
i["Plot"] not in ["N/A"] and
i["Awards"] not in ["N/A"] and
i["Poster"] not in ["N/A"]):
                    test_piece.append(i["Title"])
        print(test_piece)
        return test_piece

"""def parse_Title():
    cursor = connect()
    str_array=[]
    result_movies = movie_NO_na()
    print(result_movies)
    for j in result_movies:

            str_array=j
            #print(str_array)

            cursor.execute('insert into petl2.movies(title) values(%s)',[str_array] )"""


"""def parse_Year():
        cursor = connect()
        result_title= []
        #result_movies=movie_english()
        with open('/Users/yorkmac040/PycharmProjects/jaya/mohan/python-etl-p2/datasets/json/moviesdata.json','r') as g:
            data = json.loads(g.read())
            for i in data:
                #print(i)
                if(i["Title"]in result_movies):
                    result_title.append(i["Year"])

            for j in result_title:
                #print(j)
                date = datetime(year=int(j),month=1,day=1)
                #cursor.execute('insert into petl2.movies(released) values(%s)', [date,])"""


"""def parse_Rated():
    cursor = connect()
    result_title = []
    result_movies = movie_english()
    with open('/Users/yorkmac040/PycharmProjects/jaya/mohan/python-etl-p2/datasets/json/moviesdata.json', 'r') as g:
        data = json.loads(g.read())
        for i in data:
            if(i["Title"]in result_movies and i["Rated"] in "R"):
                    result_title.append(i["Rated"])

        print(result_title)
        for i in result_title:
            print([i])"""

def Exclude2018():
    result_title = movie_NO_na()
    result_year=[]
    with open('/Users/yorkmac040/PycharmProjects/jaya/mohan/python-etl-p2/datasets/json/moviesdata.json', 'r') as g:
        data = json.loads(g.read())
        for i in data:

            if (i["Title"] in result_title ):
                # print(i["Title"])
                result_year.append(i["Released"])
        for k in result_year:
            date_object=datetime.strptime(str(k), '%d %b %Y')

        return result_title

            #Function Insert Table is used to insert filtered values .With formatting
def parse_InsertTable():
    cursor = connect()
    result_title = []
    result_value = []
    result_writer = []
    result_runtime= []
    result_rated= []
    result_released = []
    result_director = []
    result_genre = []
    result_plot = []
    result_awards = []
    result_poster = []
    result_movies = movie_NO_na()
    with open('/Users/yorkmac040/PycharmProjects/jaya/mohan/python-etl-p2/datasets/json/moviesdata.json', 'r') as g:
        data = json.loads(g.read())
        for i in data:
            if(i["Title"]in result_movies and i):
                result_title.append(i["Title"])
                result_rated.append(i["Rated"])
                result_released.append(i["Released"])
                result_director.append(i["Director"])
                result_value.append(i["Actors"])
                result_writer.append(i["Writer"])
                result_runtime.append(i["Runtime"])
                result_genre.append(i["Genre"])
                result_plot.append(i["Plot"])
                result_awards.append(i["Awards"])
                result_poster.append(i["Poster"])


        #print(result_title)

        for a,b,c,d,e,f,h,i,j,k,l in zip(result_value,result_title,result_awards,result_genre,result_poster,result_plot,result_director,result_released,result_rated,result_writer,result_runtime):
            str_actor = []
            str_actor.append(a)
            str_title=[]
            str_title.append(b)
            str_runtime=int(l.strip("min"))
            str_awards=[]
            str_awards.append(c)
            str_genre=[]
            str_genre.append(d)
            str_poster=[]
            str_poster.append(e)
            str_plot=[]
            str_plot.append(f)
            str_director=[]
            str_director.append(h)
            datetime_object = datetime.strptime(str(i), '%d %b %Y')
            str_rated=[]
            str_rated.append(j)
            str_writer=[]
            str_writer.append(k)

            cursor.execute('insert into petl2.movies(title,actors,runtime,awards,genre,poster,plot,director,released,rated,writers) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)', [str_title,str_actor,str_runtime,str_awards,str_genre,str_poster,str_plot,str_director,datetime_object,str_rated,str_writer])
            #cursor.execute('Delete from petl2.movies where title in (select title from petl2.movies where extract(year from released)<=2018 )')
"""def parse_genre():
    cursor = connect()
    result_title = []
    result_movies = movie_english()
    with open('/Users/yorkmac040/PycharmProjects/jaya/mohan/python-etl-p2/datasets/json/moviesdata.json', 'r') as g:
        data = json.loads(g.read())
        for i in data:
            if(i["Title"]in result_movies):
             result_title.append(i["Genre"])

        for j in result_title:
            str_array = []
            str_array.append(j)
            print(str_array)
            #cursor.execute('insert into petl2.movies(genre) values(%s)',[str_array])"""

"""def parse_actors():
    cursor = connect()
    result_title = []
    result_movies = movie_NO_na()
    with open('/Users/yorkmac040/PycharmProjects/jaya/mohan/python-etl-p2/datasets/json/moviesdata.json', 'r') as g:
        data = json.loads(g.read())
        for i in data:
            if(i["Title"]in result_movies):
             result_title.append(i["Actors"])

        for j in result_title:
            str_array = []
            str_array.append(j)
            print(str_array)
            cursor.execute('insert into petl2.movies(genre) values(%s)',[str_array])"""


def connect():
    connection = psycopg2.connect(f"""
        host='{pgsql_config['host']}'
        dbname='{pgsql_config['dbname']}'
        user='{pgsql_config['user']}'
        password='{pgsql_config['password']}'
    """)

    # Configure connection
    connection.autocommit = True

    # Return connection cursor to perform database operations
    return connection.cursor()

