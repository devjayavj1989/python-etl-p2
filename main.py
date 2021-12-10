import pgsql
import sql
import json
import requests

def get_movie_data(title):
    headers = {"Authorization": "9855f49b"}
    request_url = f"https://www.omdbapi.com/?t={title}&apikey=686eed26"
    return requests.get(request_url, headers=headers).json()

if __name__ == '__main__':

 try:
# function used to filter title for 2018 movies
    def parse():
        result_title=[]
        with open('/Users/yorkmac040/PycharmProjects/jaya/mohan/python-etl-p2/datasets/json/movies.json','r') as g:
            data = json.loads(g.read())
            for i in data:
                if (i["year"]>=2018):
                   # print(i["title"])
                    result_title.append( i["title"])
            return result_title

# removing duplicates
    def removingDuplicates():

        result_title=parse()
        result_title2=[]
        result_title2=set(result_title)
        return result_title2

#Api call
    def functionCall():
        api_result=[]
        return_withoutDuplicates= removingDuplicates()
        for i in return_withoutDuplicates:
            api_result.append(get_movie_data(i))

        with open('/Users/yorkmac040/PycharmProjects/jaya/mohan/python-etl-p2/datasets/json/moviesdata2.json','w') as f:
            json.dump(api_result, f)


    functionCall()


    pgsql.query(sql.test_CreateSchema, ["schema"])
    pgsql.query(sql.test_CreateMoviesTable, ["tables"])
    pgsql.parse_Director()
 except ValueError as ve:
    print(ve)
