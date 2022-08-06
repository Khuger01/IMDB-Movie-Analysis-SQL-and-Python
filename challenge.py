import argparse
import pandas as pd
from data_ingestion import create_sqlite_engine


def command_line_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", help="Name of the database", required=True)
    parser.add_argument("--table", help="Name of table in the database", required=True)
    parser.add_argument("--query", help="Query to be executed on the database", required=True)
    parser.add_argument(
        "--seed", help="Seed setting for reproducibility",
    )
    parser.add_argument("--year", help="Release year of movie")
    parser.add_argument("--movie", help="Name of movie")
    args = parser.parse_args()
    return args


class QueryDatabase:
    def __init__(self, engine):
        self.engine = engine
#Top 10 movies based on IMDB rating
    def get_top_10_movies(self):
        query = "SELECT Series_Title,IMDB_Rating FROM movies LIMIT 10;"
        df = pd.read_sql(query, self.engine)
        print(df)
#Top 10 lead actors (Star1 field) based on their movies average IMDB rating
    def get_top_10_actors(self):
        query = """SELECT Star1, AVG(IMDB_Rating) AS AVG_IMDB_Rating FROM movies
                GROUP BY Star1
                ORDER BY AVG_IMDB_Rating DESC
                LIMIT 10;"""
        df = pd.read_sql(query, self.engine)
        print(df)
#Given a user input value of a year (e.g. 2019) determine the longest running movie of the year and return it, along with its runtime in hours
    def get_longest_movie(self, year):
        query = f"""SELECT Series_Title, ROUND(Runtime/60.0,2) || ' hrs' As Runtime  FROM movies
                WHERE Released_Year = {year}
                ORDER BY Runtime DESC
                LIMIT 1;
                """
        df = pd.read_sql(query, self.engine)
        print(df)
#Given a user input value of a year (e.g. 1990) return all the movies from that year, sorted by IMDB rating
    def get_movies_for_year_specified(self, year):
        query = f"""SELECT Series_Title, IMDB_Rating  FROM movies
                WHERE Released_Year = {year}
                ORDER BY IMDB_Rating DESC
                """
        df = pd.read_sql(query, self.engine)
        print(df)
#Determine the year that had the highest grossing movies and return it, along with the average gross of movies that year
    def get_gross_year(self):
        query = """SELECT Released_Year, AVG(Gross) AS AVG_Gross  FROM movies
                GROUP BY Released_Year
                ORDER BY AVG_Gross DESC
                LIMIT 1;
                """
        df = pd.read_sql(query, self.engine)
        print(df)
#Given a user input of part of a movie name (e.g. lord of the rings) return all matching movies and their IMDB rating
    def find_movie(self, movie):
        query = f"""SELECT Series_Title, IMDB_Rating  FROM movies
                WHERE Series_Title LIKE '%{movie}%'
                """
        df = pd.read_sql(query, self.engine)
        print(df)


def main(args):
    engine = create_sqlite_engine()
    query_database = QueryDatabase(engine)

    if args.database == "test" and args.table == "movies":
        if args.query == "top_10_movies":
            query_database.get_top_10_movies()
        if args.query == "top_10_actors":
            query_database.get_top_10_actors()
        if args.query == "year":
            query_database.get_movies_for_year_specified(args.year)
        if args.query == "longest_movie":
            query_database.get_longest_movie(args.year)
        if args.query == "gross_year":
            query_database.get_gross_year()
        if args.query == "find_movie":
            query_database.find_movie(args.movie)
    else:
        print("Enter the right database and table name")


if __name__ == "__main__":
    args = command_line_args()
    main(args)
