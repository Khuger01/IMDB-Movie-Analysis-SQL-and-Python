import pandas as pd
from sqlalchemy import create_engine  #facilitates the communication between Python programs and databases

#Function that reads and loads our CSV File
def load_data(path="imdb_top_1000.csv"):
    df = pd.read_csv(path)
    return df

#Data Cleaning 
def preprocess(df):
    df["Runtime"] = df["Runtime"].map(lambda x: x.split(" ")[0])
    df = df.astype({"Runtime": int})

    # replace null values with the most frequently occuring
    df["Certificate"] = df["Certificate"].fillna(df["Certificate"].mode()[0])

    return df

#Create SQLite Database
def create_sqlite_engine(database_name="test.db"):
    engine = create_engine(f"sqlite:///{database_name}")
    engine.connect()
    return engine

#Create Table in database
def add_database_columns(df, engine, table_name="movies"):
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists="replace")

#Loads data to our table 
def add_csv_data_to_sqlite(df, table_name, engine):
    df.to_sql(name=table_name, con=engine, if_exists="append")


def main():
    df = load_data()
    df = preprocess(df)
    engine = create_sqlite_engine(database_name="test.db")
    add_database_columns(df, engine, table_name="movies")
    add_csv_data_to_sqlite(df, table_name="movies", engine=engine)


if __name__ == "__main__":
    main()
    print("Data ingested successfully")
