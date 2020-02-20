import sys
import pandas as pd
import datetime
from sqlalchemy import create_engine


def load_data(movies_url, reviews_url):
    movies = pd.read_csv(movies_url, delimiter='::', header=None, names=['movie_id', 'movie', 'genre'],
                         dtype={'movie_id': object}, encoding="utf8", engine='python')
    reviews = pd.read_csv(reviews_url, delimiter='::', header=None,
                          names=['user_id', 'movie_id', 'rating', 'timestamp'],
                          dtype={'movie_id': object, 'user_id': object, 'timestamp': object}, encoding="utf8",
                          engine='python')

    genres = []

    for val in movies.genre:
        genres.extend(str(val).split('|'))

    genres = set(genres)

    return movies, reviews, genres


def extract_date(movie):
    date = str(movie)[-5:-1]
    return date


def century_encode(year, century):
    if year in range((century-1)*100, century*100):
        return 1
    else:
        return 0


def clean_movies(movies_df, genres):

    movies_df['date'] = movies_df['movie'].apply(extract_date).astype(int)
    movies_df['1800s'] = movies_df['date'].apply(century_encode, args = [19])
    movies_df['1900s'] = movies_df['date'].apply(century_encode, args = [20])
    movies_df['2000s'] = movies_df['date'].apply(century_encode, args = [21])

    def split_genres(val):
        try:
            if val.find(gene) >-1:
                return 1
            else:
                return 0
        except AttributeError:
            return 0

    for gene in genres:
        movies_df[gene] = movies_df['genre'].apply(split_genres)

    return movies_df


def clean_reviews(reviews_df):
    reviews_df['date'] = reviews_df['timestamp'].apply(
        lambda val: datetime.datetime.fromtimestamp(int(val)).strftime('%Y-%m-%d %H:%M:%S'))

    return reviews_df


def save_data(df, database_filename, database_tablename):
    """
    This function loads the cleaned dataframe to SQLite database.
    Arguments:
        df <- Cleaned DataFrame
        database_filename <- filename of the .db file
    """
    engine = create_engine('sqlite:///' + database_filename)
    df.to_sql(database_tablename, engine, index=False)


def main():
    """
    Main Data Processing function
    This function implement the ETL pipeline:
        1) Data extraction from web
        2) Data cleaning and pre-processing
        3) Data loading to SQLite database
    """
    print(sys.argv)
    if len(sys.argv) == 4:

        movies_url, reviews_url, database_filepath = sys.argv[1:]

        print('Loading data...\n    MOVIES: {}\n    REVIEWS: {}'
              .format(movies_url, reviews_url))
        movies, reviews, genres = load_data(movies_url, reviews_url)

        print('Cleaning data...')
        movies = clean_movies(movies, genres)
        reviews = clean_reviews(reviews)

        print('Saving data...\n    DATABASE: {}'.format(database_filepath))
        save_data(movies, database_filepath, 'movies')
        save_data(reviews, database_filepath, 'reviews')

        print('Cleaned data saved to database!')

    else:
        print('Please provide the url of the movies and reviews ' \
              'datasets as the first and second argument respectively, as ' \
              'well as the filepath of the database to save the cleaned data ' \
              'to as the third argument. \n\nExample: etl.py ' \
              'https://raw.githubusercontent.com/sidooms/MovieTweetings/master/latest/movies.dat' \
              'https://raw.githubusercontent.com/sidooms/MovieTweetings/master/latest/ratings.dat' \
              'TwitterMovies.db')


if __name__ == '__main__':
    main()