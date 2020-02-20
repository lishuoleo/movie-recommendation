import sys
from sqlalchemy import create_engine
import pandas as pd


def load_data(database_filepath):
    engine = create_engine('sqlite:///' + database_filepath)
    movies = pd.read_sql_table('movies', engine)
    reviews = pd.read_sql_table('reviews', engine)
    return movies, reviews


def rank_table(movies, reviews):
    movie_ratings = reviews.groupby('movie_id')['rating']
    avg_ratings = movie_ratings.mean()
    num_ratings = movie_ratings.count()
    last_rating = reviews.groupby('movie_id').max()['date']
    rating_count_df = pd.DataFrame({'avg_rating': avg_ratings, 'num_ratings': num_ratings, 'last_rating': last_rating})
    movie_recs = movies.set_index('movie_id').join(rating_count_df)
    ranked_movies = movie_recs.sort_values(['avg_rating', 'num_ratings', 'last_rating'], ascending=False)
    ranked_movies = ranked_movies[ranked_movies['num_ratings'] > 4]
    return ranked_movies


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
    if len(sys.argv) == 3:

        database_filepath, database_tablename = sys.argv[1:]

        print('Loading data...')
        movies, reviews = load_data('TwitterMovies.db')

        print('Generating Movie Ranking Table...')
        ranked_movies = rank_table(movies, reviews)


        print('Saving data...\n    DATABASE: {}'.format(database_filepath))
        save_data(ranked_movies, database_filepath, database_tablename)

        print('Movie Ranking Table saved to database!')

    else:
        print('Please provide the filepaths of the databae and intended' \
              'table name as the first and second argument respectively. ' \
              '\n\nExample: etl2_ranked_movie.py ' \
              'TwitterMovies.db ' \
              'movie_ranking')


if __name__ == '__main__':
    main()