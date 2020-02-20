from sqlalchemy import create_engine
import pandas as pd


def load_data(database_filepath):
    engine = create_engine('sqlite:///' + database_filepath)
    ranked_movies = pd.read_sql_table('movie_ranking', engine)
    return ranked_movies


def popular_recommendations(user_id, n_top, ranked_movies):
    '''
    INPUT:
    user_id - the user_id of the individual you are making recommendations for
    n_top - an integer of the number recommendations you want back
    OUTPUT:
    top_movies - a list of the n_top recommended movies by movie title in order best to worst
    '''
    # Do stuff
    top_movies = list(ranked_movies['movie'][:n_top])

    return top_movies  # a list of the n_top movies as recommended


def popular_recs_filtered(user_id, n_top, ranked_movies, years=None, genres=None):
    '''
    REDO THIS DOC STRING

    INPUT:
    user_id - the user_id (str) of the individual you are making recommendations for
    n_top - an integer of the number recommendations you want back
    ranked_movies - a pandas dataframe of the already ranked movies based on avg rating, count, and time
    years - a list of strings with years of movies
    genres - a list of strings with genres of movies

    OUTPUT:
    top_movies - a list of the n_top recommended movies by movie title in order best to worst
    '''
    # Filter movies based on year and genre
    if years is not None:
        ranked_movies = ranked_movies[ranked_movies['date'].isin(years)]

    if genres is not None:
        num_genre_match = ranked_movies[genres].sum(axis=1)
        ranked_movies = ranked_movies.loc[num_genre_match > 0, :]

    # create top movies list
    top_movies = list(ranked_movies['movie'][:n_top])

    return top_movies


def main():
    print('Loading data...')
    ranked_movies = load_data('../data/TwitterMovies.db')
    genres_list = ['Action', 'Adult', 'Adventure', 'Animation', 'Biography', 'Comedy', 'Crime', 'Documentary', 'Drama',
                   'Family', 'Fantasy', 'Film-Noir', 'Game-Show', 'History', 'Horror', 'Music', 'Musical', 'Mystery',
                   'News', 'Reality-TV', 'Romance', 'Sci-Fi', 'Short', 'Sport', 'Talk-Show', 'Thriller', 'War',
                   'Western', 'nan']

    print('Enter your name:')
    name = input()
    print('Welcome to Movie Recommendation from Leo, ' + name)
    print('Any specific production year for the movies?')
    years = []
    years_input = input()
    years.append(years_input)
    print('Noted. Any particular genre?')
    print('Here is a list of all genres: ')
    print(genres_list)
    genres = []
    genres_input = input()
    genres.append(genres_input)
    print('Last question, how many recommendations?')
    n_top = int(input())

    top_movies = popular_recs_filtered('11', n_top, ranked_movies, years, genres)

    print('Thank you for your input! The list of the recommended movies are: ')
    for i, name in enumerate(top_movies):
        print('No.{}: {}'.format(i+1, name))


if __name__ == '__main__':
    main()