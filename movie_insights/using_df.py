import pandas as pd
import numpy as np
import time
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties

'''
DATA Structure

Structure of movies.dat
movie_id::movie_title (movie_year)::genre|genre|genre

Structure of users.dat
userid::twitter_id

Structure of ratings.dat
user_id::movie_id::rating::rating_timestamp

the most popular movie genres, year by year, for the past decade by using user rating from tweets.

'''

# configuration 
# file names
MOVIES_FILE="./data/movies.dat"
RATINGS_FILE="./data/ratings.dat"
USERS_FILE="./data/users.dat"

MOVIE_GENRE_NORM_FILE="./norm_data/movie_genre_norm.dat"
MOVIE_NORM_FILE="./norm_data/movie_norm.dat"
RATING_NORM_FILE="./norm_data/rating_norm.dat"

GENRE_MAX_COMMENTS_FILE="./norm_data/genre_max_comments.csv"
GENRE_MAX_RATINGS_FILE="./norm_data/genre_max_ratings.csv"


def splitGenreData(val:str):
    """Method to split "|" seperated values in the genre field.

    Keyword arguments:
    val -- the pipe seperated values from the genre field
    """    
    if isinstance(val,str):
        return [(x) for x in val.split("|")] if val!=None else []
    else:
        return []

def getMovieYear(val:str):
    """Extracts the movie name from the title containing the year in the format "loren epsum loren epsum (YYYY) loren epsum"

    Keyword arguments:
    val -- the field of the movie name
    """    
    if isinstance(val,str):
        beg=val.find("(")
        en=val.find(")")
        return val[beg+1:en]
    else:
        return None


def getTimeElement(t_str,format):
    return time.strftime(format, time.localtime(int(t_str)))

def createNormalisedDataFiles():
    movies_df=pd.read_csv(MOVIES_FILE,   converters={'movie_id': lambda x: str(x)}, header=None, delimiter="::",engine="python",names=["movie_id","movie_title_year","genre"])
    ratings_df=pd.read_csv(RATINGS_FILE, header=None, delimiter="::",engine="python",names=["user_id","movie_id","rating","rating_timestamp"])
    users_df=pd.read_csv(USERS_FILE, header=None, delimiter="::",engine="python",names=("user_id","twitter_id"))
    


    # extracting genre into different data frame
    movie_genre_df=movies_df[["movie_id","genre"]].copy()
    movie_genre_df["genre"]=movie_genre_df["genre"].apply(splitGenreData)
    movie_genre_df=movie_genre_df.explode('genre')
    # removing blanks
    movie_genre_df['genre'].replace('', np.nan, inplace=True)
    movie_genre_df.dropna(subset=['genre'], inplace=True)
    
    # adding year column to movie data in seperate column
    movies_df["year"]=movies_df["movie_title_year"].apply(getMovieYear)
    movies_df=movies_df.drop(['genre'],axis=1)
    
    # splitting time elements to day, month, year, hour and minute
    ratings_df["rating_date"]=ratings_df["rating_timestamp"].apply(lambda t:[getTimeElement(t,"%d"),getTimeElement(t,"%m"),getTimeElement(t,"%Y"),getTimeElement(t,"%H"),getTimeElement(t,"%M")])
    ratings_df[["rating_date_day","rating_date_month","rating_date_year","rating_date_hour","rating_date_minute"]]=pd.DataFrame(ratings_df["rating_date"].tolist())
    ratings_df=ratings_df.drop(['rating_date'],axis=1)
   
    # creating files out of normalised DF
    movie_genre_df.to_csv(MOVIE_GENRE_NORM_FILE,index=False) # this file has entries movie_id, genre1. If the movie has multiple genre multiple lines with same movieid are produced.
    movies_df.to_csv(MOVIE_NORM_FILE,index=False) # movie_id, title, year of release
    ratings_df.to_csv(RATING_NORM_FILE,index=False) # user_id,movie_id,rating,rating_timestamp,rating_date_day,rating_date_month,rating_date_year,rating_date_hour,rating_date_minute


# highest average rating gener by Year of release
def higherRatings():
    movie_genre_df=pd.read_csv(MOVIE_GENRE_NORM_FILE)
    ratings_df=pd.read_csv(RATING_NORM_FILE)
    movies_df=pd.read_csv(MOVIE_NORM_FILE)
    
    # dropping unused columns
    ratings_df.drop(["rating_timestamp","rating_date_day","rating_date_month","rating_date_year","rating_date_hour","rating_date_minute"],axis=1,inplace=True)
    movies_df.drop(["movie_title_year"],axis=1,inplace=True)

    movie_genre_mean_df=pd.merge(pd.merge(movies_df,ratings_df,on='movie_id',how='left'),movie_genre_df,on='movie_id',how='left')
    movie_genre_mean_df.drop(['movie_id','user_id'],axis=1,inplace=True)
    movie_genre_mean_df=movie_genre_mean_df.groupby(['year','genre']).mean()
    movie_genre_mean_df.reset_index( inplace=True)
    movie_mean_df=movie_genre_mean_df.drop(['genre'],axis=1)
    
    movie_mean_df=movie_mean_df.groupby(['year']).max()
    movie_mean_df.reset_index( inplace=True)   
    result_df=pd.merge(movie_mean_df,movie_genre_mean_df,on=['year','rating'],how='left')
    result_df.to_csv(GENRE_MAX_RATINGS_FILE,index=False)


# most twetted genre by Year of release
def makeRatingYearGenre():
    movie_genre_df=pd.read_csv(MOVIE_GENRE_NORM_FILE)
    ratings_df=pd.read_csv(RATING_NORM_FILE)
    movies_df=pd.read_csv(MOVIE_NORM_FILE)
    
    # dropping unused columns
    ratings_df.drop(["rating_timestamp","rating_date_day","rating_date_month","rating_date_year","rating_date_hour","rating_date_minute"],axis=1,inplace=True)
    movies_df.drop(["movie_title_year"],axis=1,inplace=True)

    merged_df=pd.merge(pd.merge(movies_df,ratings_df,on='movie_id',how='left'),movie_genre_df,on='movie_id',how='left')

    count_ratings_by_year_df=merged_df.groupby(['year','genre']).count().drop(['user_id','rating'],axis=1)
    count_ratings_by_year_df.reset_index(level=0, inplace=True)
    count_ratings_by_year_df["genre_c"]=count_ratings_by_year_df.index
    
    year_max_of_movie_df=count_ratings_by_year_df.groupby(['year'], sort=True).max()
    year_max_of_movie_df.reset_index(level=0,inplace=True)
    year_max_of_movie_df=year_max_of_movie_df.groupby("year").max()

    result_df=pd.merge(year_max_of_movie_df,count_ratings_by_year_df,on=['year','movie_id'],how="inner").drop(['genre_c_x'],axis=1)
    result_df.rename(columns={"movie_id":"remarks_count","genre_c_y":"genre"},inplace=True)
    result_df.to_csv(GENRE_MAX_COMMENTS_FILE,index=False)

def visualiseRatingYearGenre_all():
    data_df=pd.read_csv(GENRE_MAX_COMMENTS_FILE)# columns: year,remarks_count,genre
    genre_grouped_df=data_df.groupby(['genre']).sum()

    print(genre_grouped_df.head())
    genre=genre_grouped_df.index.values
    remarks_count=genre_grouped_df['remarks_count']
    patches, texts =plt.pie(remarks_count,labels=genre)


    fontP = FontProperties()
    fontP.set_size('small')
    plt.legend(patches,genre,prop=fontP,loc='upper center', bbox_to_anchor=(0.5, 1.05),ncol=6, fancybox=True, shadow=True)
    genre_grouped_df.to_csv("./norm_data/visualiseRatingYearGenre_all.csv")
    plt.show()

def visualiseRatingYearGenre_exclude_bigs():
    data_df=pd.read_csv(GENRE_MAX_COMMENTS_FILE)# columns: year,remarks_count,genre
    genre_grouped_df=data_df[data_df["genre"]!= "Drama"][data_df["genre"]!= "Action"].groupby(['genre']).sum()

    print(genre_grouped_df.head())
    genre=genre_grouped_df.index.values
    remarks_count=genre_grouped_df['remarks_count']
    patches, texts =plt.pie(remarks_count,labels=genre)

    fontP = FontProperties()
    fontP.set_size('small')
    plt.legend(patches,genre,prop=fontP,loc='upper center', bbox_to_anchor=(0.5, 1.05),ncol=6, fancybox=True, shadow=True)
    genre_grouped_df.to_csv("./norm_data/visualiseRatingYearGenre_exclude_bigs.csv")
    plt.show()


def main():
    print("Starting.....")
    createNormalisedDataFiles()
    makeRatingYearGenre()
    higherRatings()
    visualiseRatingYearGenre_all()
    visualiseRatingYearGenre_all()
    visualiseRatingYearGenre_exclude_bigs()

    print("Done")

main()