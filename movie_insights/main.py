import apache_beam as beam
import datetime
# Data Source
# https://github.com/momenton/MovieTweetings/tree/master/snapshots/100K

pipe=beam.Pipeline()

# movie_file_name="data/movies.dat"
# ratings_file_name="data/ratings.dat"
# users_file_name="data/users.dat"

movie_file_name="data/s_movies.dat"
ratings_file_name="data/s_ratings.dat"
users_file_name="data/s_users.dat"

movie_norm_file="p_norm/movie_norm.dat"
rating_norm_file="p_norm/rating_norm.dat"
movie_genre_norm_file="p_norm/movie_genre_norm.dat"

genre_max_comments_file="p_norm/genre_max_comments.csv"
genre_max_ratings_file="p_norm/genre_max_ratings.csv"



class MovieColonSplitterYear(beam.DoFn):
    def process(self,row):
        retVal=row.split("::")
        yearBeacketStart=retVal[1].find("(")
        yearBeacketEnd=retVal[1].find(")")
        retVal.append(retVal[1][yearBeacketStart+1:yearBeacketEnd])
        retVal[2]=retVal[2].split("|")
        # print(f"In the text retVal[1] the start and end index is {yearBeacketStart+1} and {yearBeacketEnd}")
        return [retVal]

class MovieYear(beam.DoFn):
    def process(self,row):
        # print(f"Processing {row[0]}  {row[1]}  {row[2]}")
        return [[row[0],row[1],row[3]]]


class MovieGenre(beam.DoFn):
    def process(self,row):
        retVal=[]
        for gen in row[2]:
            retVal.append([row[0],gen])
        return retVal


class RatingProcessFileRow(beam.DoFn):
    def process(self,row):
        #seperate every element seperated by ::
        retVal=row.split("::")#user_id::movie_id::rating::rating_timestamp
        #convert timestamp to time object
        retVal=[int(x) for x in retVal]
        date_form = datetime.date.fromtimestamp(retVal[3])
        retVal[3]=date_form.day
        retVal.append(date_form.month)
        retVal.append(date_form.year)
        return [retVal]

def createNormData():
    movies_collection=(
            pipe
            | beam.io.ReadFromText(movie_file_name)
            | "Make Year and Genre columns">> beam.ParDo(MovieColonSplitterYear())
    )

    movie_year_pcollection=(
            movies_collection
            | "Make Movie Year Collection" >> beam.ParDo(MovieYear())
            | "Printing ">> beam.io.textio.WriteToText(movie_norm_file,num_shards=0)
    )

    movie_genre_pcollection=(
            movies_collection
            | "Make Movie Genre Collection" >> beam.ParDo(MovieGenre())
            | "Printing genre collection">> beam.io.textio.WriteToText(movie_genre_norm_file,num_shards=0)
    )

    pipe.run()


    # user_id::movie_id::rating::rating_timestamp
    rating_pipe=beam.Pipeline()
    rating_collection=(
        rating_pipe
        | beam.io.ReadFromText(ratings_file_name)
        | beam.ParDo(RatingProcessFileRow())
        | beam.io.textio.WriteToText(rating_norm_file,num_shards=0)
    )

    rating_pipe.run()


def main():
    createNormData()

main()