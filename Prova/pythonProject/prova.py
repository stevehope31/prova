from Backend import *
import seaborn as sns

PATH_DS = "C:\\Users\\ste\\Desktop\\Hotel_Reviews.csv"

if __name__ == "__main__":
    spark = SparkBuilder("appProva")
    dataset = spark.query.getCorrelationBetweenReviewAndSeason()
    dataset.show()
