from pyspark.ml.feature import Tokenizer, StopWordsRemover
from pyspark.sql import SparkSession
from pyspark.sql.functions import (regexp_replace, split, expr, col, to_date, regexp_extract, udf, count,
                                   avg, sum, when, first, concat, max, min, countDistinct, explode, lower, lit, year,
                                   month)
from pyspark.sql.types import IntegerType, FloatType, StringType

from Utility import PATH_DS, estraiCitta


class SparkBuilder:

    def __init__(self, appname):
        self.spark = (SparkSession.builder.master("local[*]").
                      appName(appname).getOrCreate())
        self.dataset = self.spark.read.csv(PATH_DS, header=True, inferSchema=True)
        self.castDataset()

    def castDataset(self):
        df = self.dataset
        #Effettuaiamo il cast di qualche colonna
        df = df.withColumn("Additional_Number_of_Scoring", df["Additional_Number_of_Scoring"].cast(IntegerType()))
        df = df.withColumn("Review_Date", to_date(df["Review_Date"], "M/d/yyyy"))
        df = df.withColumn("Average_Score", df["Average_Score"].cast(FloatType()))
        df = df.withColumn("Review_Total_Negative_Word_Counts", df["Review_Total_Negative_Word_Counts"] \
                           .cast(IntegerType()))
        df = df.withColumn("Total_Number_of_Reviews", df["Total_Number_of_Reviews"].cast(IntegerType()))
        df = df.withColumn("Review_Total_Positive_Word_Counts", df["Review_Total_Positive_Word_Counts"] \
                           .cast(IntegerType()))
        df = df.withColumn("Total_Number_of_Reviews_Reviewer_Has_Given",
                           df["Total_Number_of_Reviews_Reviewer_Has_Given"].cast(IntegerType()))
        df = df.withColumn("Reviewer_Score", df["Reviewer_Score"].cast(FloatType()))
        df = df.withColumn("Review_Year", year(df["Review_Date"]))
        df = df.withColumn("Review_Month", month(df["Review_Date"]))
        df = df.withColumn("days_since_review",
                           regexp_extract(col("days_since_review"), r"(\d+)", 1).cast(IntegerType()))

        df = df.withColumn("lat", df["lat"].cast(FloatType()))
        df = df.withColumn("lng", df["lng"].cast(FloatType()))

        #Convertiamo i tag in un array di stringhe
        df = df.withColumn("Tags", regexp_replace(col("Tags"), "[\[\]']", ""))
        df = df.withColumn("Tags", split(col("Tags"), ", "))
        df = df.withColumn("Tags", expr("transform(Tags, x -> trim(x))"))

        # Aggiunta di due colonne per facilitare le query:
        #Country_Hotel rappresenta la nazionalità dell'hotel
        df = df.withColumn("Country_Hotel",
                           regexp_extract(col("Hotel_Address"), r'(United\s+Kingdom|\b[A-Z][a-z]+)$', 1))

        udf_estraiCitta = udf(estraiCitta, StringType())
        #City_Hotel rappresenta la città dove è ubicato l'hotel
        df = df.withColumn("City_Hotel", udf_estraiCitta(col("Hotel_Address"), col("Country_Hotel")))

        # dataset dannaggiato
        df = df.filter(col("lat").isNotNull() & col("lat").isNotNull() & (~col("Reviewer_Nationality").like(" ")))

        self.dataset = df.cache()
        self.query = QueryManager(self)

    def closeConnection(self):
        self.spark.stop()
        print("Connessione Chiusa")


class QueryManager:
    def __init__(self, spark: SparkBuilder):
        self.spark = spark

    def getCountryHotel(self):
        return self.spark.dataset.select(col("Country_Hotel").alias("Country_Hotel")).distinct().collect()

    def getCityHotel(self):
        return self.spark.dataset.select(col("City_Hotel").alias("City_Hotel")).distinct().collect()

    def getHotelName(self):
        return self.spark.dataset.select(col("Hotel_Name").alias("Hotel_Name")).distinct().collect()

    def getTags(self):
        tag = self.spark.dataset.select(explode(col("Tags")).alias("Tag"))
        listoftags = tag.rdd.flatMap(lambda row: row).distinct().collect()
        return listoftags

    def getHotelsByCountry(self, country_name):
        return self.spark.dataset.filter(col("Country_Hotel").like(country_name)).select(
            col("Hotel_Name")).distinct().collect()

    def getHotelsByCity(self, cityname):
        return self.spark.dataset.filter(col("City_Hotel").like(cityname)).select(
            col("Hotel_Name")).distinct().collect()

    # Dataset contenente i dati raggruppati per citta
    def countryInformation(self):
        df = self.spark.dataset

        all_info = df.groupby("City_Hotel").agg(
            count("*").alias("Total_Reviews"),
            countDistinct("Hotel_Name").alias("Number_Hotel"),
            avg("Average_Score").alias("Average_Score"),
            sum(when((col("Negative_Review").like("No Negative")) | (col("Negative_Review").like("Nothing")),
                     0).otherwise(1)).alias("TotalN"),
            sum(when((col("Positive_Review").like("No Positive")) | (col("Positive_Review").like("Nothing")),
                     0).otherwise(1)).alias("TotalP"),
        )
        return all_info

    def wordsFrequency(self):
        df = self.spark.dataset

        # Unisci le parole delle recensioni in una singola colonna e convertile in minuscolo
        words = df.select(
            explode(
                split(
                    lower(concat(col("Negative_Review"), lit(" "), col("Positive_Review"))), "\s+")).alias("word"))

        # Conta il numero di volte che ogni parola appare
        return words.groupby("word").agg(
            count("*").alias("Frequency")
        )

    def maxMinFrequency(self):
        df = self.wordsFrequency()
        max_word = df.orderBy(col("Frequency").desc()).first()
        min_word = df.orderBy(col("Frequency").asc()).first()
        return max_word, min_word

    # restituisce un dataframe contenente latitudine e longitutide
    def getlatlong(self):
        return self.spark.dataset.groupBy("Hotel_Name").agg({"lng": "first", "lat": "first"}).toPandas()

    def getNumOfHotel(self):
        return self.spark.dataset.select(col("Hotel_Name")).distinct().count()

    def getCountryHotel(self):
        return self.spark.dataset.select(col("Country_Hotel")).distinct().collect()

    def getReviewerNationality(self):
        df = self.spark.dataset
        return df.select(col("Reviewer_Nationality")).distinct()

    def mostLeastUsedWordsByCity(self):
        # frequencyword = self.wordsFrequency()
        df = self.spark.dataset

        fdf = df.select(col("City_Hotel"), explode(
            split(lower(concat(col("Negative_Review"), lit(" "), col("Positive_Review"))), "\s+")).alias("Words"))

        word_frequency = fdf.groupBy("City_Hotel", "Words").agg(
            count("*").alias("Frequency")
        )

        maxword = word_frequency.groupBy("City_Hotel").agg(
            max("Frequency").alias("Frequency")
        )
        minword = word_frequency.groupBy("City_Hotel").agg(
            min("Frequency").alias("Frequency")
        )

        maxword = maxword.join(word_frequency, ["City_Hotel", "Frequency"], how="inner")
        minword = minword.join(word_frequency, ["City_Hotel", "Frequency"], how="inner").groupBy("City_Hotel").agg(
            first("Words").alias("Words"),
            first("Frequency").alias("Frequency")
        )

        return maxword, minword

    def hotelHighestAravarageScore(self):
        df = self.spark.dataset
        fdf = df.groupBy("Hotel_Name").agg(avg("Avarage_Score").alias("Avarage_Score"))
        return fdf

    def hotelStatistics(self):
        df = self.spark.dataset

        res = df.groupBy("Hotel_Name").agg(
            count("*").alias("Total_Reviews"),
            sum(when(col("Positive_Review") != "No Positive", 1).otherwise(0)).alias("Total_Positive_Reviews"),
            sum(when(col("Negative_Review") != "No Negative", 1).otherwise(0)).alias("Total_Negative_Reviews"),
            max("Reviewer_Score").alias("Max_Reviewer_Score"),
            min("Reviewer_Score").alias("Min_Reviewer_Score"),
            avg("Reviewer_Score").alias("Avg_Reviewer_Score"),
            first("lat").alias("Latitude"),
            first("lng").alias("Longitude"),
            avg("Additional_Number_of_Scoring").alias("Avg_Additional_Number_of_Scoring")
        )
        return res

    def getH1H2statistic(self, hotelname1, hotelname2):
        df = self.hotelStatistics()

        h1_stats = df.filter(col("Hotel_Name") == hotelname1).first().asDict()
        h2_stats = df.filter(col("Hotel_Name") == hotelname2).first().asDict()

        return h1_stats, h2_stats

    def longestShortestReviews(self):
        df = self.spark.dataset

        # Seleziona le recensioni positive con Review_Total_Positive_Word_Counts massimo
        max_positive_count = df.selectExpr("MAX(Review_Total_Positive_Word_Counts) as Max_Positive_Count") \
            .collect()[0]["Max_Positive_Count"]
        reviews_positive = df.filter((col("Review_Total_Positive_Word_Counts") == max_positive_count))\
            .select("Positive_Review", "Review_Total_Positive_Word_Counts")

        # Seleziona la recensione negativa con Review_Total_Negative_Word_Counts massimo
        max_negative_count = df.selectExpr("MAX(Review_Total_Negative_Word_Counts) as Max_Negative_Count") \
            .collect()[0]["Max_Negative_Count"]
        negative_review = df.filter(col("Review_Total_Negative_Word_Counts") == max_negative_count) \
            .select("Negative_Review", "Review_Total_Negative_Word_Counts")

        return reviews_positive, negative_review

    def mostAndLeastTagUsed(self):
        df = self.spark.dataset

        df_tags = df.select(explode("Tags").alias("word"))

        # Conta la frequenza di ciascun tag
        frequenza_tag = df_tags.groupBy("word").count()

        # Ordina il DataFrame per la frequenza in ordine decrescente
        frequenza_tag = frequenza_tag.orderBy("count", ascending=False)

        return frequenza_tag

    def getWhenLastReviewWPOfHotel(self):
        df = self.spark.dataset

        # Trova il minimo days_since_review per ogni hotel
        fdf = df.groupBy("Hotel_Name").agg(min(col("days_since_review")).alias("min_days_since_review"))
        fdf = fdf.withColumnRenamed("Hotel_Name", "Name")

        # Unisci il DataFrame originale con se stesso usando Hotel_Name e min_days_since_review
        joined_df = df.join(fdf, (df["Hotel_Name"] == fdf["Name"]) & (
                df["days_since_review"] == fdf["min_days_since_review"]), "inner")

        # Seleziona le colonne desiderate
        recensioni = joined_df.select("Hotel_Name", "days_since_review", "Positive_Review", "Negative_Review").orderBy(
            "days_since_review")

        recensioni = recensioni.groupBy("Hotel_Name").agg(
            {"days_since_review": "first", "Positive_Review": "first", "Negative_Review": "first"}).toPandas()

        return recensioni

    def getLastPositiveNegativeReviews(self, hotel_name, hotel_name2):
        df = self.getWhenLastReviewWPOfHotel()
        df.rename(columns={'first(days_since_review)': 'DSR'}, inplace=True)
        df.rename(columns={'first(Positive_Review)': 'Positive'}, inplace=True)
        df.rename(columns={'first(Negative_Review)': 'Negative'}, inplace=True)
        return df[df['Hotel_Name'] == hotel_name], df[df['Hotel_Name'] == hotel_name2]

    def getDatasetForClassification(self):
        df = self.spark.dataset
        review_p = df.select(col("Positive_Review").alias("Review"))
        new_df_p = review_p.withColumn("Sentiment", lit(1))
        new_df_p = new_df_p.filter(~col("Review").like("No Positive"))

        review_n = df.select(col("Negative_Review").alias("Review"))
        new_df_n = review_n.withColumn("Sentiment", lit(0))
        new_df_n = new_df_n.filter(~col("Review").like("No Negative"))

        dataset = new_df_p.union(new_df_n)
        print(dataset.count())
        return dataset.toPandas()

    def getNumberOfDifferentReviewerNationality(self):
        df = self.spark.dataset

        numNationality = df.groupBy("City_Hotel") \
            .agg(countDistinct("Reviewer_Nationality").alias("Different_Nationality")) \
            .orderBy("City_Hotel")

        return numNationality

    #Media delle valutazioni degli hotel per anno o mese
    def getValutationByYearAMonth(self):
        df = self.spark.dataset
        # Calcolare la media delle valutazioni degli hotel per anno
        average_score_per_year = df.groupBy("Review_Year").avg("Average_Score").orderBy("Review_Year")

        # Calcolare la media delle valutazioni degli hotel per mese
        average_score_per_month = df.groupBy("Review_Year", "Review_Month").avg("Average_Score").orderBy("Review_Year",
                                                                                                         "Review_Month")
        return average_score_per_year.toPandas(),average_score_per_month.toPandas()

    def getTotalReviewByYearAMonth(self):
        df = self.spark.dataset
        # Contare il numero totale di recensioni per anno
        reviews_count_per_year = df.groupBy("Review_Year").count().orderBy("Review_Year")

        # Contare il numero totale di recensioni per mese
        reviews_count_per_month = df.groupBy("Review_Year", "Review_Month").count().orderBy("Review_Year",
                                                                                            "Review_Month")
        return reviews_count_per_year,reviews_count_per_month

    def avarageNegativeAndPositveWordsForMonthAndYear(self):
        df = self.spark.dataset
        average_negative_words = (df.groupBy("Review_Year", "Review_Month").agg(
            avg("Review_Total_Negative_Word_Counts").alias("Avarage"))
                                  .orderBy("Review_Year", "Review_Month"))
        average_positive_words = (df.groupBy("Review_Year", "Review_Month").agg(
            avg("Review_Total_Positive_Word_Counts").alias("Avarage"))
                                  .orderBy("Review_Year", "Review_Month"))
        return average_positive_words, average_negative_words

    def getMostAndLeastUsedWordPerYear(self):
        df = self.spark.dataset
        df = df.withColumn("Full_Review", concat(col("Negative_Review"), lit(" "), col("Positive_Review")))

        # Estrarre le parole dalle recensioni
        fdf = df.select("Review_Year", explode(split(lower("Full_Review"), "\s+")).alias("Words"))

        # Contare la frequenza delle parole per ogni anno
        word_counts_per_year = fdf.groupBy("Review_Year", "Words").count()

        # Trovare la parola più utilizzata e meno utilizzata per ogni anno
        word_most_used_per_year = word_counts_per_year.orderBy("Review_Year",
                                                               word_counts_per_year["count"].desc()).groupby(
            "Review_Year").agg({'Words': 'first'})
        word_least_used_per_year = word_counts_per_year.orderBy("Review_Year", word_counts_per_year["count"]).groupby(
            "Review_Year").agg({'Words': 'first'})

        return word_most_used_per_year,word_least_used_per_year

    def getCorrelationBetweenReviewAndSeason(self):
        df = self.spark.dataset
        df = df.withColumn("Season",
                           when((df["Review_Month"] >= 3) & (df["Review_Month"] <= 5), "Spring")
                           .when((df["Review_Month"] >= 6) & (df["Review_Month"] <= 8), "Summer")
                           .when((df["Review_Month"] >= 9) & (df["Review_Month"] <= 11), "Autumn")
                           .otherwise("Winter"))
        # Raggruppare le recensioni per stagione e calcolare il numero di recensioni e la valutazione media
        result = df.groupBy("Season").agg(
            count("*").alias("Total"),
            avg("Average_Score").alias("AScore")
        ).orderBy("Season")
        return result

    