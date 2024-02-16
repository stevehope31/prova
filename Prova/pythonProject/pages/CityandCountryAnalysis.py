import plotly.express as px
import streamlit as st

from Backend import SparkBuilder


@st.cache_resource
def getSpark():
    return SparkBuilder("appName")


def main():
    with st.spinner('Caricamento in corso, Attendere Prego...'):
        spark = getSpark()
        countryInformation = spark.query.countryInformation().toPandas()
        mostused, leastused = spark.query.mostLeastUsedWordsByCity()
        totalNationality = spark.query.getReviewerNationality().toPandas()

    st.title("City and Country Analysis")
    st.header("Total number of reviews for city")
    st.bar_chart(countryInformation, x="City_Hotel", y="Total_Reviews", use_container_width=True)

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Number of Hotels for City")
        totalhotel = px.pie(countryInformation, values="Number_Hotel", names="City_Hotel")
        st.plotly_chart(totalhotel, use_container_width=True, theme="streamlit")
    with col2:
        st.subheader("Avarage Score for each City")
        avghotel = px.bar(countryInformation, x="Average_Score", y="City_Hotel", orientation="h")
        st.plotly_chart(avghotel, use_container_width=True, theme="streamlit")

    st.divider()

    st.subheader("Number of Positive and Negative Review for each City")
    totalposnegreview = px.bar(countryInformation, y=["TotalN", "TotalP"], x="City_Hotel", orientation="v",
                               barmode="stack")
    st.plotly_chart(totalposnegreview, use_container_width=True, theme="streamlit")

    st.divider()

    col3, col4 = st.columns(2)

    with col3:
        st.subheader("Most Used Word")
        st.table(mostused)
    with col4:
        st.subheader("Least Used Word")
        st.table(leastused)

    st.divider()

    st.subheader("Total of Different Nationality of Reviewer for City")
    totalposnegreview = px.bar(totalNationality, y="Different_Nationality", x="City_Hotel", orientation="v")
    st.plotly_chart(totalposnegreview, use_container_width=True, theme="streamlit")


main()
