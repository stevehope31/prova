import plotly.express as px
import streamlit as st

from Backend import SparkBuilder


@st.cache_resource
def getSpark():
    return SparkBuilder("appName")

with st.spinner("Loading data... Please wait..."):
    spark = getSpark()
    as_year, as_month = spark.query.getValutationByYearAMonth()
    tr_year, tr_month = spark.query.getTotalReviewByYearAMonth()
    awp_year, awn_month = spark.query.avarageNegativeAndPositveWordsForMonthAndYear()
    muw_year, luw_year = spark.query.getMostAndLeastUsedWordPerYear()
    correlationSeason = spark.query.getCorrelationBetweenReviewAndSeason().toPandas()

st.title("Year Analysis")

col1, col2 = st.columns(2)

with col1:
    st.title("Avarage Score per year")
    st.table(as_year)
with col2:
    st.title("Distribution of the Scores per year and month")
    fig = px.line(as_month, x='Review_Month', y='avg(Average_Score)', color='Review_Year',
                  labels={'Average_Score': 'Media Valutazione', 'Review_Month': 'Mese', 'Review_Year': 'Anno'})
    st.plotly_chart(fig)

st.divider()

col3, col4 = st.columns(2)

with col3:
    st.title("Distribution of the total count of reviews per year and month")
    fig = px.line(tr_month, x='Review_Month', y='count', color='Review_Year',
                  labels={'count': 'Totale Recensioni', 'Review_Month': 'Mese', 'Review_Year': 'Anno'})
    st.plotly_chart(fig)
with col4:
    st.title("Total Review per year")
    st.table(tr_year)

st.divider()

col5, col6 = st.columns(2)

with col5:
    st.title("Distribution of the avarege of positive reviews per year and month")
    fig = px.line(awp_year, x='Review_Month', y='Avarage', color='Review_Year',
                  labels={'Avarage': 'Media', 'Review_Month': 'Mese', 'Review_Year': 'Anno'})
    st.plotly_chart(fig)
with col6:
    st.title("Distribution of the avarege of negative reviews per year and month")
    fig = px.line(awn_month, x='Review_Month', y='Avarage', color='Review_Year',
                  labels={'Avarage': 'Media', 'Review_Month': 'Mese', 'Review_Year': 'Anno'})
    st.plotly_chart(fig)

st.divider()

col7, col8 = st.columns(2)
with col7:
    st.subheader("The most used word by year")
    st.table(muw_year.toPandas())
with col8:
    st.subheader("The least used word by year")
    st.table(luw_year)
st.divider()
st.title("Total Review between Season")

fig = px.bar(correlationSeason, x='Season', y='Total', title='Total for Season',
             labels={'Season': 'Stagione', 'Total': 'Totale'})
st.plotly_chart(fig)