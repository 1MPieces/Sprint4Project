# app.py

import streamlit as st
import pandas as pd
import plotly.express as px

import megaline_module as mm

monthly_df = mm.monthly_user
# project requirements:st.header, st.plotly_chart scatter, st.plotly_chart histogram
# text header for dataframe
pertinent_df = monthly_df[['user_id', 'month', 'plan', 'revenue']]

all_data = st.checkbox('All Monthly User Data')

if all_data:
    st.header("All Megaline Monthly User Data")
    st.dataframe(monthly_df)
else:
    st.header('Pertinent Megaline Data')
    st.dataframe(pertinent_df)

# display the dataframe with streamlit
fig=px.scatter(
        monthly_df,
        x="month",
        y="revenue",
        color="plan"
)
st.header("User Revenue per Month")
st.plotly_chart(fig, use_container_width=True)

fig2=px.histogram(
        monthly_df,
        x='revenue',
        color='plan',
        nbins = 100
)
st.header("Revenue Histogram")
st.plotly_chart(fig2, use_container_width=True)

