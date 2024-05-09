import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import streamlit.components.v1 as components

# Function to generate or load data
def generate_data():
    np.random.seed(0)
    dates = pd.date_range(start="2018-01-01", end="2022-12-31", freq='M')
    categories = ['Fiction', 'Non-fiction', 'Science', 'History', 'Children']
    data = []
    for date in dates:
        for category in categories:
            data.append({'Date': date, 'Category': category, 'Sales': np.random.randint(100, 1000)})
    return pd.DataFrame(data)

@st.cache_data
def load_data():
    file_path = 'book_sales_data.csv'
    if not os.path.exists(file_path):
        df = generate_data()
        df.to_csv(file_path, index=False)
    return pd.read_csv(file_path, parse_dates=['Date'])

def plot_bar_chart(df):
    fig, ax = plt.subplots()
    df.groupby('Category')['Sales']. sum().plot(kind='bar', ax=ax)
    st.pyplot(fig)

def display_html():
    html_url = "http://localhost:8000/bcr_race.html"
    components.iframe(html_url, width=700, height=600, scrolling=False)

def apply_custom_css():
    st.markdown("""
        <style>
        .reportview-container {
            padding: 1rem 1rem 1rem 1rem;  /* Adjust the padding around the report view */
        }
        .stDataFrame, .stMarkdown, .stPlotlyChart {
            margin-bottom: 2rem;  /* Add space between components */
        }
        .css-18e3th9 {
            padding: 0;  /* Adjust the padding directly inside the iframe or other Streamlit components */
        }
        </style>
        """, unsafe_allow_html=True)

def main():
    apply_custom_css()
    st.title("Book Sales Analysis")
    df = load_data()
    year = st.sidebar.selectbox('Select Year', sorted(df['Date'].dt.year.unique()), index=0)
    df_filtered = df[df['Date'].dt.year == year]

    st.write("### Yearly Sales Data")
    plot_bar_chart(df_filtered)
    st.write("### Bar Chart Race for Category Sales")
    display_html()

if __name__ == "__main__":
    main()
