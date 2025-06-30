import streamlit as st
import pandas as pd
import plotly.graph_objs as go
from snowflake.snowpark import Session

import snowflake.snowpark.functions as F
from snowflake.snowpark.functions import col
from datetime import datetime, timedelta
import numpy as np
import unidecode
from snowflake.snowpark.context import get_active_session
import re

import _snowflake
import json
import streamlit as st
import time
from typing import Dict, List, Optional, Tuple
from snowflake.snowpark.exceptions import SnowparkSQLException

session = get_active_session()

# Créer l'application Streamlit
st.set_page_config(page_title="Dashboard Adhérant Summit Sport", layout="wide")
st.image("ssphotobanner.png")
st.title("Dashboard Adhérant Summit Sport")

st.logo("sslogosquare.png")


tab1, tab2, tab3 = st.tabs(["Prévoir les Ventes", "Poser des Questions", "Personaliser l'Experience Client"])

################## PRÉVISION DE VENTES ###################################################

with tab1:

    #####FILTERS##### 
    magasin_options = session.table("ss_101.raw_pos.magasins").select("STORE_NAME").sort("STORE_NAME").collect()
    product_options = [row[0] for row in magasin_options]

    col1, col2 = st.columns(2)
    with col1:
        selected_magasin = st.selectbox("Sélectionnez un magasin :", magasin_options, index=None, placeholder="Tous les magasins")
        if selected_magasin: 
            selected_magasin_clean = unidecode.unidecode(selected_magasin).replace("SUMMITSPORT ", "")
            selected_magasin_cleaned = re.sub(r'[^A-Za-z0-9]', '', selected_magasin_clean)

    with col2:
        selected_range = st.selectbox("Sélectionnez la période :", ["30 derniers jours", "90 derniers jours", "180 derniers jours"])

    ##### GET DATA ######
    if not selected_magasin:
        filtered_data = session.table(["SPORTS_DB", "SPORTS_TRANSFORMATION", "instore_sales_crm3_daily_aggregated"]).sort(col("SALE_DATE")).to_pandas()
    else:
        filtered_data = session.table(["SPORTS_DB", "SPORTS_TRANSFORMATION", "INSTORE_SALES_CRM3_DAILY_MAGASIN_AGGREGATED"]).filter(
            (F.col("STORE_NAME") == selected_magasin)).sort(col("SALE_DATE")).to_pandas()

    filtered_data['SALE_DATE'] = pd.to_datetime(filtered_data['SALE_DATE'])
    end_date = filtered_data['SALE_DATE'].max()
    if selected_range == "30 derniers jours":
        start_date = end_date - timedelta(days=30)
    elif selected_range == "90 derniers jours":
        start_date = end_date - timedelta(days=90)
    else:
        start_date = end_date - timedelta(days=180)

    filtered_data = filtered_data[(filtered_data['SALE_DATE'] >= start_date) & (filtered_data['SALE_DATE'] <= end_date)]
    filtered_data['SALE_DATE'] = pd.to_datetime(filtered_data['SALE_DATE'])

    #### CREATE INITIAL FIGURE #######
    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=filtered_data['SALE_DATE'],
        y=filtered_data['DAILY_REVENUE'],
        mode='lines',
        name='Total Revenue',
        line=dict(color='red'),
        yaxis='y1'
    ))

    fig.add_trace(go.Scatter(
        x=filtered_data['SALE_DATE'],
        y=filtered_data['DAILY_TRANSACTIONS'],
        mode='lines',
        name='# de Ventes',
        line=dict(color='navy'),
        yaxis='y2'
    ))

    fig.update_layout(
        title=f"Ventes {selected_range}",
        xaxis_title="Date",
        yaxis=dict(
            title="Revenue des Ventes (€)",
            titlefont=dict(color="red"),
            tickfont=dict(color="red"),
        ),
        yaxis2=dict(
            title="# des Ventes",
            titlefont=dict(color="navy"),
            tickfont=dict(color="navy"),
            overlaying="y",
            side="right"
        ),
        legend_title="Indicateurs",
        hovermode="x unified",
        template="plotly_white"
    )

    ####### FORECAST BUTTON ########
    if st.button("Visualisez des prédictions de vente"):
        if not selected_magasin:
            forecast_data = session.table(["SPORTS_DB", "SPORTS_datascience", "SPORTS_AGGREGATED_FORECAST"]).sort(col("SALE_DATE")).to_pandas()
        else:
            forecast_data = session.table(["SPORTS_DB", "SPORTS_datascience", "SPORTS_AGGREGATED_FORECAST_STORE"]).filter(
                (F.col("STORE_NAME") == selected_magasin)).sort(col("SALE_DATE")).to_pandas()

        forecast_data['SALE_DATE'] = pd.to_datetime(forecast_data['SALE_DATE'])
        end_date = forecast_data['SALE_DATE'].max()
        if selected_range == "30 derniers jours":
            start_date = end_date - timedelta(days=30)
        elif selected_range == "90 derniers jours":
            start_date = end_date - timedelta(days=90)
        else:
            start_date = end_date - timedelta(days=180)

        forecast_data = forecast_data[(forecast_data['SALE_DATE'] >= start_date) & (forecast_data['SALE_DATE'] <= end_date)]

        with st.status("Génération des prédictions", expanded=True) as status:        
            st.write("... modèle entraîné ...")
            status.update(label="Prédictions finies", state="complete", expanded=True)

        # Get last historical point
        last_hist_date = filtered_data['SALE_DATE'].max()
        last_hist_value = filtered_data.loc[filtered_data['SALE_DATE'] == last_hist_date, 'DAILY_REVENUE'].values[0]
        
        # Filter forecast data starting after that point
        forecast_only = forecast_data[forecast_data['SALE_DATE'] > last_hist_date].copy()
        
        # Prepend last actual value to forecast for seamless line
        stitched_dates = [last_hist_date] + list(forecast_only['SALE_DATE'])
        stitched_values = [last_hist_value] + list(forecast_only['FORECAST'])
        
        # Add the connected forecast trace
        fig.add_trace(go.Scatter(
            x=stitched_dates,
            y=stitched_values,
            mode='lines',
            name='Prévision de Revenue',
            line=dict(color='red', dash='dot'),
            yaxis='y1'
        ))

        fig.add_trace(go.Scatter(
            x=forecast_data['SALE_DATE'],
            y=forecast_data['FORECAST'],
            mode='lines',
            name='Prévision de Revenue',
            line=dict(color='red', dash='dot'),
            yaxis='y1'
        ))

        fig.add_trace(go.Scatter(
            x=forecast_data['SALE_DATE'],
            y=forecast_data['UPPER_BOUND'],
            mode='lines',
            name='Borne Supérieure',
            line=dict(color='darkred', dash='dash'),
            yaxis='y1',
            showlegend=False
        ))

        fig.add_trace(go.Scatter(
            x=forecast_data['SALE_DATE'],
            y=forecast_data['LOWER_BOUND'],
            mode='lines',
            name='Borne Inférieure',
            line=dict(color='salmon', dash='dash'),
            fill='tonexty',
            fillcolor='rgba(255, 0, 0, 0.2)',
            yaxis='y1',
            showlegend=False
        ))

        st.toast('Prédictions générées')

    #### DISPLAY FINAL FIGURE ####
    st.plotly_chart(fig, use_container_width=True)

    with st.expander("Liste des ventes"):
        if selected_magasin:
            st.write(session.table("SPORTS_DB.SPORTS_DATA.INSTORE_SALES_DATA_CRM3").filter(
                (F.col("STORE_NAME") == selected_magasin)).sort(col("SALE_DATE"), ascending=False))
        else: 
            st.write(session.table("SPORTS_DB.SPORTS_DATA.INSTORE_SALES_DATA_CRM3").sort(col("SALE_DATE"), ascending=False))
