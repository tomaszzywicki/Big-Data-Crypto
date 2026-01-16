import streamlit as st
from pymongo import MongoClient
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime

# --- CONNECTION CONFIGURATION ---
# Using the default MongoDB URI for the local VM environment
client = MongoClient("mongodb://localhost:27017/")
db = client["crypto_database"]

st.set_page_config(page_title="Crypto Analysis Hub", layout="wide", page_icon="🚀")

# CSS to ensure the UI looks clean in Dark Mode
st.markdown("""
    <style>
    .main {
        background-color: #0e1117;
    }
    div[data-testid="stMetricValue"] {
        color: #00ff00;
    }
    </style>
    """, unsafe_allow_html=True)

st.title("Crypto Analysis Hub (Batch & Speed Layer)")

# Main Navigation using Tabs
tab1, tab2 = st.tabs(["Market Overview (Batch)", "Live Pulse (Speed)"])

# --- TAB 1: BATCH LAYER (HISTORICAL DATA) ---
with tab1:
    st.header("Historical Trend Analysis")
    
    # Retrieve batch processing results from MongoDB
    batch_data = list(db["batch_stats"].find())
    if batch_data:
        df = pd.DataFrame(batch_data)
        # Combine date parts into a single datetime object
        df['dt'] = pd.to_datetime(df[['year', 'month', 'day', 'hour']].assign(minute=0))
        unique_batch_coins = sorted(df['currency'].unique())

        # --- SECTION 1: PRICE OVERVIEW WITH MIN/MAX BRACKETS ---
        st.subheader("Price Overview (Average + Min/Max Brackets)")
        
        main_coin = st.selectbox("Select currency for primary analysis:", unique_batch_coins, key="main_price_select")
        mdf = df[df['currency'] == main_coin].sort_values('dt')

        fig_price = go.Figure()

        # Primary Price Line (Neon Green)
        fig_price.add_trace(go.Scatter(
            x=mdf['dt'], 
            y=mdf['avg_price'],
            mode='lines+markers',
            name='Average Price',
            line=dict(color='#00ff00', width=3),
            marker=dict(size=8, color='#00ff00', symbol='circle'),
            
            # SUBTLE ERROR BARS (Grey with Alpha) - Represents Min/Max Range
            error_y=dict(
                type='data',
                symmetric=False,
                array=mdf['max_price'] - mdf['avg_price'],      # Upper bracket
                arrayminus=mdf['avg_price'] - mdf['min_price'], # Lower bracket
                visible=True,
                color='rgba(150, 150, 150, 0.4)', 
                thickness=2,
                width=10 
            ),
            # Enhanced Hover info
            hovertemplate="<b>%{x}</b><br>Max: %{customdata[0]:.4f}<br>Avg: %{y:.4f}<br>Min: %{customdata[1]:.4f}<extra></extra>",
            customdata=mdf[['max_price', 'min_price']]
        ))

        fig_price.update_layout(
            template="plotly_dark",
            xaxis_title="Date & Time",
            yaxis_title="Price (USD)",
            height=500,
            hovermode="x unified",
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)'
        )
        st.plotly_chart(fig_price, use_container_width=True)
        
        st.divider()

        # --- SECTION 2: SENTIMENT COMPARISON ---
        st.subheader("Sentiment Strength Comparison (Long/Short Ratio)")
        selected_batch_coins = st.multiselect(
            "Filter currencies for comparison:", 
            options=unique_batch_coins, 
            default=unique_batch_coins
        )

        fig_ls = go.Figure()
        for coin in selected_batch_coins:
            c_df = df[df['currency'] == coin].sort_values('dt')
            fig_ls.add_trace(go.Scatter(x=c_df['dt'], y=c_df['sentiment_strength'], name=f"{coin.upper()}", mode='lines+markers'))
        
        fig_ls.update_layout(template="plotly_dark", height=400, margin=dict(t=20), paper_bgcolor='rgba(0,0,0,0)')
        st.plotly_chart(fig_ls, use_container_width=True)

        st.divider()

        # --- SECTION 3: PRICE VS SENTIMENT CORRELATION (Area Chart) ---
        st.subheader("Price vs. Sentiment Correlation")
        coin_to_inspect = st.selectbox("Select currency for correlation study:", unique_batch_coins, key="corr_select")
        inspect_df = df[df['currency'] == coin_to_inspect].sort_values('dt')
        
        fig_dual = make_subplots(specs=[[{"secondary_y": True}]])

        # 1. Price (Solid Line - Green)
        fig_dual.add_trace(go.Scatter(
            x=inspect_df['dt'], 
            y=inspect_df['avg_price'], 
            name="Avg Price",
            mode='lines+markers',
            line=dict(color='#00ff00', width=3),
            marker=dict(size=6)
        ), secondary_y=False)

        # 2. Sentiment (Dashed Line - Orange)
        fig_dual.add_trace(go.Scatter(
            x=inspect_df['dt'], 
            y=inspect_df['sentiment_strength'], 
            name="Sentiment Strength",
            mode='lines+markers',
            line=dict(color='#FFA500', width=3, dash='dot'), 
            marker=dict(size=6, symbol='diamond')
        ), secondary_y=True)

        fig_dual.update_layout(
            template="plotly_dark",
            height=500,
            hovermode="x unified",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            # Sentiment Axis (Right) - Now scales perfectly to data
            yaxis2=dict(
                title="Sentiment (L/S Ratio)",
                showgrid=False,
                zeroline=False,  # Don't force zero line
                autorange=True,  # Allow zoom (e.g., from 1.0 to 1.2)
                fixedrange=False
            ),
            # Price Axis (Left)
            yaxis=dict(
                title="Price (USD)",
                showgrid=True,
                gridcolor='#333'
            ),
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)'
        )
        
        st.plotly_chart(fig_dual, use_container_width=True)
        
        # Historical Statistics Table
        st.subheader("Momentum & Correlation Statistics")
        stats_df = inspect_df[['dt', 'avg_momentum', 'rolling_volatility', 'sentiment_price_corr']].tail(10).copy()
        stats_df.columns = ['Timestamp', 'Avg Momentum (%)', 'Volatility', 'Sentiment/Price Corr']
        st.dataframe(stats_df, use_container_width=True, hide_index=True)
    else:
        st.info("No historical data found. Please run batch_analyzer.py on the VM.")

# --- TAB 2: SPEED LAYER (LIVE DATA) ---
with tab2:
    col1, col2 = st.columns([4, 1], vertical_alignment="center")

    with col1:
        st.header("Real-time Anomaly Radar")

    with col2:
        if st.button("🔄 Refresh View", use_container_width=True):
            st.rerun()
    
    live_data_cursor = db["live_alerts"].find().sort("_id", -1).limit(100)
    data_list = list(live_data_cursor)
    
    if data_list:
        ldf = pd.DataFrame(data_list)
        
        def parse_to_dt(x):
            ts = x.get('end') if isinstance(x, dict) else x
            return pd.to_datetime(ts)
        
        ldf['window_dt'] = ldf['window'].apply(parse_to_dt)
        ldf['Time'] = ldf['window_dt'].dt.strftime('%H:%M:%S')
        
        cutoff_ts = ldf['window_dt'].max() - pd.Timedelta(minutes=15)
        ldf_15m = ldf[ldf['window_dt'] >= cutoff_ts].copy()
        unique_live_coins = sorted(ldf['currency'].unique())

        # SECTION 1: TICKERS
        st.subheader("Current Ticker Status")
        val_cols = st.columns(len(unique_live_coins))
        for i, coin in enumerate(unique_live_coins):
            latest_coin = ldf[ldf['currency'] == coin].iloc[0]
            alert = latest_coin['alert_type']
            with val_cols[i]:
                st.metric(f"{coin.upper()}", f"${latest_coin['live_price']:.4f}")
                if "Normal" in alert: st.success(f"● {alert}")
                elif "EXTREME" in alert: st.error(f"● {alert}")
                else: st.warning(f"● {alert}")

        st.divider()

        # SECTION 2: GAUGE METERS
        st.subheader("Sentiment Heat (Fear & Greed)")
        gauge_cols = st.columns(len(unique_live_coins))
        
        for i, coin in enumerate(unique_live_coins):
            latest_coin = ldf[ldf['currency'] == coin].iloc[0]
            current_sent = latest_coin['live_sentiment']
            with gauge_cols[i]:
                st.markdown(f"<p style='text-align: center; font-weight: bold; font-size: 20px; margin-bottom: 25px; color: white;'>{coin.upper()}</p>", unsafe_allow_html=True)
                
                fig_gauge = go.Figure(go.Indicator(
                    mode = "gauge+number", 
                    value = current_sent,
                    number = {'font': {'size': 40, 'color': 'white'}, 'valueformat': ".2f"},
                    domain = {'x': [0, 1], 'y': [0.15, 1]},
                    gauge = {
                        'axis': {'range': [0, 4], 'tickwidth': 1, 'tickcolor': "white"},
                        'bar': {'color': "white", 'thickness': 0.2},
                        'steps': [
                            {'range': [0, 1], 'color': "#ff4b4b"},
                            {'range': [1, 2.5], 'color': "#31333f"},
                            {'range': [2.5, 4], 'color': "#00ff00"}
                        ],
                        'threshold': {'line': {'color': "white", 'width': 4}, 'value': current_sent}
                    }
                ))
                
                fig_gauge.update_layout(
                    height=220, 
                    margin=dict(l=35, r=35, t=0, b=0), 
                    template="plotly_dark", 
                    paper_bgcolor='rgba(0,0,0,0)'
                )
                st.plotly_chart(fig_gauge, use_container_width=True)

        # SECTION 3: IMPROVED BORDERED LEGEND
        st.markdown("""
            <div style="border: 1px solid #444; border-radius: 12px; padding: 20px; background-color: #1e2129; margin-top: 20px;">
                <h4 style="margin-top: 0; font-size: 18px; color: #eee; border-bottom: 1px solid #444; padding-bottom: 10px;">🔍 Sentiment Guide & Thresholds</h4>
                <div style="display: flex; justify-content: space-between; gap: 20px; align-items: stretch;">
                    <div style="flex: 1;">
                        <span style="background-color: #ff4b4b; padding: 4px 12px; border-radius: 6px; font-size: 13px; font-weight: bold; color: white;">0.0 - 1.0</span>
                        <p style="font-size: 14px; margin: 10px 0; color: #ccc;"><b>Extreme Fear:</b> Indicates heavy panic selling. In contrarian analysis, this is often a signal that the market is bottoming out.</p>
                    </div>
                    <div style="flex: 1; border-left: 1px solid #444; padding-left: 20px;">
                        <span style="background-color: #31333f; border: 1px solid #666; padding: 4px 12px; border-radius: 6px; font-size: 13px; font-weight: bold; color: #ddd;">1.0 - 2.5</span>
                        <p style="font-size: 14px; margin: 10px 0; color: #ccc;"><b>Neutral:</b> Demand and supply are in equilibrium. Investors are looking for a catalyst or clear trend direction.</p>
                    </div>
                    <div style="flex: 1; border-left: 1px solid #444; padding-left: 20px;">
                        <span style="background-color: #00ff00; color: black; padding: 4px 12px; border-radius: 6px; font-size: 13px; font-weight: bold;">2.5 - 4.0</span>
                        <p style="font-size: 14px; margin: 10px 0; color: #ccc;"><b>Euphoria:</b> Excessive buying pressure. Often a warning sign that the market is overextended and due for a pullback.</p>
                    </div>
                </div>
            </div>
        """, unsafe_allow_html=True)

        st.write("") 
        st.divider()

        # SECTION 4: EVENT LOGS
        st.subheader("Event Logs (Last 15 Minutes)")
        unique_live_coins = sorted(ldf['currency'].unique())
        try: btc_idx = unique_live_coins.index('bitcoin')
        except: btc_idx = 0

        selected_log_coin = st.selectbox("Filter logs by currency:", unique_live_coins, index=btc_idx, key="live_log_filter")
        log_df_filtered = ldf_15m[ldf_15m['currency'] == selected_log_coin][['Time', 'currency', 'alert_type', 'live_price']]
        log_df_filtered.columns = ['Time', 'Currency', 'Status', 'Price']
        
        st.dataframe(
            log_df_filtered.style.applymap(lambda val: 'color: #ff4b4b; font-weight: bold' if 'EXTREME' in str(val) else '', subset=['Status']),
            use_container_width=True, hide_index=True
        )
        
    