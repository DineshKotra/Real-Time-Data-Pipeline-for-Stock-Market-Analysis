import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import RealDictCursor

# PostgreSQL configuration
PG_HOST = 'localhost'
PG_PORT = 5432
PG_DATABASE = 'stockmarket'
PG_USER = 'postgres'
PG_PASSWORD = 'postgres'

# Page configuration
st.set_page_config(
    page_title="Stock Market Analysis Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        margin-bottom: 1rem;
    }
    .sub-header {
        font-size: 1.5rem;
        font-weight: 600;
        margin-bottom: 1rem;
    }
    .card {
        padding: 1rem;
        border-radius: 0.5rem;
        background-color: #f8f9fa;
        box-shadow: 0 0.125rem 0.25rem rgba(0, 0, 0, 0.075);
        margin-bottom: 1rem;
    }
    .metric-value {
        font-size: 2rem;
        font-weight: 700;
    }
    .metric-label {
        font-size: 0.875rem;
        color: #6c757d;
    }
    .positive {
        color: #28a745;
    }
    .negative {
        color: #dc3545;
    }
</style>
""", unsafe_allow_html=True)

@st.cache_resource
def get_postgres_connection():
    """Create and return a PostgreSQL connection"""
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        database=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD
    )

@st.cache_data(ttl=300)
def fetch_stock_data(symbol, timeframe='1d'):
    """Fetch stock data from PostgreSQL"""
    conn = get_postgres_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    if timeframe == '1d':
        # Fetch minute-by-minute data for the last day
        query = """
        SELECT 
            window_start as timestamp,
            avg_price as price,
            avg_volume as volume
        FROM 
            stock_analytics
        WHERE 
            symbol = %s
            AND window_start >= NOW() - INTERVAL '1 day'
        ORDER BY 
            window_start
        """
    elif timeframe == '1w':
        # Fetch hourly data for the last week
        query = """
        SELECT 
            date_trunc('hour', window_start) as timestamp,
            AVG(avg_price) as price,
            AVG(avg_volume) as volume
        FROM 
            stock_analytics
        WHERE 
            symbol = %s
            AND window_start >= NOW() - INTERVAL '1 week'
        GROUP BY 
            date_trunc('hour', window_start)
        ORDER BY 
            timestamp
        """
    else:
        # Fetch daily data for the last month
        query = """
        SELECT 
            date as timestamp,
            close_price as price,
            volume
        FROM 
            daily_summary
        WHERE 
            symbol = %s
            AND date >= CURRENT_DATE - INTERVAL '30 days'
        ORDER BY 
            date
        """
    
    cursor.execute(query, (symbol,))
    data = cursor.fetchall()
    cursor.close()
    
    return pd.DataFrame(data)

@st.cache_data(ttl=300)
def fetch_anomalies(symbol=None):
    """Fetch anomaly data from PostgreSQL"""
    conn = get_postgres_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    if symbol:
        query = """
        SELECT 
            window_start as timestamp,
            symbol,
            anomaly_score,
            anomaly_type
        FROM 
            stock_anomalies
        WHERE 
            symbol = %s
            AND window_start >= NOW() - INTERVAL '1 day'
        ORDER BY 
            anomaly_score DESC
        LIMIT 10
        """
        cursor.execute(query, (symbol,))
    else:
        query = """
        SELECT 
            window_start as timestamp,
            symbol,
            anomaly_score,
            anomaly_type
        FROM 
            stock_anomalies
        WHERE 
            window_start >= NOW() - INTERVAL '1 day'
        ORDER BY 
            anomaly_score DESC
        LIMIT 10
        """
        cursor.execute(query)
    
    data = cursor.fetchall()
    cursor.close()
    
    return pd.DataFrame(data)

@st.cache_data(ttl=300)
def fetch_market_overview():
    """Fetch market overview data"""
    conn = get_postgres_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    query = """
    SELECT 
        symbol,
        close_price as price,
        price_change_pct
    FROM 
        daily_summary
    WHERE 
        date = (SELECT MAX(date) FROM daily_summary)
    ORDER BY 
        symbol
    """
    
    cursor.execute(query)
    data = cursor.fetchall()
    cursor.close()
    
    return pd.DataFrame(data)

def main():
    # Sidebar
    st.sidebar.markdown('<div class="sub-header">Stock Market Analysis</div>', unsafe_allow_html=True)
    
    # Stock selection
    symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META']
    selected_symbol = st.sidebar.selectbox('Select Stock', symbols)
    
    # Timeframe selection
    timeframe = st.sidebar.radio('Timeframe', ['1d', '1w', '1m'], horizontal=True)
    
    # Refresh button
    if st.sidebar.button('Refresh Data'):
        st.cache_data.clear()
    
    # Main content
    st.markdown('<div class="main-header">Stock Market Analysis Dashboard</div>', unsafe_allow_html=True)
    
    # Market overview
    market_data = fetch_market_overview()
    
    # Create metrics row
    col1, col2, col3, col4, col5 = st.columns(5)
    
    for i, (_, row) in enumerate(market_data.iterrows()):
        col = [col1, col2, col3, col4, col5][i]
        with col:
            st.markdown(f"""
            <div class="card">
                <div class="metric-label">{row['symbol']}</div>
                <div class="metric-value">${row['price']:.2f}</div>
                <div class="metric-label {'positive' if row['price_change_pct'] >= 0 else 'negative'}">
                    {'+' if row['price_change_pct'] >= 0 else ''}{row['price_change_pct']:.2f}%
                </div>
            </div>
            """, unsafe_allow_html=True)
    
    # Stock chart and anomalies
    col1, col2 = st.columns([3, 1])
    
    with col1:
        st.markdown(f'<div class="sub-header">{selected_symbol} Performance</div>', unsafe_allow_html=True)
        
        # Fetch stock data
        stock_data = fetch_stock_data(selected_symbol, timeframe)
        
        if not stock_data.empty:
            # Create interactive chart
            fig = go.Figure()
            
            # Add price line
            fig.add_trace(go.Scatter(
                x=stock_data['timestamp'],
                y=stock_data['price'],
                mode='lines',
                name='Price',
                line=dict(color='#1f77b4', width=2)
            ))
            
            # Add volume bars
            fig.add_trace(go.Bar(
                x=stock_data['timestamp'],
                y=stock_data['volume'],
                name='Volume',
                marker=dict(color='rgba(0, 0, 255, 0.3)'),
                opacity=0.3,
                yaxis='y2'
            ))
            
            # Layout
            fig.update_layout(
                height=500,
                margin=dict(l=0, r=0, t=0, b=0),
                legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1),
                yaxis=dict(title='Price ($)', side='left', showgrid=True),
                yaxis2=dict(title='Volume', side='right', overlaying='y', showgrid=False),
                xaxis=dict(showgrid=True),
                hovermode='x unified'
            )
            
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info(f"No data available for {selected_symbol} with the selected timeframe.")
    
    with col2:
        st.markdown('<div class="sub-header">Anomaly Detection</div>', unsafe_allow_html=True)
        
        # Fetch anomalies
        anomalies = fetch_anomalies(selected_symbol)
        
        if not anomalies.empty:
            for _, anomaly in anomalies.iterrows():
                st.markdown(f"""
                <div class="card">
                    <div class="metric-label">{anomaly['anomaly_type']}</div>
                    <div class="metric-value negative">{anomaly['anomaly_score']:.1f}</div>
                    <div class="metric-label">
                        {anomaly['timestamp'].strftime('%Y-%m-%d %H:%M')}
                    </div>
                </div>
                """, unsafe_allow_html=True)
        else:
            st.info(f"No anomalies detected for {selected_symbol}.")
    
    # Additional analysis
    st.markdown('<div class="sub-header">Market Analysis</div>', unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Correlation heatmap
        st.markdown('<div class="card">Correlation Analysis</div>', unsafe_allow_html=True)
        
        # Create dummy correlation data
        corr_data = pd.DataFrame(np.random.rand(5, 5), columns=symbols, index=symbols)
        for i in range(5):
            corr_data.iloc[i, i] = 1.0
        
        fig = px.imshow(
            corr_data,
            text_auto=True,
            color_continuous_scale='Blues',
            labels=dict(x="Stock", y="Stock", color="Correlation")
        )
        fig.update_layout(height=400)
        
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Volume comparison
        st.markdown('<div class="card">Volume Comparison</div>', unsafe_allow_html=True)
        
        # Create dummy volume data
        volume_data = pd.DataFrame({
            'Stock': symbols,
            'Volume': np.random.randint(1000000, 10000000, size=5)
        })
        
        fig = px.bar(
            volume_data,
            x='Stock',
            y='Volume',
            color='Stock',
            labels={'Volume': 'Trading Volume'},
            height=400
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    # Pipeline status
    st.markdown('<div class="sub-header">Pipeline Status</div>', unsafe_allow_html=True)
    
    col1, col2, col3, col4 = st.columns(4)
    
    components = {
        'Kafka': 'Healthy',
        'Spark': 'Healthy',
        'Airflow': 'Warning',
        'PostgreSQL': 'Healthy'
    }
    
    for i, (component, status) in enumerate(components.items()):
        col = [col1, col2, col3, col4][i]
        with col:
            color = '#28a745' if status == 'Healthy' else '#ffc107' if status == 'Warning' else '#dc3545'
            st.markdown(f"""
            <div class="card">
                <div class="metric-label">{component}</div>
                <div class="metric-value" style="color: {color};">{status}</div>
                <div class="metric-label">Last updated: {datetime.now().strftime('%H:%M:%S')}</div>
            </div>
            """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()
