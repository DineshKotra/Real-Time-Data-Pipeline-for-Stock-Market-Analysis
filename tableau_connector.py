import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
import tableauserverclient as TSC
import os
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# PostgreSQL configuration
PG_HOST = 'localhost'
PG_PORT = 5432
PG_DATABASE = 'stockmarket'
PG_USER = 'postgres'
PG_PASSWORD = 'postgres'

# Tableau Server configuration
TABLEAU_SERVER = 'https://your-tableau-server'
TABLEAU_USERNAME = 'your-username'
TABLEAU_PASSWORD = 'your-password'
TABLEAU_SITE = 'your-site'
TABLEAU_PROJECT = 'Stock Market Analysis'

def get_postgres_connection():
    """Create and return a PostgreSQL connection"""
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        database=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD
    )

def fetch_data_for_tableau(conn):
    """Fetch data from PostgreSQL for Tableau visualization"""
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    # Stock analytics data
    cursor.execute("""
    SELECT 
        window_start,
        symbol,
        avg_price,
        max_price,
        min_price,
        avg_volume,
        price_stddev
    FROM 
        stock_analytics
    WHERE 
        window_start >= NOW() - INTERVAL '1 day'
    ORDER BY 
        window_start, symbol
    """)
    analytics_data = cursor.fetchall()
    
    # Anomaly data
    cursor.execute("""
    SELECT 
        window_start,
        symbol,
        anomaly_score,
        anomaly_type
    FROM 
        stock_anomalies
    WHERE 
        window_start >= NOW() - INTERVAL '1 day'
    ORDER BY 
        anomaly_score DESC
    """)
    anomaly_data = cursor.fetchall()
    
    # Daily summary data
    cursor.execute("""
    SELECT 
        date,
        symbol,
        open_price,
        close_price,
        high_price,
        low_price,
        volume,
        price_change_pct
    FROM 
        daily_summary
    WHERE 
        date >= CURRENT_DATE - INTERVAL '30 days'
    ORDER BY 
        date, symbol
    """)
    summary_data = cursor.fetchall()
    
    cursor.close()
    
    return {
        'analytics': pd.DataFrame(analytics_data),
        'anomalies': pd.DataFrame(anomaly_data),
        'summary': pd.DataFrame(summary_data)
    }

def export_to_csv(data_dict):
    """Export data to CSV files for Tableau"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    os.makedirs('tableau_data', exist_ok=True)
    
    file_paths = {}
    
    for name, df in data_dict.items():
        if not df.empty:
            file_path = f'tableau_data/{name}_{timestamp}.csv'
            df.to_csv(file_path, index=False)
            file_paths[name] = file_path
            logger.info(f"Exported {len(df)} rows to {file_path}")
    
    return file_paths

def publish_to_tableau(file_paths):
    """Publish data sources to Tableau Server"""
    tableau_auth = TSC.TableauAuth(TABLEAU_USERNAME, TABLEAU_PASSWORD, TABLEAU_SITE)
    server = TSC.Server(TABLEAU_SERVER)
    
    try:
        with server.auth.sign_in(tableau_auth):
            # Find the project
            all_projects, pagination_item = server.projects.get()
            project = next((project for project in all_projects if project.name == TABLEAU_PROJECT), None)
            
            if not project:
                logger.error(f"Project '{TABLEAU_PROJECT}' not found")
                return
            
            # Publish each data source
            for name, file_path in file_paths.items():
                datasource = TSC.DatasourceItem(project_id=project.id)
                
                # Publish the data source
                datasource = server.datasources.publish(
                    datasource,
                    file_path,
                    TSC.Server.PublishMode.Overwrite
                )
                
                logger.info(f"Published {name} data source to Tableau Server: {datasource.id}")
    
    except Exception as e:
        logger.error(f"Error publishing to Tableau: {e}")

def main():
    """Main function to export data to Tableau"""
    conn = get_postgres_connection()
    
    try:
        # Fetch data
        data_dict = fetch_data_for_tableau(conn)
        
        # Export to CSV
        file_paths = export_to_csv(data_dict)
        
        # Publish to Tableau
        publish_to_tableau(file_paths)
        
        logger.info("Data successfully exported to Tableau")
    
    except Exception as e:
        logger.error(f"Error in Tableau export pipeline: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()

