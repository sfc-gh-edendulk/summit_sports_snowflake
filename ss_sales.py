# Import libraries
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta, date
import snowflake.snowpark.context as context
import numpy as np

# Get current session
session = context.get_active_session()

# Configuration de la page
st.set_page_config(
    page_title="Dashboard Ventes - SS 101",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Style CSS personnalisé
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .sub-header {
        font-size: 1.5rem;
        font-weight: bold;
        color: #333;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)

# Fonctions utilitaires
@st.cache_data(ttl=300)
def run_query(query_sql):
    """Execute query and return DataFrame"""
    try:
        df = session.sql(query_sql).to_pandas()
        # Conversion sécurisée des colonnes de dates
        for col in df.columns:
            if 'DATE' in col.upper() or 'PERIOD' in col.upper():
                if df[col].dtype == 'object':
                    try:
                        df[col] = pd.to_datetime(df[col]).dt.date
                    except:
                        pass
        return df
    except Exception as e:
        st.error(f"Erreur d'exécution: {e}")
        return pd.DataFrame()

def format_number(value, type='currency'):
    """Format numbers for display"""
    if pd.isna(value) or value is None:
        return "N/A"
    
    try:
        if type == 'currency':
            return f"{float(value):,.0f} €"
        elif type == 'percentage':
            return f"{float(value):.1f}%"
        elif type == 'number':
            return f"{float(value):,.0f}"
        else:
            return str(value)
    except (ValueError, TypeError):
        return str(value)

def safe_calculate_delta(current, previous):
    """Calcul sécurisé des deltas"""
    try:
        if previous is None or previous == 0 or pd.isna(previous):
            return None
        delta = ((current - previous) / previous * 100)
        return f"{delta:.1f}%"
    except:
        return None

# Header principal
st.markdown('<h1 class="main-header">📊 Dashboard Ventes SS 101</h1>', unsafe_allow_html=True)

# Sidebar pour les filtres
st.sidebar.header("🔍 Filtres")

# Filtres de date avec gestion d'erreur
try:
    col1, col2 = st.sidebar.columns(2)
    with col1:
        start_date = st.date_input(
            "Date début",
            value=date.today() - timedelta(days=30),
            key="start_date"
        )
    with col2:
        end_date = st.date_input(
            "Date fin",
            value=date.today(),
            key="end_date"
        )
    
    # Conversion sécurisée des dates
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')
    
    st.sidebar.info(f"📅 Période: {start_date_str} au {end_date_str}")
    
except Exception as e:
    st.error(f"Erreur avec les dates: {e}")
    st.stop()

# Onglets principaux
tab1, tab2, tab3, tab4 = st.tabs([
    "📈 Vue d'ensemble", 
    "🛍️ Produits", 
    "🏪 Magasins",
    "📋 Données Détaillées et Export"
])

# ==================== TAB 1: VUE D'ENSEMBLE ====================
with tab1:
    st.markdown('<h2 class="sub-header">📊 Indicateurs Clés de Performance</h2>', unsafe_allow_html=True)
    
    try:
        # Requête KPIs actuels
        kpi_query = f"""
        SELECT 
            COUNT(*) as total_orders,
            COALESCE(SUM(SALES_PRICE_EURO), 0) as total_revenue,
            COALESCE(AVG(SALES_PRICE_EURO), 0) as avg_order_value,
            COALESCE(SUM(QUANTITY), 0) as total_quantity,
            COUNT(DISTINCT CUSTOMER_ID) as unique_customers
        FROM ss_101.analytics.orders_v
        WHERE SALE_DATE BETWEEN '{start_date_str}' AND '{end_date_str}'
        """
        
        kpi_data = run_query(kpi_query)
        
        if not kpi_data.empty:
            kpis = kpi_data.iloc[0]
            
            # KPIs période précédente pour comparaison
            days_diff = (end_date - start_date).days
            prev_start = start_date - timedelta(days=days_diff)
            prev_end = start_date - timedelta(days=1)
            
            prev_kpi_query = f"""
            SELECT 
                COUNT(*) as total_orders,
                COALESCE(SUM(SALES_PRICE_EURO), 0) as total_revenue,
                COALESCE(AVG(SALES_PRICE_EURO), 0) as avg_order_value
            FROM ss_101.analytics.orders_v
            WHERE SALE_DATE BETWEEN '{prev_start.strftime('%Y-%m-%d')}' AND '{prev_end.strftime('%Y-%m-%d')}'
            """
            
            prev_kpi_data = run_query(prev_kpi_query)
            
            # Calcul des deltas sécurisé
            delta_revenue = None
            delta_orders = None
            delta_aov = None
            
            if not prev_kpi_data.empty:
                prev_kpis = prev_kpi_data.iloc[0]
                delta_revenue = safe_calculate_delta(kpis['TOTAL_REVENUE'], prev_kpis['TOTAL_REVENUE'])
                delta_orders = safe_calculate_delta(kpis['TOTAL_ORDERS'], prev_kpis['TOTAL_ORDERS'])
                delta_aov = safe_calculate_delta(kpis['AVG_ORDER_VALUE'], prev_kpis['AVG_ORDER_VALUE'])
            
            # Affichage des KPIs
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric(
                    "💰 Chiffre d'Affaires",
                    format_number(kpis['TOTAL_REVENUE'], 'currency'),
                    delta_revenue
                )
            
            with col2:
                st.metric(
                    "📦 Commandes",
                    format_number(kpis['TOTAL_ORDERS'], 'number'),
                    delta_orders
                )
            
            with col3:
                st.metric(
                    "🛒 Panier Moyen",
                    format_number(kpis['AVG_ORDER_VALUE'], 'currency'),
                    delta_aov
                )
            
            with col4:
                st.metric(
                    "👥 Clients Uniques",
                    format_number(kpis['UNIQUE_CUSTOMERS'], 'number')
                )
            
            st.markdown("---")
            
            # Graphiques de tendances
            daily_sales_query = f"""
            SELECT 
                SALE_DATE::DATE as SALE_DATE,
                COUNT(*) as NB_ORDERS,
                COALESCE(SUM(SALES_PRICE_EURO), 0) as REVENUE,
                COALESCE(AVG(SALES_PRICE_EURO), 0) as AVG_ORDER_VALUE
            FROM ss_101.analytics.orders_v
            WHERE SALE_DATE BETWEEN '{start_date_str}' AND '{end_date_str}'
            GROUP BY SALE_DATE
            ORDER BY SALE_DATE
            """
            
            daily_sales = run_query(daily_sales_query)
            
            if not daily_sales.empty:
                # Conversion sécurisée des dates pour Plotly
                daily_sales['SALE_DATE_STR'] = daily_sales['SALE_DATE'].astype(str)
                
                col1, col2 = st.columns(2)
                
                with col1:
                    # Évolution du CA
                    fig_revenue = px.line(
                        daily_sales,
                        x='SALE_DATE_STR',
                        y='REVENUE',
                        title="📈 Évolution du Chiffre d'Affaires",
                        labels={'SALE_DATE_STR': 'Date', 'REVENUE': 'Revenus (€)'}
                    )
                    fig_revenue.update_layout(height=400)
                    st.plotly_chart(fig_revenue, use_container_width=True)
                
                with col2:
                    # Évolution des commandes
                    fig_orders = px.bar(
                        daily_sales,
                        x='SALE_DATE_STR',
                        y='NB_ORDERS',
                        title="📦 Évolution du Nombre de Commandes"
                    )
                    fig_orders.update_layout(height=400)
                    st.plotly_chart(fig_orders, use_container_width=True)
                
                # Métriques additionnelles sécurisées
                col1, col2 = st.columns(2)
                
                with col1:
                    peak_day_idx = daily_sales['REVENUE'].idxmax()
                    peak_day = daily_sales.loc[peak_day_idx, 'SALE_DATE']
                    st.metric("🏆 Meilleur Jour", str(peak_day))
                
                with col2:
                    avg_daily_revenue = daily_sales['REVENUE'].mean()
                    st.metric("📊 CA Quotidien Moyen", format_number(avg_daily_revenue, 'currency'))
        
        else:
            st.warning("⚠️ Aucune donnée trouvée pour cette période")
            
    except Exception as e:
        st.error(f"Erreur dans l'onglet Vue d'ensemble: {e}")
        st.info("💡 Vérifiez que la table orders_dt existe et contient des données")


# ==================== TAB 2: PRODUITS ====================
with tab2:
    st.markdown('<h2 class="sub-header">🛍️ Analyse des Produits</h2>', unsafe_allow_html=True)
    
    try:
        # Filtres produits
        col1, col2 = st.columns(2)
        with col1:
            product_limit = st.slider("Nombre de produits", 5, 50, 20)
        with col2:
            sort_by = st.selectbox("Trier par", ["Revenus", "Quantité", "Commandes"])
        
        # Requête produits
        products_query = f"""
            SELECT 
                COALESCE(PRODUCT_NAME, 'Non spécifié') as PRODUCT_NAME,
                COALESCE(BRAND, 'Non spécifié') as BRAND,
                COALESCE(PRODUCT_CATEGORY, 'Non spécifié') as PRODUCT_CATEGORY,
                SUM(QUANTITY) as TOTAL_QUANTITY,
                COALESCE(SUM(SALES_PRICE_EURO), 0) as TOTAL_REVENUE,
                COUNT(*) as NB_ORDERS,
                COALESCE(AVG(SALES_PRICE_EURO), 0) as AVG_PRICE
            FROM ss_101.analytics.orders_v
            WHERE SALE_DATE BETWEEN '{start_date_str}' AND '{end_date_str}'
                AND PRODUCT_NAME IS NOT NULL
            GROUP BY PRODUCT_NAME, BRAND, PRODUCT_CATEGORY
            ORDER BY TOTAL_REVENUE DESC
            LIMIT {product_limit}
        """
        
        products_data = run_query(products_query)
        
        if not products_data.empty:
            # Tri selon sélection
            if sort_by == "Quantité":
                products_data = products_data.sort_values('TOTAL_QUANTITY', ascending=False)
            elif sort_by == "Commandes":
                products_data = products_data.sort_values('NB_ORDERS', ascending=False)
            
            # Graphique principal
            fig = px.bar(
                products_data.head(15),
                x='TOTAL_REVENUE',
                y='PRODUCT_NAME',
                orientation='h',
                title=f"🏆 Top Produits par {sort_by}",
                color='TOTAL_REVENUE',
                color_continuous_scale='Blues'
            )
            fig.update_layout(yaxis={'categoryorder': 'total ascending'}, height=600)
            st.plotly_chart(fig, use_container_width=True)
            
            # Tableau détaillé
            st.markdown("### 📋 Détail des Produits")
            display_df = products_data.copy()
            display_df['TOTAL_REVENUE'] = display_df['TOTAL_REVENUE'].apply(lambda x: format_number(x, 'currency'))
            display_df['AVG_PRICE'] = display_df['AVG_PRICE'].apply(lambda x: format_number(x, 'currency'))
            
            st.dataframe(
                display_df[[
                    'PRODUCT_NAME', 'BRAND', 'PRODUCT_CATEGORY', 
                    'TOTAL_REVENUE', 'TOTAL_QUANTITY', 'NB_ORDERS', 'AVG_PRICE'
                ]],
                use_container_width=True,
                height=400
            )
        
        else:
            st.warning("⚠️ Aucun produit trouvé pour cette période")
            
    except Exception as e:
        st.error(f"Erreur dans l'analyse des produits: {e}")


# ==================== TAB 3: MAGASINS ====================
with tab3:
    st.markdown('<h2 class="sub-header">🏪 Performance des Magasins</h2>', unsafe_allow_html=True)
    
    try:
        # Requête magasins
        stores_query = f"""
            SELECT 
                COALESCE(STORE_NAME, 'Non spécifié') as STORE_NAME,
                COALESCE(STORE_TYPE, 'Non spécifié') as STORE_TYPE,
                COALESCE(POSTCODE, 'N/A') as POSTCODE,
                COUNT(*) as NB_ORDERS,
                COALESCE(SUM(SALES_PRICE_EURO), 0) as REVENUE,
                COUNT(DISTINCT CUSTOMER_ID) as UNIQUE_CUSTOMERS,
                COALESCE(AVG(SALES_PRICE_EURO), 0) as AVG_ORDER_VALUE
            FROM ss_101.analytics.orders_v
            WHERE SALE_DATE BETWEEN '{start_date_str}' AND '{end_date_str}'
                AND STORE_NAME IS NOT NULL
            GROUP BY STORE_NAME, STORE_TYPE, POSTCODE
            ORDER BY REVENUE DESC
        """
        
        stores_data = run_query(stores_query)
        
        if not stores_data.empty:
            # KPIs magasins
            total_stores = len(stores_data)
            avg_revenue_per_store = stores_data['REVENUE'].mean()
            best_store = stores_data.iloc[0]['STORE_NAME']
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("🏪 Magasins Actifs", format_number(total_stores, 'number'))
            with col2:
                st.metric("💰 CA Moyen/Magasin", format_number(avg_revenue_per_store, 'currency'))
            with col3:
                st.metric("🏆 Meilleur Magasin", best_store)
            
            # Performance par magasin
            fig = px.bar(
                stores_data.head(15),
                x='REVENUE',
                y='STORE_NAME',
                orientation='h',
                title="🏆 Top 15 Magasins par CA",
                color='STORE_TYPE'
            )
            fig.update_layout(yaxis={'categoryorder': 'total ascending'}, height=600)
            st.plotly_chart(fig, use_container_width=True)
            
            # Tableau détaillé
            st.markdown("### 📋 Performance Détaillée des Magasins")
            display_stores = stores_data.copy()
            display_stores['REVENUE'] = display_stores['REVENUE'].apply(lambda x: format_number(x, 'currency'))
            display_stores['AVG_ORDER_VALUE'] = display_stores['AVG_ORDER_VALUE'].apply(lambda x: format_number(x, 'currency'))
            
            st.dataframe(
                display_stores[[
                    'STORE_NAME', 'STORE_TYPE', 'POSTCODE', 
                    'REVENUE', 'NB_ORDERS', 'UNIQUE_CUSTOMERS', 'AVG_ORDER_VALUE'
                ]],
                use_container_width=True,
                height=400
            )
        
        else:
            st.warning("⚠️ Aucun magasin trouvé pour cette période")
            
    except Exception as e:
        st.error(f"Erreur dans l'analyse des magasins: {e}")

# ==================== TAB 4: DONNÉES & EXPORT ====================
with tab4:
    st.markdown('<h2 class="sub-header">📋 Données Détaillées et Export</h2>', unsafe_allow_html=True)
    
    # Section requête personnalisée
    st.markdown("### 🔍 Requête Personnalisée")
    
    with st.expander("💡 Exemples de requêtes"):
        st.code("""
-- Données de base
SELECT * FROM ss_101.analytics.orders_v LIMIT 100;

-- Top 10 produits
SELECT PRODUCT_NAME, SUM(SALES_PRICE_EURO) as revenue
FROM ss_101.analytics.orders_v
WHERE SALE_DATE >= CURRENT_DATE() - 30
GROUP BY PRODUCT_NAME
ORDER BY revenue DESC
LIMIT 10;

-- Ventes par mois
SELECT 
    DATE_TRUNC('month', SALE_DATE) as month,
    SUM(SALES_PRICE_EURO) as revenue
FROM ss_101.analytics.orders_v
GROUP BY month
ORDER BY month;
        """)
    
    custom_query = st.text_area(
        "Entrez votre requête SQL",
        height=150,
        placeholder="SELECT * FROM ss_101.analytics.orders_v LIMIT 100"
    )
    
    col1, col2 = st.columns([1, 4])
    with col1:
        execute_query = st.button("▶️ Exécuter", type="primary")
    with col2:
        if st.button("🧹 Effacer"):
            st.rerun()
    
    if execute_query and custom_query.strip():
        try:
            with st.spinner("Exécution de la requête..."):
                custom_result = run_query(custom_query)
                
                if not custom_result.empty:
                    st.success(f"✅ Requête exécutée ! {len(custom_result)} lignes retournées.")
                    st.dataframe(custom_result, use_container_width=True)
                    
                    # Option de téléchargement
                    csv_custom = custom_result.to_csv(index=False)
                    st.download_button(
                        label="⬇️ Télécharger CSV",
                        data=csv_custom,
                        file_name=f"requete_personnalisee_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv"
                    )
                else:
                    st.warning("⚠️ Aucun résultat retourné.")
                    
        except Exception as e:
            st.error(f"❌ Erreur: {str(e)}")

# Footer
st.markdown("---")
col1, col2, col3 = st.columns(3)

with col1:
    st.markdown("📊 **Dashboard SS 101**")

with col2:
    st.markdown(f"📅 **Période**: {start_date_str} → {end_date_str}")

with col3:
    if st.button("🔄 Actualiser"):
        st.cache_data.clear()
        st.rerun()
