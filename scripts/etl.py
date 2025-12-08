import pandas as pd
import pyodbc
import os
from sqlalchemy import create_engine  # noqa: F401  # Utilisé dans la partie Chargement (L)
# =================================================================
# PARTIE 1 : EXTRACTION SQL SERVER
# =================================================================

# ✅ SERVEUR SQL IDENTIFIÉ : Vous avez trouvé ce nom avec SSMS.
SQL_SERVER_NAME = r'DESKTOP-F8N2M8C\SQLEXPRESS' 
SQL_DATABASE_NAME = 'Northwind' 

# Chaîne de connexion pour l'authentification Windows
SQL_CONN_STRING = (
    f'DRIVER={{ODBC Driver 17 for SQL Server}};'
    f'SERVER={SQL_SERVER_NAME};'
    f'DATABASE={SQL_DATABASE_NAME};'
    r'Trusted_Connection=yes;'
    r'TrustServerCertificate=yes;'
)

print(f"Tentative de connexion à SQL Server: {SQL_SERVER_NAME}/{SQL_DATABASE_NAME}")

try:
    sql_conn = pyodbc.connect(SQL_CONN_STRING)
    print("✅ Connexion à SQL Server réussie.")

    # Requêtes d'extraction des tables nécessaires au schéma en étoile :
    queries = {
        'Orders': 'SELECT * FROM Orders',
        'OrderDetails': 'SELECT * FROM "Order Details"',
        'Customers_SQL': 'SELECT * FROM Customers',
        'Products': 'SELECT * FROM Products',
        'Categories': 'SELECT * FROM Categories',
        'Employees': 'SELECT * FROM Employees',
        'Shippers': 'SELECT * FROM Shippers'
    }

    raw_data_sql = {}
    for table, query in queries.items():
        raw_data_sql[table] = pd.read_sql(query, sql_conn)
        print(f"- Extrait la table {table} ({len(raw_data_sql[table])} lignes).")

except pyodbc.Error as ex:
    print(f"❌ Erreur de connexion à SQL Server.")
    print(ex)


# =================================================================
# PARTIE 2 : EXTRACTION ACCESS
# =================================================================

# ⚠️ À ADAPTER : Mettez ici le chemin COMPLET vers votre fichier Northwind Access !
# Exemple: r'C:\Users\tk computer\Documents\Northwind.accdb'
ACCESS_FILE_PATH = r'C:/Users/tk computer/OneDrive/Documents/Database1.accdb' 

ACCESS_CONN_STRING = (
    r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
    f'DBQ={ACCESS_FILE_PATH};'
)

print(f"\nTentative de connexion à Access: {os.path.basename(ACCESS_FILE_PATH)}")

try:
    access_conn = pyodbc.connect(ACCESS_CONN_STRING)
    print("✅ Connexion à Access réussie.")

    # Extrait les données complémentaires/contradictoires d'Access (ici, les Notes des clients)
    df_access_customers = pd.read_sql("SELECT CustomerID, Notes FROM Customers", access_conn)
    print(f"- Extrait les données complémentaires d'Access ({len(df_access_customers)} lignes).")

except pyodbc.Error as ex:
    print(f"❌ Erreur de connexion à Access.")
    print(ex)
    

# ... (Après la section d'extraction des tables de Northwind et d'Access)
# ... (Après les print("- Extrait la table Orders (830 lignes)."))

# =================================================================
# ÉTAPE : Exportation des Données Sources (RAW)
# =================================================================
print("\n--- Démarrage de l'Exportation des fichiers sources (RAW) ---")

# Chemin de sortie pour les données brutes
RAW_OUTPUT_DIR = 'data/raw/' 

# Crée le dossier 'data/raw' s'il n'existe pas
os.makedirs(RAW_OUTPUT_DIR, exist_ok=True)


# Le dictionnaire 'raw_data_sql' contient toutes les tables extraites de la BDD Northwind
dfs_to_export_raw = raw_data_sql.copy() 

# Ajoutez la table des notes clients d'Access (si vous l'avez nommée df_access_customers)
if 'df_access_customers' in locals():
    dfs_to_export_raw['Customers_Access_Notes'] = df_access_customers

for name, df in dfs_to_export_raw.items():
    file_path = os.path.join(RAW_OUTPUT_DIR, f'{name}.csv')
    try:
        df.to_csv(
            file_path,
            index=False,
            sep=';',
            encoding='utf-8'
        )
        print(f"  - Exportation de {name}.csv (RAW) réussie vers {RAW_OUTPUT_DIR}.")
    except Exception as e:
        print(f"  ❌ Échec de l'exportation de {name}.csv (RAW): {e}")

print("--- Exportation des fichiers sources (RAW) terminée ---")


# --- Démarrage de la Transformation (T) ---
# =================================================================
# PARTIE 3 : TRANSFORMATION (T)
# =================================================================

print("\n--- Démarrage de la Transformation (T) ---")

# -----------------------------------------------------------------
# 3.1 Création et Nettoyage de la Dimension Date (DimDate)
# -----------------------------------------------------------------

# Extrait toutes les dates uniques de la table Orders
dates = pd.concat([
    raw_data_sql['Orders']['OrderDate'],
    raw_data_sql['Orders']['RequiredDate'],
    raw_data_sql['Orders']['ShippedDate']
]).drop_duplicates().dropna().to_frame(name='DateKey')

dates['DateKey'] = pd.to_datetime(dates['DateKey'])
dates.rename(columns={'DateKey': 'Date'}, inplace=True)
dates['DateKey'] = dates['Date'].dt.strftime('%Y%m%d').astype(int)

# Création des attributs de temps
dates['Year'] = dates['Date'].dt.year
dates['Quarter'] = dates['Date'].dt.quarter
dates['Month'] = dates['Date'].dt.month
dates['Day'] = dates['Date'].dt.day
dates['DayName'] = dates['Date'].dt.day_name()
dates['MonthName'] = dates['Date'].dt.month_name()

DimDate = dates[['DateKey', 'Date', 'Year', 'Quarter', 'Month', 'Day', 'DayName', 'MonthName']]
print(f"- Création de DimDate (clés uniques : {len(DimDate)}).")


# -----------------------------------------------------------------
# 3.2 Création de la Dimension Clients (DimCustomers)
# -----------------------------------------------------------------

# >>> CORRIGER LE TYPE DE DONNÉES <<<
raw_data_sql['Customers_SQL']['CustomerID'] = raw_data_sql['Customers_SQL']['CustomerID'].astype(str)
df_access_customers['CustomerID'] = df_access_customers['CustomerID'].astype(str)

# Fusion des données de Customers SQL et Access
DimCustomers = raw_data_sql['Customers_SQL'].merge(
    df_access_customers, 
    on='CustomerID', 
    how='left'
)

# Renommage et sélection des colonnes
DimCustomers.rename(columns={
    'CustomerID': 'CustomerKey',
    'ContactName': 'CustomerContactName',
    'CompanyName': 'CustomerCompanyName',
    'Country': 'CustomerCountry',
    'City': 'CustomerCity',
    'Notes': 'CustomerNotes'
}, inplace=True)

# Nettoyage et sélection des attributs
DimCustomers = DimCustomers[[
    'CustomerKey', 'CustomerCompanyName', 'CustomerContactName', 
    'CustomerCountry', 'CustomerCity', 'CustomerNotes'
]]
print(f"- Création de DimCustomers ({len(DimCustomers)} lignes).")


# -----------------------------------------------------------------
# 3.3 Création de la Dimension Produits (DimProducts)
# -----------------------------------------------------------------

# Fusion Products et Categories
DimProducts = raw_data_sql['Products'].merge(
    raw_data_sql['Categories'], 
    on='CategoryID', 
    how='left'
)

# Renommage et sélection des colonnes
DimProducts.rename(columns={
    'ProductID': 'ProductKey',
    'ProductName': 'ProductName',
    'CategoryName': 'CategoryName',
    'UnitsInStock': 'UnitsInStock',
    'UnitPrice': 'StandardPrice'
}, inplace=True)

# Sélection des attributs
DimProducts = DimProducts[[
    'ProductKey', 'ProductName', 'CategoryName', 'StandardPrice', 'UnitsInStock'
]]
print(f"- Création de DimProducts ({len(DimProducts)} lignes).")


# -----------------------------------------------------------------
# 3.4 Création des autres Dimensions (Employees, Shippers)
# -----------------------------------------------------------------

# DimEmployees
DimEmployees = raw_data_sql['Employees'].rename(columns={'EmployeeID': 'EmployeeKey'})
DimEmployees = DimEmployees[['EmployeeKey', 'LastName', 'FirstName', 'Title', 'City', 'Country']]
print(f"- Création de DimEmployees ({len(DimEmployees)} lignes).")

# DimShippers
DimShippers = raw_data_sql['Shippers'].rename(columns={'ShipperID': 'ShipperKey', 'CompanyName': 'ShipperCompanyName'})
DimShippers = DimShippers[['ShipperKey', 'ShipperCompanyName']]
print(f"- Création de DimShippers ({len(DimShippers)} lignes).")


# -----------------------------------------------------------------
# 3.5 Création de la Table de Faits (FactSales)
# -----------------------------------------------------------------

# ⚠️ CORRECTION : Renommer la colonne UnitPrice dans OrderDetails avant la jointure
# (afin d'éviter le suffixe ambigu '_x' et s'assurer que le prix utilisé est le prix unitaire de la commande)
raw_data_sql['OrderDetails'].rename(columns={'UnitPrice': 'SaleUnitPrice'}, inplace=True)

# Jointure des Orders et OrderDetails
FactSales = raw_data_sql['OrderDetails'].merge(
    raw_data_sql['Orders'],
    on='OrderID',
    how='left'
)

# Calcul du prix unitaire total après remise
# MODIFIER 'UnitPrice_x' par 'SaleUnitPrice'
FactSales['SalesAmount'] = FactSales['Quantity'] * FactSales['SaleUnitPrice'] * (1 - FactSales['Discount'])

# Jointure des clés de date (Conversion des dates en clés entières)
FactSales = FactSales.merge(
    DimDate[['Date', 'DateKey']], 
    left_on='OrderDate', 
    right_on='Date', 
    how='left'
).rename(columns={'DateKey': 'OrderDateKey'}).drop(columns=['Date'])

FactSales = FactSales.merge(
    DimDate[['Date', 'DateKey']], 
    left_on='ShippedDate', 
    right_on='Date', 
    how='left'
).rename(columns={'DateKey': 'ShippedDateKey'}).drop(columns=['Date'])

# Renommage final des clés et sélection des mesures et clés
# FactSales.rename(columns={
#     'CustomerID': 'CustomerKey',
#     'EmployeeID': 'EmployeeKey',
#     'ShipperID': 'ShipperKey',
#     'ProductID': 'ProductKey',
#     'Quantity': 'OrderQuantity'
# }, inplace=True)

# --- CORRECTIF : Renommer les colonnes pour correspondre au schéma cible ---
FactSales.rename(columns={
    'ShipVia': 'ShipperID',      # De la table Orders
    'Quantity': 'OrderQuantity',  # De la table Order Details
    'UnitPrice_x': 'SaleUnitPrice' # (Si 'UnitPrice_x' est le nom après votre merge)
}, inplace=True)

# Sélection finale des colonnes de la Fact table
FactSales = FactSales[[
    'OrderID', 
    'CustomerID', 
    'EmployeeID', 
    'ShipperID', 
    'ProductID', 
    'OrderDateKey', 
    'ShippedDateKey',
    'OrderQuantity', 
    'SaleUnitPrice', 
    'Discount', 
    'SalesAmount', 
    'Freight'
]]

print(f"- Création de FactSales ({len(FactSales)} lignes).")
print("--- Transformation (T) terminée ---")
import os
# ... (votre code d'extraction et de transformation)

# --- Démarrage de la Transformation (T) ---
# ... (votre code de création des Dim et Fact)
# --- Transformation (T) terminée ---

# =================================================================
# ÉTAPE : Exportation des DataFrames (CSV)
# =================================================================
print("\n--- Démarrage de l'Exportation des fichiers ---")

# Définition du chemin du dossier de sortie (assurez-vous qu'il existe)
OUTPUT_DIR = 'data/clean/' # Si vous voulez un sous-dossier, sinon utilisez 'data/'

# Crée le dossier s'il n'existe pas
os.makedirs(OUTPUT_DIR, exist_ok=True)


# Définition d'un dictionnaire des DataFrames à exporter
dfs_to_export = {
    'DimDate': DimDate,
    'DimCustomers': DimCustomers,
    'DimProducts': DimProducts,
    'DimEmployees': DimEmployees,
    'DimShippers': DimShippers,
    'FactSales': FactSales
}

for name, df in dfs_to_export.items():
    file_path = os.path.join(OUTPUT_DIR, f'{name}.csv')
    try:
        df.to_csv(
            file_path,
            index=False, # Important : N'inclut pas l'index de Pandas
            sep=';',     # Utilisation du point-virgule comme séparateur pour la lisibilité
            encoding='utf-8'
        )
        print(f"  - Exportation de {name}.csv réussie.")
    except Exception as e:
        print(f"  ❌ Échec de l'exportation de {name}.csv: {e}")

print("--- Exportation des fichiers terminée ---")


# =================================================================
# ÉTAPE SUIVANTE : Le Chargement (L) dans le Data Warehouse...
# =================================================================

# =================================================================
# ÉTAPE FINALE : Le Chargement (L) dans le Data Warehouse (NorthwindDW)
# =================================================================

# --- 1. DÉFINITION DE LA CONNEXION CIBLE ---
SQL_DW_SERVER = r'DESKTOP-F8N2M8C\SQLEXPRESS' 
SQL_DW_DATABASE = 'NorthwindDW' 
SQL_DW_DRIVER = 'ODBC Driver 17 for SQL Server' 

# Chaîne de connexion SQLAlchemy
SQL_DW_CONN_STRING = f'mssql+pyodbc://{SQL_DW_SERVER}/{SQL_DW_DATABASE}?driver={SQL_DW_DRIVER}'

# --- 2. CRÉATION DU MOTEUR ET CONNEXION ---
print("\n--- Démarrage du Chargement (L) ---")

try:
    sql_dw_engine = create_engine(SQL_DW_CONN_STRING)
    print(f"Tentative de connexion au Data Warehouse: {SQL_DW_DATABASE}")
    sql_dw_engine.connect()
    print("✅ Connexion au Data Warehouse réussie.")
except Exception as e:
    print(f"❌ Échec de la connexion au Data Warehouse. Veuillez vérifier les paramètres: {e}")
    # Vous pouvez ajouter 'import sys; sys.exit()' ici si vous voulez stopper l'exécution
    # Si la connexion échoue, le script ne peut pas continuer.
    
    
# --- 3. CHARGEMENT DES TABLES (Dimensions et Faits) ---

# Liste des DataFrames à charger
tables_a_charger = {
    'DimDate': DimDate, 
    'DimCustomers': DimCustomers, 
    'DimProducts': DimProducts,
    'DimEmployees': DimEmployees,
    'DimShippers': DimShippers,
    'FactSales': FactSales 
}

print("\n--- Démarrage du Chargement des tables ---")

# Boucle de chargement
for table_name, df in tables_a_charger.items():
    try:
        df.to_sql(
            name=table_name, 
            con=sql_dw_engine, 
            if_exists='replace', # Écrase les tables à chaque exécution de l'ETL
            index=False          # N'écrit pas l'index de Pandas
        )
        print(f"  - Chargement de la table {table_name} ({len(df)} lignes) réussi.")
    except Exception as e:
        print(f"  ❌ Échec du chargement de la table {table_name}: {e}")


print("\n--- Chargement (L) terminé ---")

# --- 4. NETTOYAGE ---
# Fermeture de la connexion (Bonne pratique)
if 'sql_dw_engine' in locals():
    sql_dw_engine.dispose()