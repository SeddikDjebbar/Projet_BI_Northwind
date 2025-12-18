import pandas as pd
import pyodbc
import os
from sqlalchemy import create_engine  # noqa: F401  # Utilisé dans la partie Chargement (L)
from sqlalchemy import text
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
        'Shippers': 'SELECT * FROM Shippers',
        'Suppliers': 'SELECT * FROM Suppliers'
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

# =================================================================
# PARTIE 1 BIS : EXTRACTION ACCESS (Source Secondaire)
# =================================================================

# Configuration de la connexion Access (CHEMIN ABSOLU UTILISÉ POUR LA FIABILITÉ)
# Le 'r' devant la chaîne sert à ignorer les séquences d'échappement dans les chemins Windows.
ACCESS_DB_PATH = r'C:\Users\tk computer\OneDrive\Bureau\etude\BI\Projet_BI_Northwind\data\Northwind 2012.accdb' 
ACCESS_DRIVER = '{Microsoft Access Driver (*.mdb, *.accdb)}' 

ACCESS_CONN_STRING = (
    f'DRIVER={ACCESS_DRIVER};'
    f'DBQ={ACCESS_DB_PATH};'
)

data_access = {}
print(f"\nTentative de connexion à la source Access: {ACCESS_DB_PATH}")
try:
    access_conn = pyodbc.connect(ACCESS_CONN_STRING)
    print("✅ Connexion à Access réussie.")

    queries_access = {
        'Customers_Access': 'SELECT * FROM Customers',
        'Products_Access': 'SELECT * FROM Products',
        'Suppliers_Access': 'SELECT * FROM Suppliers',
        'Orders_Access': 'SELECT * FROM Orders',
        'OrderDetails_Access': 'SELECT * FROM "Order Details"',
    }
    
    for table_name, query in queries_access.items():
        print(f"  - Extraction de {table_name}...")
        data_access[table_name] = pd.read_sql(query, access_conn) 
        
    access_conn.close()
    print("✅ Extraction des tables de la source Access terminée.")

except pyodbc.Error as e:
    print(f"❌ Échec de la connexion/extraction Access. L'analyse des deux sources sera limitée. Détails: {e}")
    data_access = {}
# --- Démarrage de la Transformation (T) ---
# =================================================================
# =================================================================
# PARTIE 3 : TRANSFORMATION (T)
# =================================================================

print("\n--- Démarrage de la Transformation (T) ---")

# =================================================================
# >>> AJOUT MULTI-SOURCE : CONSOLIDATION DES SOURCES <<<
# Nous utilisons maintenant les DATA CONSOLIDÉES pour toutes les transformations.
# =================================================================

# Nous devons d'abord nous assurer que tous les DataFrames existent, sinon nous utilisons uniquement la source SQL.
# -----------------------------------------------------------------
# 3.0 CONSOLIDATION DES DONNÉES DE FAITS (Orders et OrderDetails)
# -----------------------------------------------------------------

# Consolidation des commandes (Orders)
# La source principale est data['Orders'] (ou raw_data_sql['Orders']).
# Nous allons renommer raw_data_sql en data pour simplifier.
data = raw_data_sql 

if 'Orders_Access' in data_access:
    # On concatène les données de SQL Server et Access, en ignorant l'index.
    Orders_Combined = pd.concat([data['Orders'], data_access['Orders_Access']], ignore_index=True)
    print(f"  - Commandes consolidées : {len(data['Orders'])} (SQL) + {len(data_access['Orders_Access'])} (Access) -> {len(Orders_Combined)} (Total)")
else:
    Orders_Combined = data['Orders'].copy()
    print("  - Utilisation uniquement des commandes SQL Server.")

# Consolidation des détails de commandes (OrderDetails)
if 'OrderDetails_Access' in data_access:
    # On concatène les données de SQL Server et Access.
    OrderDetails_Combined = pd.concat([data['OrderDetails'], data_access['OrderDetails_Access']], ignore_index=True)
    print(f"  - Détails consolidés : {len(data['OrderDetails'])} (SQL) + {len(data_access['OrderDetails_Access'])} (Access) -> {len(OrderDetails_Combined)} (Total)")
else:
    OrderDetails_Combined = data['OrderDetails'].copy()
    print("  - Utilisation uniquement des détails SQL Server.")
# -----------------------------------------------------------------
# 3.0 CONSOLIDATION DES DONNÉES DE FAITS (Orders et OrderDetails)
# -----------------------------------------------------------------

# ... (Le code pour Orders_Combined reste inchangé) ...

# Consolidation des détails de commandes (OrderDetails)
if 'OrderDetails_Access' in data_access:
    # On concatène les données de SQL Server et Access.
    OrderDetails_Combined = pd.concat([data['OrderDetails'], data_access['OrderDetails_Access']], ignore_index=True)
    
    # === NOUVEAU : Dédupliquer les lignes de détail ===
    # Une ligne est unique par la combinaison de l'OrderID et du ProductID.
    initial_len = len(OrderDetails_Combined)
    OrderDetails_Combined.drop_duplicates(subset=['OrderID', 'ProductID'], keep='first', inplace=True)
    
    if len(OrderDetails_Combined) < initial_len:
         print(f"  - Détails de commande : {initial_len - len(OrderDetails_Combined)} doublons retirés.")

    print(f"  - Détails consolidés : {len(data['OrderDetails'])} (SQL) + {len(data_access['OrderDetails_Access'])} (Access) -> {len(OrderDetails_Combined)} (Total Net)")
else:
    OrderDetails_Combined = data['OrderDetails'].copy()
    print("  - Utilisation uniquement des détails SQL Server.")


# === NOUVEAU : Vérification de la taille de FactSales après création ===
# (Ajouter ces lignes juste après la création de FactSales en 3.5)
# ...



# -----------------------------------------------------------------
# 3.1 Création et Nettoyage de la Dimension Date (DimDate)
# -----------------------------------------------------------------

# Extrait toutes les dates uniques de la table Orders CONSOLIDÉE
dates = pd.concat([
    Orders_Combined['OrderDate'], # Utilise les commandes consolidées
    Orders_Combined['RequiredDate'], # Utilise les commandes consolidées
    Orders_Combined['ShippedDate'] # Utilise les commandes consolidées
]).drop_duplicates().dropna().to_frame(name='DateKey')

dates['DateKey'] = pd.to_datetime(dates['DateKey'])
dates.rename(columns={'DateKey': 'Date'}, inplace=True)
dates['DateKey'] = dates['Date'].dt.strftime('%Y%m%d').astype(int)

# Création des attributs de temps (votre code est correct ici)
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

# >>> AJOUT MULTI-SOURCE : CONCATENATION <<<
if 'Customers_Access' in data_access:
    customers_sql = data['Customers_SQL'].copy()
    customers_access = data_access['Customers_Access'].copy()
    
    # CONCATÉNATION et SUPPRESSION des doublons (priorité à SQL Server)
    DimCustomers_Raw = pd.concat([customers_sql, customers_access]).drop_duplicates(subset=['CustomerID'], keep='first')
    print(f"  - Clients consolidés : {len(DimCustomers_Raw)} lignes.")
else:
    DimCustomers_Raw = data['Customers_SQL'].copy()
    print("  - Utilisation uniquement des clients SQL Server.")

# >>> CORRECTION KEYERROR : Ajoute la colonne Notes si elle manque (cas où Access échoue) <<<
if 'Notes' not in DimCustomers_Raw.columns:
    DimCustomers_Raw['Notes'] = None 

# Renommage et sélection des colonnes (utilise DimCustomers_Raw)
DimCustomers = DimCustomers_Raw.rename(columns={
    'CustomerID': 'CustomerKey',
    'ContactName': 'CustomerContactName',
    'CompanyName': 'CustomerCompanyName',
    'Country': 'CustomerCountry',
    'City': 'CustomerCity',
    'Notes': 'CustomerNotes'
})

# Nettoyage et sélection des attributs
DimCustomers = DimCustomers[[
    'CustomerKey', 'CustomerCompanyName', 'CustomerContactName', 
    'CustomerCountry', 'CustomerCity', 'CustomerNotes'
]]
print(f"- Création de DimCustomers ({len(DimCustomers)} lignes).")

# -----------------------------------------------------------------
# 3.3 Création de la Dimension Produits (DimProducts)
# -----------------------------------------------------------------

# >>> AJOUT MULTI-SOURCE : CONCATENATION PRODUITS <<<
if 'Products_Access' in data_access:
    products_sql = data['Products'].copy()
    products_access = data_access['Products_Access'].copy()
    
    # CONCATÉNATION et SUPPRESSION des doublons (priorité à SQL Server)
    DimProducts_Raw = pd.concat([products_sql, products_access]).drop_duplicates(subset=['ProductID'], keep='first')
    print(f"  - Produits consolidés : {len(DimProducts_Raw)} lignes.")
else:
    DimProducts_Raw = data['Products'].copy()
    print("  - Utilisation uniquement des produits SQL Server.")
    
# Fusion DimProducts_Raw (consolidée) et Categories (issue de SQL Server)
DimProducts = DimProducts_Raw.merge(
    data['Categories'], # Categories n'est pas dans Access, donc on utilise SQL
    on='CategoryID', 
    how='left'
)

# Renommage et sélection des colonnes (votre code est correct)
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
# 3.4 Création des autres Dimensions (Employees, Shippers, Suppliers)
# -----------------------------------------------------------------

# DimEmployees (Non affecté par Access dans notre plan)
DimEmployees = data['Employees'].rename(columns={'EmployeeID': 'EmployeeKey'})
DimEmployees = DimEmployees[['EmployeeKey', 'LastName', 'FirstName', 'Title', 'City', 'Country']]
print(f"- Création de DimEmployees ({len(DimEmployees)} lignes).")

# DimShippers (Non affecté par Access dans notre plan)
DimShippers = data['Shippers'].rename(columns={'ShipperID': 'ShipperKey', 'CompanyName': 'ShipperCompanyName'})
DimShippers = DimShippers[['ShipperKey', 'ShipperCompanyName']]
print(f"- Création de DimShippers ({len(DimShippers)} lignes).")

# >>> AJOUT MULTI-SOURCE : CONCATENATION FOURNISSEURS <<<
if 'Suppliers_Access' in data_access:
    suppliers_sql = data['Suppliers'].copy()
    suppliers_access = data_access['Suppliers_Access'].copy()
    
    # CONCATÉNATION et SUPPRESSION des doublons (priorité à SQL Server)
    DimSuppliers_Raw = pd.concat([suppliers_sql, suppliers_access]).drop_duplicates(subset=['SupplierID'], keep='first')
    print(f"  - Fournisseurs consolidés : {len(DimSuppliers_Raw)} lignes.")
else:
    DimSuppliers_Raw = data['Suppliers'].copy()
    print("  - Utilisation uniquement des fournisseurs SQL Server.")

DimSuppliers = DimSuppliers_Raw.rename(columns={'SupplierID': 'SupplierKey', 'CompanyName': 'SupplierCompanyName'})
DimSuppliers = DimSuppliers[['SupplierKey', 'SupplierCompanyName', 'Country']]
print(f"- Création de DimSuppliers ({len(DimSuppliers)} lignes).")


# -----------------------------------------------------------------
# 3.5 Création de la Table de Faits (FactSales)
# -----------------------------------------------------------------

# ⚠️ CORRECTION : Renommer la colonne UnitPrice dans OrderDetails avant la jointure
# (Utilise OrderDetails_Combined)
OrderDetails_Combined.rename(columns={'UnitPrice': 'SaleUnitPrice'}, inplace=True)

# Jointure des Orders et OrderDetails CONSOLIDÉS
FactSales = OrderDetails_Combined.merge(
    Orders_Combined, # Utilise les commandes consolidées
    on='OrderID',
    how='left'
)

# Calcul du prix unitaire total après remise
FactSales['SalesAmount'] = FactSales['Quantity'] * FactSales['SaleUnitPrice'] * (1 - FactSales['Discount'])

# Jointure des clés de date (Conversion des dates en clés entières)
# Le reste de votre code de jointure des clés est correct, il est important qu'il soit après la fusion.

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

# --- CORRECTION : Renommer les colonnes pour correspondre au schéma cible ---
FactSales.rename(columns={
    'ShipVia': 'ShipperID', 
    'Quantity': 'OrderQuantity',
    # Note : 'SaleUnitPrice' a déjà été renommé au début du bloc.
    # Note : Assurez-vous que les colonnes CustomerID, EmployeeID, ProductID sont présentes.
}, inplace=True)

# Sélection finale des colonnes de la Fact table
# N.B. : Il manque la colonne SupplierID dans la Fact Table si elle n'est pas dans OrderDetails ou Orders. 
# Je suppose que votre Fact Table est correcte avec les colonnes listées.
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

# Liste des DataFrames à charger (SANS FactSales)
tables_a_charger = {
    'DimDate': DimDate, 
    'DimCustomers': DimCustomers, 
    'DimProducts': DimProducts,
    'DimEmployees': DimEmployees,
    'DimShippers': DimShippers,
    'DimSuppliers': DimSuppliers
}

print("\n--- Démarrage du Chargement des tables ---")

# Boucle de chargement pour les Dimensions
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

# === NOUVEAU : CHARGEMENT DE FACTSALES SÉPARÉ (Plus sûr) ===
# 1. Chargement dans une table de staging (Staging)
try:
    print(f"  - Chargement de la table FactSales dans STAGING ({len(FactSales)} lignes)...")
    FactSales.to_sql(
        name='FactSales_Staging', 
        con=sql_dw_engine, 
        if_exists='replace', 
        index=False
    )
    
    # 2. Remplacement du contenu de la table de production par les données de staging
    with sql_dw_engine.begin() as connection:
        # Supprime toutes les lignes de FactSales
        connection.execute(text("DELETE FROM FactSales;")) # Utilisez 'text' de sqlalchemy
        
        # Insère toutes les lignes de Staging dans FactSales
        connection.execute(text("INSERT INTO FactSales SELECT * FROM FactSales_Staging;"))

    print(f"  - Chargement de la table FactSales ({len(FactSales)} lignes) réussi via Staging.")
    
except Exception as e:
    print(f"  ❌ Échec du chargement de la table FactSales: {e}")

print("\n--- Chargement (L) terminé ---")

# Assurez-vous d'ajouter from sqlalchemy import create_engine, text
# en tête du script si 'text' n'est pas déjà importé.