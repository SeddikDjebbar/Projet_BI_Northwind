# Projet BI — Northwind DW 📊

**Résumé :**
Ce projet met en place un pipeline ETL simple pour construire un Data Warehouse (DW) à partir des sources Northwind (SQL Server + Microsoft Access). Le script principal `scripts/etl.py` extrait, transforme et prépare des tables dimensionnelles et une table de faits. Un notebook d'analyse (`notebooks/analysis_notebook.ipynb`) fournit des visualisations et des vérifications sur le DW construit.

---

## Structure du dépôt 🗂️

- `scripts/etl.py` : script ETL (Extraction → Transformation → (Chargement))
- `notebooks/analysis_notebook.ipynb` : notebook d'analyse et visualisation
- `data/raw/` : export des tables sources brutes (CSV)
- `data/clean/` : tables nettoyées prêtes à charger dans le DW (CSV)
- `reports/`, `figures/`, `video/` : livrables et exports

---

## Prérequis 🔧

- Python 3.8+ recommandé
- ODBC Driver 17 for SQL Server (pour `pyodbc`)
- Pilote Microsoft Access (pour `.accdb`) si vous utilisez Access
- Packages Python (ex.) :

```powershell
# Créez et activez un environnement virtuel (Windows PowerShell)
python -m venv venv; .\venv\Scripts\Activate.ps1

# Option A — installer depuis `requirements.txt` (recommandé si présent)
# Créez d'abord le fichier requirements si nécessaire :
# pip freeze > requirements.txt
pip install -r requirements.txt

# Option B — installer directement (si vous n'avez pas de requirements.txt)
pip install -U pip
pip install pandas pyodbc sqlalchemy matplotlib seaborn jupyter

# Commande unique (PowerShell) pour tout faire en une ligne :
# python -m venv venv; .\venv\Scripts\Activate.ps1; pip install -U pip; pip install -r requirements.txt
```

Astuce : vous pouvez créer un `requirements.txt` à partir des packages ci-dessus.

---

## Configuration de `etl.py` ⚙️

Avant d'exécuter le script, modifiez les variables en haut de `scripts/etl.py` :

- `SQL_SERVER_NAME` : nom de votre instance SQL Server (ex. `DESKTOP-XXXX\SQLEXPRESS`)
- `SQL_DATABASE_NAME` : base source (ex. `Northwind`)
- `ACCESS_FILE_PATH` ou `ACCESS_DB_PATH` : chemin absolu vers le fichier `.accdb` (ex. `r'C:\Users\...\Northwind.accdb'`)
- `RAW_OUTPUT_DIR` : dossier où exporter les CSV bruts (par défaut `data/raw/`)

Remarque : le script utilise la connexion Windows (Trusted Connection). Si vous avez besoin d'authentification SQL (login/password), adaptez la chaîne de connexion.

---

## Exécution du pipeline ETL ▶️

1. Activez votre environnement virtuel (voir la section Prérequis).
2. Assurez-vous que SQL Server et Access sont accessibles depuis votre machine (drivers installés).
3. Lancez le script :

```powershell
python scripts\etl.py
```

Comportement attendu :
- Les tables sources sont extraites depuis SQL Server et Access.
- Les exports bruts sont sauvegardés dans `data/raw/` (si la partie d'export est activée).
- Les dimensions (DimDate, DimCustomers, DimProducts, DimEmployees, DimShippers) et la table de faits (FactSales) sont construites en mémoire.
- Le bloc de chargement vers le Data Warehouse (DW) est prêt mais commenté par défaut — vous pouvez utiliser `sqlalchemy.create_engine` pour charger vers votre cible.

---

## Détails du script ETL (fichier : `scripts/etl.py`) 🔍

Le script est organisé en étapes séquentielles (E → T → L) exécutées lorsque vous lancez `python scripts/etl.py` :

1. Extraction (E)
   - SQL Server : connexion via `pyodbc` et `SQL_CONN_STRING`. Tables extraites : `Orders`, `Order Details`, `Customers`, `Products`, `Categories`, `Employees`, `Shippers`, `Suppliers`.
   - Microsoft Access (optionnel) : si `ACCESS_DB_PATH` est configuré, le script lit des tables complémentaires (ex. `Customers_Access`, `OrderDetails_Access`) et les stocke dans `data_access`.

2. Export RAW (optionnel)
   - Les DataFrames extraits peuvent être exportés dans `data/raw/` en CSV (`;` séparateur). Contrôlez le chemin via `RAW_OUTPUT_DIR`.

3. Consolidation multi-source
   - Si la source Access est disponible, les tables équivalentes (Orders, OrderDetails, Customers, Products, Suppliers) sont concaténées à la source SQL. Le script donne la priorité aux lignes SQL (drop_duplicates keep='first') et supprime les doublons sur clés logiques (ex. `OrderID`, `ProductID`).

4. Transformation (T)
   - DimDate : conversion des dates en clé `DateKey` (YYYYMMDD) et création des attributs temporels (Year, Quarter, Month, Day, DayName, MonthName).
   - DimCustomers : concaténation SQL + Access, normalisation/renommage (`CustomerKey`, `CustomerNotes`, ...), ajout de la colonne `Notes` si absente.
   - DimProducts : consolidation produits + jointure avec `Categories` pour obtenir `CategoryName` et `StandardPrice`.
   - DimEmployees/DimShippers/DimSuppliers : renommages et sélection des attributs utiles.
   - FactSales : fusion `OrderDetails` + `Orders`, renommage `UnitPrice`→`SaleUnitPrice`, calcul `SalesAmount = Quantity * SaleUnitPrice * (1 - Discount)`, création des clés de date (`OrderDateKey`, `ShippedDateKey`) et sélection finale des colonnes de faits.

5. Export CLEAN
   - Les dimensions et la table de faits sont exportées dans `data/clean/` en CSV prêts pour chargement ou audit.

6. Chargement (L) vers le Data Warehouse (optionnel, sécurisé)
   - Connexion SQLAlchemy via `create_engine()` et `SQL_DW_CONN_STRING`.
   - Chargement des dimensions avec `to_sql(..., if_exists='replace')`.
   - Pour `FactSales`, le script charge d'abord dans `FactSales_Staging` puis exécute un `DELETE` + `INSERT` en production (transactionnel) pour éviter les incohérences.

Bonnes pratiques, tests & dépannage 🛠️
- Pour tester uniquement l'extraction : commentez les blocs Transformation/Chargement ou exécutez le script par pas dans un REPL.
- Évitez d'écraser une base de production : testez d'abord sur `NorthwindDW` de dev.
- Si Access n'est pas disponible, le script fonctionne en mode SQL-only (consultez les messages d'erreur imprimés).
- Pour rendre le script réutilisable : envisagez de le refactorer en fonctions et d'ajouter des options CLI (`--skip-load`, `--export-raw`, `--dw-conn`).


---

## Chargement vers le Data Warehouse (optionnel) 🏗️

Le code contient un emplacement pour créer une connexion SQLAlchemy et charger les DataFrames :

```python
# Exemple (décommentez et adaptez) :
from sqlalchemy import create_engine
sql_dw_engine = create_engine('mssql+pyodbc://<SERVER>/<DB>?driver=ODBC+Driver+17+for+SQL+Server')
DimCustomers.to_sql('DimCustomers', sql_dw_engine, if_exists='replace', index=False)
FactSales.to_sql('FactSales', sql_dw_engine, if_exists='replace', index=False)
```

Conseils :
- Testez d'abord sur une base de dev `NorthwindDW` avant d'écraser une base de production.
- L'import `create_engine` est volontairement présent et annoté pour éviter les avertissements linters si le bloc reste commenté.

---

## Utilisation du Notebook d'analyse 🧪

Le notebook `notebooks/analysis_notebook.ipynb` contient des cellules pour :
- Se connecter au DW (mettez à jour `SQL_DW_SERVER` et `SQL_DW_DATABASE` dans le notebook si besoin).
- Exécuter des requêtes d'agrégation sur `FactSales` et les dimensions.
- Produire des graphiques (tendance des ventes, top employés, répartition par catégorie, etc.).

Pour l'utiliser :

```powershell
jupyter notebook notebooks\analysis_notebook.ipynb
```

Ou ouvrez le notebook depuis VS Code (extension Jupyter) et exécutez les cellules dans l'ordre.

---

## Résolution des problèmes courants ⚠️

- Erreur de connexion `pyodbc.Error`: vérifiez le nom du serveur, le nom de la base, et que le driver ODBC est installé.
- Problème avec Access: vérifiez l'installation du pilote Access (32 vs 64 bits) et utilisez le chemin absolu vers `.accdb`.
- Avertissement linter « import 'create_engine' is not accessed » : l'import est volontaire, il est annoté dans le code (`# noqa: F401`) car le chargement est optionnel.

---

## Fichiers de sortie 📁

- `data/raw/` : tables originales exportées (CSV)
- `data/clean/` : résultats de transformation (CSV) prêts à être chargés

---


## Auteurs & Licence ✍️

- Auteur : SeddikDjebbar (adaptations personnelles)

---

Bonne utilisation ! ✅