

# 📁 README : Projet Business Intelligence Northwind 

## 🎯 1. Vue d'Ensemble du Projet

Ce projet implémente une solution de Business Intelligence complète, transformant les données opérationnelles de Northwind (SQL Server et Access) en un Data Warehouse (DW) structuré.

  * **Source de Données** : Base de données Northwind (SQL Server) et données complémentaires (Notes Clients) d'un fichier Access.
  * **Cible (DW)** : Base de données `NorthwindDW` sur SQL Server (`DESKTOP-F8N2M8C\SQLEXPRESS`).
  * **Outil d'Analyse** : Microsoft Power BI.

## 🛠️ 2. Choix Techniques et Architecture

### 2.1. Justification du Schéma en Étoile

L'architecture du Data Warehouse est basée sur le **Schéma en Étoile** (Star Schema). Ce modèle est optimisé pour la **rapidité d'analyse** et la simplicité de requêtage dans Power BI.

### 2.2. Bibliothèques Python Utilisées

Le script ETL (`etl.py` dans le dossier `scripts/`) utilise :

  * **`pandas`** : Transformation, nettoyage, et modélisation des données.
  * **`pyodbc`** : Connexion aux bases de données SQL Server et Access pour l'extraction.
  * **`sqlalchemy`** : Chargement des DataFrames dans le Data Warehouse SQL Server.

C'est une excellente idée d'inclure une explication détaillée de votre code ETL dans le `README.md`. Cela montre votre compréhension technique et facilite la reproduction pour l'évaluateur.

Voici la section que vous pouvez utiliser pour expliquer le script `etl.py`, en insistant sur son utilité et son exécution :

-----

## ⚙️ 3. Explication Détaillée du Script ETL (`scripts/etl.py`)

Le script `etl.py` est le cœur de la solution BI. Il automatise le processus de transformation des données transactionnelles en un Data Warehouse prêt pour l'analyse dans Power BI.

### 3.1. Utilité et Rôle du Script

L'utilité principale du script est de garantir que les données sont **unifiées, nettoyées et structurées** selon le Schéma en Étoile avant l'analyse.

  * **Gestion des Sources Hétérogènes :** Le script résout le problème de la source double en extrayant à la fois les données de **SQL Server** et les données complémentaires des **Notes Clients** du fichier Access.
  * **Calcul des Métriques Clés :** Il calcule la métrique analytique fondamentale, `SalesAmount` (Montant des ventes après remise), directement dans la phase de transformation.
  * **Modélisation :** Il crée toutes les tables de dimensions (`DimDate`, `DimCustomers`, `DimProducts`, etc.) et la table de faits (`FactSales`).
  * **Recharge Complète :** À chaque exécution, il garantit la fraicheur des données en écrasant (`if_exists='replace'`) les anciennes tables dans le Data Warehouse `NorthwindDW`.

### 3.2. Exécution du Script (`etl.py`)

Pour que le script s'exécute correctement, il nécessite une configuration des accès aux données et un environnement Python fonctionnel.

#### A. Prérequis Techniques

1.  **Installation des Dépendances :** Les bibliothèques `pandas`, `pyodbc`, et `sqlalchemy` doivent être installées.
    ```bash
    pip install pandas pyodbc sqlalchemy
    ```
2.  **Configuration des Connexions :**
      * Vérifier que le serveur SQL (`DESKTOP-F8N2M8C\SQLEXPRESS`) est accessible.
      * Mettre à jour la variable `ACCESS_FILE_PATH` dans le script avec le chemin complet de votre fichier Access (`Database1.accdb`).

#### B. Commande d'Exécution

Une fois les dépendances installées et le chemin Access configuré, exécutez le script depuis la racine du projet :

```bash
python scripts/etl.py
```

L'exécution se termine par une vérification de la connexion et le chargement des tables dans le Data Warehouse `NorthwindDW`.

### 3.3. Utilisation des Données Transformées

Après l'exécution, les données sont prêtes à être utilisées :

  * **Dans Power BI :** Vous pouvez vous connecter à la source **SQL Server** et sélectionner la base de données `NorthwindDW`. Le modèle sera directement importé dans Power BI, reflétant le Schéma en Étoile.
  * **Archivage :** Les fichiers CSV propres des dimensions et faits sont également exportés vers le dossier `data/clean/` pour l'archivage et la vérification.
## 📁 4. Structure de l'Arborescence du Projet

Le projet respecte l'arborescence demandée, en utilisant `figures/` pour les livrables finaux :

```
Nom_Du_Projet_Northwind/
├── data/
│   ├── raw/           # Données extraites brutes
│   └── clean/         # Données transformées (Schéma en Étoile)
├── scripts/           # Code Python
│   └── etl.py         # Le pipeline ETL
├── figures/           # Livrables : .pbix, Rapport PDF, Screenshots
├── video/             # La vidéo de présentation
├── notebooks/         
└── README.md          # Le présent fichier
```

## 📊 5. Livrables et Résultats

Les produits finaux du projet sont disponibles dans le dossier `figures/` : le **Tableau de Bord** (.pbix).
la documentation est dans le dossier `reports/`
les tableaux CSV sont dans les dossier **resulta*** `data/clean` **source**`data/raw`
les script etl.py **le code d'extraction transformation chargemment** est dans le dossier `scripts`

# remarque 
le dossier venv ent utilser pour telechrger l'environment vertuel pour excuter le ETL son problem de biblioteque ponda