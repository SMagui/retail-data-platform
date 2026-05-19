# Retail Data Platform

Pipeline de données en architecture médaillon (Bronze → Silver → Gold) pour un système de vente au détail.

---

## Stack technique

| Composant | Outil |
|---|---|
| Source de données | MySQL 8 (port 3307, conteneur Docker) |
| Stockage distribué | HDFS (Hadoop, conteneur Docker) |
| Traitement | Apache Spark 3.5.1 (PySpark) |
| Sérialisation | Apache Parquet (via PyArrow) |
| Langage | Python 3.11 |
| Environnement | Anaconda (`Envmodule1`) |

---

## Architecture

```
MySQL (source)
    │
    ▼
[Notebook 01] Exploration & découverte des données
    │
    ▼
[Notebook 02] Bronze Layer ──► HDFS /data/bronze/ (CSV bruts)
    │
    ▼
[Notebook 03] Silver Layer ──► data/silver/ (Parquet nettoyé)
    │
    ▼
[Notebook 04] Gold Layer   ──► data/gold/ (Parquet KPIs agrégés)
```

---

## Tables source

Base de données : `formationsql` — 5 tables de 100 lignes chacune.

| Table | Clé primaire | Colonnes |
|---|---|---|
| `clients` | ClientID | Nom, Prénom, Adresse, Email, Téléphone |
| `employes` | EmployeID | Nom, Prénom, Fonction, Email, Téléphone |
| `fournisseurs` | FournisseurID | NomFournisseur, Adresse, Email, Téléphone |
| `produits` | ProduitID | NomProduit, Description, PrixUnitaire, FournisseurID |
| `ventes` | VenteID | DateVente, ClientID, ProduitID, EmployeID, QuantiteVendue, MontantTotal |

---

## Notebook 01 — Exploration MySQL

**Fichier :** `notebooks/01_mysql_exploration.ipynb`

### Objectif
Explorer les données source dans MySQL avant toute transformation : schémas, types, statistiques descriptives.

### Ce que fait le code

**Connexion MySQL via SQLAlchemy**
```python
engine = create_engine(
    "mysql+mysqlconnector://root:<password>@localhost:3307/formationsql"
)
clients_pd = pd.read_sql("SELECT * FROM clients", engine)
```
SQLAlchemy est une librairie Python d'accès aux bases relationnelles. `pd.read_sql()` exécute la requête et retourne un DataFrame Pandas (tableau en mémoire).

**Création d'une SparkSession locale**
```python
spark = SparkSession.builder \
    .master("local[2]") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .getOrCreate()
```
- `local[2]` : Spark tourne sur la machine locale avec 2 threads, sans cluster.
- Arrow activé : accélère les conversions Pandas ↔ Spark.

**Conversion Pandas → Spark**
```python
clients = spark.createDataFrame(clients_pd)
```

**Exploration**
- `df.printSchema()` : types de colonnes détectés.
- `df.describe().show()` : statistiques (count, mean, stddev, min, max).

### Résultats clés
- Ventes : quantités entre 60 et 48 499 unités, montants entre 17 940 € et 45 M€ — dont **14 transactions aberrantes** avec des quantités > 20 000 unités (filtrées en Silver).
- Produits : prix entre 59 € et 999 €, moyenne 487 €. Même nom de produit peut apparaître à des prix très différents (données générées aléatoirement).
- Aucune valeur nulle dans aucune table.

---

## Notebook 02 — Ingestion Bronze (MySQL → HDFS)

**Fichier :** `notebooks/02_mysql_to_bronze.ipynb`

### Objectif
Extraire les données brutes de MySQL et les déposer dans HDFS sans aucune transformation. C'est la couche **Bronze** : fidèle à la source, non modifiée.

### Ce que fait le code

**Connexion au cluster Spark Docker**
```python
spark = SparkSession.builder \
    .master("spark://localhost:7077") \
    .config("spark.driver.host", "host.docker.internal") \
    .getOrCreate()
```
`host.docker.internal` est le nom DNS qui permet aux conteneurs Docker d'atteindre la machine hôte Windows.

**Correction des types Pandas**
```python
ventes_pd["DateVente"]    = pd.to_datetime(ventes_pd["DateVente"])
ventes_pd["MontantTotal"] = pd.to_numeric(ventes_pd["MontantTotal"], errors="coerce")
ventes_pd["MontantTotal"] = ventes_pd["MontantTotal"].fillna(0.0)
```
MySQL retourne `DateVente` comme chaîne. On la convertit en `datetime` pour que Spark la reconnaisse. `errors="coerce"` transforme les valeurs non convertibles en `NaN` plutôt que de planter.

**Dépôt dans HDFS via Docker**
```python
# Copie le CSV dans le conteneur namenode
subprocess.run(["docker", "cp", chemin_csv, "namenode:/tmp/clients.csv"])

# Crée le répertoire HDFS et y dépose le fichier
subprocess.run(["docker", "exec", "namenode", "hdfs", "dfs",
                "-mkdir", "-p", "/data/bronze/clients"])
subprocess.run(["docker", "exec", "namenode", "hdfs", "dfs",
                "-put", "-f", "/tmp/clients.csv", "/data/bronze/clients/"])
```
`subprocess.run()` exécute des commandes shell depuis Python. La commande `hdfs dfs` est l'interface en ligne de commande d'Hadoop pour gérer les fichiers dans HDFS.

### Structure Bronze dans HDFS
```
/data/bronze/
├── clients/clients.csv
├── employes/employes.csv
├── fournisseurs/fournisseurs.csv
├── produits/produits.csv
└── ventes/ventes.csv
```

---

## Notebook 03 — Transformation Silver (Bronze → Silver)

**Fichier :** `notebooks/03_bronze_to_silver.ipynb`

### Objectif
Nettoyer, typer et filtrer les données Bronze. La couche **Silver** est propre, types corrects, valeurs aberrantes exclues — prête pour l'analyse.

### Ce que fait le code

**Lecture des CSV Bronze**
```python
ventes_pd = pd.read_csv(BRONZE + "/ventes.csv")
```
On lit directement les CSV locaux (Bronze) avec Pandas. Spark est initialisé pour la compatibilité future mais les transformations utilisent Pandas sur Windows (pas besoin de `winutils.exe`).

**Nettoyage des types et colonnes dérivées**
```python
ventes_pd["DateVente"]      = pd.to_datetime(ventes_pd["DateVente"])
ventes_pd["Annee"]          = ventes_pd["DateVente"].dt.year
ventes_pd["Mois"]           = ventes_pd["DateVente"].dt.month
ventes_pd["MontantTotal"]   = ventes_pd["MontantTotal"].astype(float)
ventes_pd["QuantiteVendue"] = ventes_pd["QuantiteVendue"].astype(int)
```
MySQL retourne `DateVente` comme chaîne. On la convertit en `datetime` puis on en extrait `Annee` et `Mois` pour faciliter les agrégations Gold.

**Détection et filtrage des valeurs aberrantes — méthode IQR**
```python
Q1  = ventes_pd["QuantiteVendue"].quantile(0.25)
Q3  = ventes_pd["QuantiteVendue"].quantile(0.75)
IQR = Q3 - Q1
seuil_haut = Q3 + 1.5 * IQR          # → 20 810 unités

ventes_clean = ventes_pd[ventes_pd["QuantiteVendue"] <= seuil_haut].copy()
```
La méthode IQR (Interquartile Range) est une technique statistique robuste pour détecter les outliers sans hypothèse sur la distribution. Toute vente avec une `QuantiteVendue` supérieure à `Q3 + 1,5 × IQR` est considérée aberrante pour un contexte retail.

| Indicateur | Valeur |
|---|---|
| Q1 (25e percentile) | 1 542 unités |
| Q3 (75e percentile) | 9 249 unités |
| Seuil haut | **20 810 unités** |
| Lignes supprimées | **14 / 100** |
| Lignes Silver | **86** |

**Écriture Parquet**
```python
df.to_parquet(SILVER + "/" + nom + "/" + nom + ".parquet", index=False)
```

### Structure Silver locale
```
data/silver/
├── clients/clients.parquet        (100 lignes)
├── employes/employes.parquet      (100 lignes)
├── fournisseurs/fournisseurs.parquet (100 lignes)
├── produits/produits.parquet      (100 lignes)
└── ventes/ventes.parquet          (86 lignes — Annee, Mois ajoutés, 14 outliers exclus)
```

---

## Notebook 04 — Couche Gold (KPIs métier)

**Fichier :** `notebooks/04_gold_kpis.ipynb`

### Objectif
Agréger les données Silver pour produire des indicateurs clés de performance (KPIs) directement exploitables.

### Chargement des données Silver
```python
# Lecture Parquet via pandas puis conversion Spark (contournement winutils)
ventes = spark.createDataFrame(pd.read_parquet(f"{SILVER}/ventes/ventes.parquet"))
```

### KPI 1 — CA par année et par mois
```python
ca_par_mois = ventes \
    .groupBy("Annee", "Mois") \
    .agg(F.round(F.sum("MontantTotal"), 2).alias("CA_Total"),
         F.count("VenteID").alias("Nb_Ventes")) \
    .orderBy("Annee", "Mois")
```
`groupBy` regroupe les lignes par valeurs communes. `agg` calcule la somme du CA et le nombre de ventes par groupe. Les colonnes `Annee` et `Mois` créées en Silver rendent cette agrégation directe.

### KPI 2 — Top 10 produits par CA
```python
top_produits = ventes \
    .join(produits, on="ProduitID", how="left") \
    .groupBy("ProduitID", "NomProduit") \
    .agg(F.round(F.sum("MontantTotal"), 2).alias("CA_Total"),
         F.sum("QuantiteVendue").alias("Qte_Vendue"),
         F.count("VenteID").alias("Nb_Ventes")) \
    .orderBy(F.desc("CA_Total")).limit(10)
```
`.join()` fait une jointure SQL entre ventes et produits sur `ProduitID`. `how="left"` conserve toutes les ventes même si un produit a été supprimé. `F.desc()` trie par ordre décroissant.

### KPI 3 — Top 10 clients par CA
```python
top_clients = ventes \
    .join(clients, on="ClientID", how="left") \
    .groupBy("ClientID", "Nom", "Prenom") \
    .agg(F.round(F.sum("MontantTotal"), 2).alias("CA_Total"),
         F.count("VenteID").alias("Nb_Achats")) \
    .orderBy(F.desc("CA_Total")).limit(10)
```

### KPI 4 — Performance des employés
```python
perf_employes = ventes \
    .join(employes, on="EmployeID", how="left") \
    .groupBy("EmployeID", "Nom", "Prenom", "Fonction") \
    .agg(F.round(F.sum("MontantTotal"), 2).alias("CA_Genere"),
         F.count("VenteID").alias("Nb_Ventes"),
         F.round(F.avg("MontantTotal"), 2).alias("Panier_Moyen")) \
    .orderBy(F.desc("CA_Genere"))
```
`F.avg()` calcule le panier moyen (montant moyen par vente) par employé.

### KPI 5 — Top 10 fournisseurs par CA
```python
ca_fournisseurs = ventes \
    .join(produits, on="ProduitID", how="left") \
    .join(fournisseurs, on="FournisseurID", how="left") \
    .groupBy("FournisseurID", "NomFournisseur") \
    .agg(F.round(F.sum("MontantTotal"), 2).alias("CA_Total"),
         F.count("VenteID").alias("Nb_Ventes"),
         F.countDistinct("ProduitID").alias("Nb_Produits")) \
    .orderBy(F.desc("CA_Total")).limit(10)
```
Double jointure : ventes → produits → fournisseurs. `F.countDistinct()` compte le nombre de références distinctes vendues par fournisseur.

### Structure Gold locale
```
data/gold/
├── ca_par_mois/ca_par_mois.parquet
├── top_produits/top_produits.parquet
├── top_clients/top_clients.parquet
├── perf_employes/perf_employes.parquet
└── ca_fournisseurs/ca_fournisseurs.parquet
```

---

## Concepts clés

### Architecture Médaillon
Approche standard en data engineering pour organiser les données en couches de qualité croissante :
- **Bronze** : copie brute et fidèle de la source, aucune modification.
- **Silver** : données nettoyées, typées, déduplication faite — prêtes à l'analyse.
- **Gold** : agrégats et métriques métier — consommables directement par les dashboards.

### Apache Spark — Évaluation Lazy
Chaque transformation Spark (`.withColumn()`, `.filter()`, `.groupBy()`) est **lazy** : elle n'est pas exécutée immédiatement mais enregistrée dans un plan d'exécution. Le calcul réel ne démarre qu'à l'appel d'une **action** comme `.count()`, `.show()` ou `.write`.

### Apache Parquet
Format de fichier orienté colonne, compressé. Contrairement au CSV, il préserve les types de données natifs (int, date, double...) et est nettement plus rapide pour les requêtes analytiques.

### PyArrow
Librairie Python qui implémente le format Arrow (échange de données en mémoire colonne) et Parquet. Utilisée ici pour contourner la dépendance à `winutils.exe` lors de l'écriture de Parquet depuis Spark sur Windows.

### Docker + HDFS
HDFS (Hadoop Distributed File System) tourne dans des conteneurs Docker simulant un mini-cluster. Le conteneur `namenode` est le nœud maître qui gère le catalogue des fichiers. On interagit avec lui via `hdfs dfs` exécuté dans le conteneur.
