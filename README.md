# 🐘 Projet Big Data — Hadoop + Spark + Twitter Streaming

## 📁 Structure du projet

```
bigdata-project/
├── docker/
│   ├── docker-compose.yml          ← Cluster Hadoop (1 NameNode + 2 DataNodes)
│   └── config/
│       ├── core-site.xml
│       ├── hdfs-site.xml
│       ├── mapred-site.xml
│       ├── yarn-site.xml
│       └── hadoop.env
├── mapreduce/
│   ├── mapper_genre.py             ← Job 1 : contenus par genre
│   ├── reducer_genre.py
│   ├── mapper_country_year.py      ← Job 2 : productions par pays/année
│   ├── reducer_country_year.py
│   ├── mapper_duration.py          ← Job 3 : durée moyenne par genre
│   ├── reducer_duration.py
│   └── run_mapreduce.sh            ← Script de lancement des 3 jobs
├── spark/
│   ├── analysis_pyspark.py         ← Analyse descriptive PySpark
│   └── streaming_twitter.py        ← Streaming + ML Sentiment Analysis
├── visualization/
│   └── visualize.py                ← Dashboard graphiques
├── dataset/                        ← Placer netflix_titles.csv ici
├── requirements.txt
└── README.md
```

---

## PARTIE I — Installation du Cluster Hadoop avec Docker

### Prérequis

- Docker Desktop installé (https://docs.docker.com/get-docker/)
- Docker Compose (inclus avec Docker Desktop)
- 4 Go de RAM disponibles minimum

### Étape 1 : Télécharger le projet

```bash
git clone <votre-repo>
cd bigdata-project
```

### Étape 2 : Démarrer le cluster

```bash
cd docker
docker-compose up -d
```

Vérifier que les 5 conteneurs sont actifs :

```bash
docker-compose ps
```

Sortie attendue :
```
NAME              STATUS
namenode          Up (healthy)
datanode1         Up
datanode2         Up
resourcemanager   Up
nodemanager       Up
```

### Étape 3 : Vérifier les interfaces web

| Service              | URL                        | Description              |
|----------------------|----------------------------|--------------------------|
| HDFS NameNode        | http://localhost:9870       | État HDFS, blocs         |
| YARN ResourceManager | http://localhost:8088       | Suivi des jobs MapReduce |
| DataNode 1           | http://localhost:9864       | État DataNode 1          |
| DataNode 2           | http://localhost:9865       | État DataNode 2          |

### Étape 4 : Formatter le NameNode (première fois seulement)

```bash
docker exec -it namenode bash
hdfs namenode -format
exit
```

Puis redémarrer :
```bash
docker-compose restart namenode
```

### Étape 5 : Tester le cluster

```bash
docker exec -it namenode bash

# Créer un répertoire sur HDFS
hdfs dfs -mkdir -p /user/root/test

# Copier un fichier
echo "Hello Hadoop" > /tmp/test.txt
hdfs dfs -put /tmp/test.txt /user/root/test/

# Vérifier
hdfs dfs -ls /user/root/test/
hdfs dfs -cat /user/root/test/test.txt
```

### Arrêter le cluster

```bash
cd docker
docker-compose down        # Arrêter (données conservées)
docker-compose down -v     # Arrêter + supprimer les volumes
```

---

## PARTIE II.A — MapReduce sur le Dataset Netflix

### Dataset

**Source** : Netflix Movies and TV Shows  
**Kaggle** : https://www.kaggle.com/datasets/shivamb/netflix-shows  
**Fichier** : `netflix_titles.csv` (8,807 lignes, 12 colonnes)

**Colonnes** :
| Colonne        | Description                        |
|----------------|------------------------------------|
| show_id        | Identifiant unique                 |
| type           | "Movie" ou "TV Show"               |
| title          | Titre du contenu                   |
| director       | Réalisateur(s)                     |
| cast           | Acteurs principaux                 |
| country        | Pays de production                 |
| date_added     | Date d'ajout sur Netflix           |
| release_year   | Année de sortie                    |
| rating         | Classification (PG, TV-MA, etc.)   |
| duration       | Durée (ex: "90 min" ou "3 Seasons")|
| listed_in      | Genres (ex: "Dramas, Comedies")    |
| description    | Synopsis                           |

**Objectifs d'analyse** :
1. Distribution des contenus par genre (Movies vs TV Shows)
2. Pays les plus producteurs par année
3. Durée moyenne des films par genre

### Exécuter les jobs MapReduce

```bash
# 1. Copier le dataset dans le projet
cp /chemin/vers/netflix_titles.csv ./dataset/

# 2. Se connecter au conteneur namenode
docker exec -it namenode bash

# 3. Copier le projet dans le conteneur
# (depuis l'hôte) :
docker cp ./mapreduce namenode:/opt/mapreduce
docker cp ./dataset/netflix_titles.csv namenode:/tmp/

# 4. Dans le conteneur, lancer les jobs
cd /opt/mapreduce
bash run_mapreduce.sh
```

### Tester localement les mappers/reducers

```bash
pip install -r requirements.txt

# Tester Job 1 (genre)
cat dataset/netflix_titles.csv | python3 mapreduce/mapper_genre.py | \
    sort | python3 mapreduce/reducer_genre.py | head -20

# Tester Job 2 (pays/année)
cat dataset/netflix_titles.csv | python3 mapreduce/mapper_country_year.py | \
    sort | python3 mapreduce/reducer_country_year.py | head -20

# Tester Job 3 (durée)
cat dataset/netflix_titles.csv | python3 mapreduce/mapper_duration.py | \
    sort | python3 mapreduce/reducer_duration.py | head -20
```

---

## PARTIE II.B — PySpark Streaming + Machine Learning

### Installation

```bash
# Installer Java (requis pour Spark)
sudo apt-get install -y openjdk-11-jdk   # Ubuntu/Debian
# ou
brew install openjdk@11                   # macOS

# Installer les dépendances Python
pip install -r requirements.txt

# Télécharger les données TextBlob
python -c "import textblob; textblob.download_corpora()"
```

### Analyse Descriptive PySpark

```bash
python spark/analysis_pyspark.py
```

Génère dans `visualization/output/` :
- `01_type_distribution.png` — Movies vs TV Shows
- `02_top_genres.png` — Top 15 genres
- `03_yearly_trend.png` — Tendance par année
- `04_top_countries.png` — Top 10 pays
- `05_duration_by_genre.png` — Durée par genre

### Twitter Streaming (mode simulation — sans clés API)

```bash
python spark/streaming_twitter.py --simulate
```

### Twitter Streaming (avec vraies clés API)

1. Créer un compte développeur : https://developer.twitter.com
2. Créer une App → obtenir les clés
3. Configurer les variables d'environnement :

```bash
export TWITTER_BEARER_TOKEN="votre_bearer_token"
export TWITTER_API_KEY="votre_api_key"
export TWITTER_API_SECRET="votre_api_secret"
export TWITTER_ACCESS_TOKEN="votre_access_token"
export TWITTER_ACCESS_SECRET="votre_access_secret"

python spark/streaming_twitter.py
```

### Générer les visualisations finales

```bash
python visualization/visualize.py
```

Ouvrir `visualization/output/rapport.html` dans un navigateur.

---

## Algorithme Machine Learning

**Problème** : Classification de sentiment des tweets (3 classes)  
**Algorithme** : Logistic Regression (Spark MLlib)

**Pipeline** :
```
Tweet text → Tokenizer → StopWordsRemover → HashingTF (1000) → IDF → LogisticRegression
```

**Labels** :
- 0 = Négatif (polarity < -0.1)
- 1 = Positif (polarity > +0.1)
- 2 = Neutre

**Double analyse** :
- TextBlob (lexique basé, temps réel)
- Spark MLlib (modèle entraîné, plus robuste)

---

## Résolution des problèmes courants

**Problème** : `Connection refused` sur le NameNode  
**Solution** : Attendre ~30s après `docker-compose up`, puis rafraîchir

**Problème** : `OutOfMemoryError` dans Spark  
**Solution** : Augmenter `spark.executor.memory` dans `analysis_pyspark.py`

**Problème** : `Twitter 429 Too Many Requests`  
**Solution** : Utiliser le mode `--simulate` ou passer au niveau Elevated Access

**Problème** : `JAVA_HOME not set`  
**Solution** : `export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))`
