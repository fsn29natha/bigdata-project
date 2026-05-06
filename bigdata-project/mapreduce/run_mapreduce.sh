#!/bin/bash
# =============================================================
#  run_mapreduce.sh - Lance les 3 jobs MapReduce via Hadoop Streaming
#  Prérequis : cluster Hadoop actif (docker-compose up -d)
# =============================================================

HADOOP_STREAMING_JAR="$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar"
HDFS_INPUT="/user/root/netflix/input"
HDFS_OUTPUT_BASE="/user/root/netflix/output"
LOCAL_DATA="./dataset/netflix_titles.csv"

# ============================================================
# 1. Préparer les données sur HDFS
# ============================================================
echo ">>> Création des répertoires HDFS..."
hdfs dfs -mkdir -p $HDFS_INPUT

echo ">>> Copie du dataset vers HDFS..."
hdfs dfs -put -f $LOCAL_DATA $HDFS_INPUT/

echo ">>> Vérification :"
hdfs dfs -ls $HDFS_INPUT

# ============================================================
# 2. Job 1 : Nombre de contenus par genre
# ============================================================
echo ""
echo ">>> [JOB 1] Comptage par Genre..."
hdfs dfs -rm -r -f $HDFS_OUTPUT_BASE/genre_count

hadoop jar $HADOOP_STREAMING_JAR \
    -files ./mapreduce/mapper_genre.py,./mapreduce/reducer_genre.py \
    -mapper  "python3 mapper_genre.py" \
    -reducer "python3 reducer_genre.py" \
    -input   $HDFS_INPUT/netflix_titles.csv \
    -output  $HDFS_OUTPUT_BASE/genre_count

echo ">>> Résultats Job 1 :"
hdfs dfs -cat $HDFS_OUTPUT_BASE/genre_count/part-00000 | head -20

# ============================================================
# 3. Job 2 : Productions par pays et par année
# ============================================================
echo ""
echo ">>> [JOB 2] Productions par Pays/Année..."
hdfs dfs -rm -r -f $HDFS_OUTPUT_BASE/country_year

hadoop jar $HADOOP_STREAMING_JAR \
    -files ./mapreduce/mapper_country_year.py,./mapreduce/reducer_country_year.py \
    -mapper  "python3 mapper_country_year.py" \
    -reducer "python3 reducer_country_year.py" \
    -input   $HDFS_INPUT/netflix_titles.csv \
    -output  $HDFS_OUTPUT_BASE/country_year

echo ">>> Résultats Job 2 :"
hdfs dfs -cat $HDFS_OUTPUT_BASE/country_year/part-00000 | head -20

# ============================================================
# 4. Job 3 : Durée moyenne des films par genre
# ============================================================
echo ""
echo ">>> [JOB 3] Durée Moyenne par Genre..."
hdfs dfs -rm -r -f $HDFS_OUTPUT_BASE/duration_avg

hadoop jar $HADOOP_STREAMING_JAR \
    -files ./mapreduce/mapper_duration.py,./mapreduce/reducer_duration.py \
    -mapper  "python3 mapper_duration.py" \
    -reducer "python3 reducer_duration.py" \
    -input   $HDFS_INPUT/netflix_titles.csv \
    -output  $HDFS_OUTPUT_BASE/duration_avg

echo ">>> Résultats Job 3 :"
hdfs dfs -cat $HDFS_OUTPUT_BASE/duration_avg/part-00000 | head -20

echo ""
echo ">>> Tous les jobs MapReduce sont terminés !"
echo ">>> Interface Web HDFS : http://localhost:9870"
echo ">>> Interface YARN     : http://localhost:8088"
