#!/bin/bash
DATASET_LOCAL=/tmp/netflix_titles.csv
DATASET_HDFS=/user/root/netflix/input/netflix_titles.csv
OUTPUT_BASE=/user/root/netflix/output
LOCAL_SCRIPT_DIR=/opt/mapreduce

# Créer les répertoires HDFS nécessaires
hdfs dfs -mkdir -p /user/root/netflix/input

# Copier le dataset dans HDFS (écrase l'ancien si existe)
hdfs dfs -put -f $DATASET_LOCAL $DATASET_HDFS

# Job 1 : Distribution par genre
hdfs dfs -rm -r -f ${OUTPUT_BASE}/genre_count
hadoop jar /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -D mapreduce.job.reduces=1 \
  -input $DATASET_HDFS \
  -output ${OUTPUT_BASE}/genre_count \
  -mapper "python3 $LOCAL_SCRIPT_DIR/mapper_genre.py" \
  -reducer "python3 $LOCAL_SCRIPT_DIR/reducer_genre.py"

echo ">>> Job 1 terminé. Résultats :"
hdfs dfs -cat ${OUTPUT_BASE}/genre_count/part-00000 | head -5

# Job 2 : Productions par pays/année
hdfs dfs -rm -r -f ${OUTPUT_BASE}/country_year
hadoop jar /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -D mapreduce.job.reduces=1 \
  -input $DATASET_HDFS \
  -output ${OUTPUT_BASE}/country_year \
  -mapper "python3 $LOCAL_SCRIPT_DIR/mapper_country_year.py" \
  -reducer "python3 $LOCAL_SCRIPT_DIR/reducer_country_year.py"

echo ">>> Job 2 terminé. Résultats :"
hdfs dfs -cat ${OUTPUT_BASE}/country_year/part-00000 | head -5

# Job 3 : Durée moyenne par genre (films)
hdfs dfs -rm -r -f ${OUTPUT_BASE}/duration_avg
hadoop jar /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -D mapreduce.job.reduces=1 \
  -input $DATASET_HDFS \
  -output ${OUTPUT_BASE}/duration_avg \
  -mapper "python3 $LOCAL_SCRIPT_DIR/mapper_duration.py" \
  -reducer "python3 $LOCAL_SCRIPT_DIR/reducer_duration.py"

echo ">>> Job 3 terminé. Résultats :"
hdfs dfs -cat ${OUTPUT_BASE}/duration_avg/part-00000 | head -5

echo "Tous les jobs MapReduce sont terminés."
