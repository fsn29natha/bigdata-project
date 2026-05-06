"""
=====================================================================
 analysis_pyspark.py — Analyse Descriptive avec PySpark
 Dataset : Netflix Movies and TV Shows (Kaggle)
 https://www.kaggle.com/datasets/shivamb/netflix-shows
=====================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import pandas as pd
import os

# ─────────────────────────────────────────
# 1. Initialisation de la SparkSession
# ─────────────────────────────────────────
spark = SparkSession.builder \
    .appName("Netflix_Analyse_Descriptive") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("✅ SparkSession créée.")

# ─────────────────────────────────────────
# 2. Chargement du dataset
# ─────────────────────────────────────────
CSV_PATH = "./dataset/netflix_titles.csv"

df = spark.read.csv(
    CSV_PATH,
    header=True,
    inferSchema=True,
    multiLine=True,
    escape='"'
)

print(f"\n📊 Nombre total de contenus : {df.count()}")
print("📋 Schéma du dataset :")
df.printSchema()
df.show(5, truncate=True)

# ─────────────────────────────────────────
# 3. Nettoyage des données
# ─────────────────────────────────────────
df_clean = df.dropna(subset=["type", "listed_in", "release_year", "country"])
df_clean = df_clean.withColumn("release_year", df_clean["release_year"].cast(IntegerType()))

# Filtrer les années aberrantes
df_clean = df_clean.filter(
    (F.col("release_year") >= 1950) & (F.col("release_year") <= 2023)
)

print(f"\n✅ Après nettoyage : {df_clean.count()} enregistrements")

# ─────────────────────────────────────────
# 4. ANALYSE 1 : Répartition Movies vs TV Shows
# ─────────────────────────────────────────
print("\n" + "="*60)
print("ANALYSE 1 : Répartition Movies vs TV Shows")
print("="*60)

type_dist = df_clean.groupBy("type") \
    .agg(F.count("*").alias("count")) \
    .orderBy(F.desc("count"))

type_dist.show()
type_pd = type_dist.toPandas()

fig, ax = plt.subplots(figsize=(6, 6))
colors = ["#E50914", "#564d4d"]
wedges, texts, autotexts = ax.pie(
    type_pd["count"],
    labels=type_pd["type"],
    autopct="%1.1f%%",
    colors=colors,
    startangle=90,
    textprops={"fontsize": 14}
)
ax.set_title("Répartition Movies vs TV Shows sur Netflix", fontsize=16, fontweight="bold")
plt.tight_layout()
plt.savefig("./visualization/output/01_type_distribution.png", dpi=150)
plt.close()
print("✅ Graphique sauvegardé : 01_type_distribution.png")

# ─────────────────────────────────────────
# 5. ANALYSE 2 : Top 10 genres
# ─────────────────────────────────────────
print("\n" + "="*60)
print("ANALYSE 2 : Top 10 genres les plus populaires")
print("="*60)

# Exploser la colonne listed_in (ex: "Action, Comedy" → deux lignes)
genres_df = df_clean.withColumn(
    "genre",
    F.explode(F.split(F.col("listed_in"), ",\s*"))
)

top_genres = genres_df.groupBy("genre") \
    .agg(F.count("*").alias("count")) \
    .orderBy(F.desc("count")) \
    .limit(15)

top_genres.show()
genres_pd = top_genres.toPandas()

fig, ax = plt.subplots(figsize=(12, 6))
bars = ax.barh(genres_pd["genre"][::-1], genres_pd["count"][::-1], color="#E50914")
ax.set_xlabel("Nombre de contenus", fontsize=12)
ax.set_title("Top 15 genres Netflix les plus représentés", fontsize=16, fontweight="bold")
ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
for bar in bars:
    width = bar.get_width()
    ax.text(width + 5, bar.get_y() + bar.get_height()/2,
            f"{int(width):,}", va="center", fontsize=9)
plt.tight_layout()
plt.savefig("./visualization/output/02_top_genres.png", dpi=150)
plt.close()
print("✅ Graphique sauvegardé : 02_top_genres.png")

# ─────────────────────────────────────────
# 6. ANALYSE 3 : Évolution des contenus par année
# ─────────────────────────────────────────
print("\n" + "="*60)
print("ANALYSE 3 : Évolution de la production par année")
print("="*60)

yearly = df_clean.groupBy("release_year", "type") \
    .agg(F.count("*").alias("count")) \
    .orderBy("release_year")

yearly_pd = yearly.toPandas()

fig, ax = plt.subplots(figsize=(14, 6))
for content_type, color in [("Movie", "#E50914"), ("TV Show", "#B20710")]:
    data = yearly_pd[yearly_pd["type"] == content_type]
    ax.plot(data["release_year"], data["count"],
            marker="o", markersize=3, label=content_type,
            color=color, linewidth=2)

ax.set_xlabel("Année de sortie", fontsize=12)
ax.set_ylabel("Nombre de contenus", fontsize=12)
ax.set_title("Évolution de la production Netflix par année", fontsize=16, fontweight="bold")
ax.legend(fontsize=12)
ax.grid(alpha=0.3)
plt.tight_layout()
plt.savefig("./visualization/output/03_yearly_trend.png", dpi=150)
plt.close()
print("✅ Graphique sauvegardé : 03_yearly_trend.png")

# ─────────────────────────────────────────
# 7. ANALYSE 4 : Top 10 pays producteurs
# ─────────────────────────────────────────
print("\n" + "="*60)
print("ANALYSE 4 : Top 10 pays producteurs")
print("="*60)

countries_df = df_clean.withColumn(
    "pays",
    F.explode(F.split(F.col("country"), ",\s*"))
)

top_countries = countries_df.groupBy("pays") \
    .agg(F.count("*").alias("count")) \
    .orderBy(F.desc("count")) \
    .limit(10)

top_countries.show()
countries_pd = top_countries.toPandas()

fig, ax = plt.subplots(figsize=(10, 6))
ax.bar(countries_pd["pays"], countries_pd["count"], color="#E50914", edgecolor="black", alpha=0.85)
ax.set_xlabel("Pays", fontsize=12)
ax.set_ylabel("Nombre de productions", fontsize=12)
ax.set_title("Top 10 pays producteurs sur Netflix", fontsize=16, fontweight="bold")
plt.xticks(rotation=30, ha="right", fontsize=10)
for i, v in enumerate(countries_pd["count"]):
    ax.text(i, v + 5, str(v), ha="center", fontsize=9)
plt.tight_layout()
plt.savefig("./visualization/output/04_top_countries.png", dpi=150)
plt.close()
print("✅ Graphique sauvegardé : 04_top_countries.png")

# ─────────────────────────────────────────
# 8. ANALYSE 5 : Durée moyenne des films par genre
# ─────────────────────────────────────────
print("\n" + "="*60)
print("ANALYSE 5 : Durée moyenne des films par genre")
print("="*60)

movies_df = df_clean.filter(F.col("type") == "Movie")
movies_df = movies_df.withColumn(
    "duration_min",
    F.regexp_extract(F.col("duration"), r"(\d+)", 1).cast(IntegerType())
)

genre_duration = movies_df.withColumn(
    "genre",
    F.explode(F.split(F.col("listed_in"), ",\s*"))
).groupBy("genre") \
 .agg(
    F.avg("duration_min").alias("avg_duration"),
    F.count("*").alias("nb_films")
 ).filter(F.col("nb_films") >= 20) \
  .orderBy(F.desc("avg_duration")) \
  .limit(12)

genre_duration.show()
dur_pd = genre_duration.toPandas()

fig, ax = plt.subplots(figsize=(12, 6))
bars = ax.bar(dur_pd["genre"], dur_pd["avg_duration"],
              color="#E50914", edgecolor="black", alpha=0.85)
ax.set_ylabel("Durée moyenne (min)", fontsize=12)
ax.set_title("Durée moyenne des films par genre (min. 20 films)", fontsize=16, fontweight="bold")
plt.xticks(rotation=35, ha="right", fontsize=9)
for bar in bars:
    ax.text(bar.get_x() + bar.get_width()/2,
            bar.get_height() + 0.5,
            f"{bar.get_height():.0f}min",
            ha="center", fontsize=8)
plt.tight_layout()
plt.savefig("./visualization/output/05_duration_by_genre.png", dpi=150)
plt.close()
print("✅ Graphique sauvegardé : 05_duration_by_genre.png")

# ─────────────────────────────────────────
# 9. Statistiques globales
# ─────────────────────────────────────────
print("\n" + "="*60)
print("RÉSUMÉ STATISTIQUE")
print("="*60)
df_clean.describe(["release_year"]).show()

print("\n✅ Analyse terminée ! Tous les graphiques sont dans ./visualization/output/")
spark.stop()
