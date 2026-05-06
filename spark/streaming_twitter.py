"""
=====================================================================
 streaming_twitter.py — PySpark Streaming + ML Sentiment Analysis
 Source : Twitter Streaming API (via Tweepy)
 Algorithme ML : Logistic Regression (Sentiment Analysis)
=====================================================================
PRÉREQUIS :
    pip install tweepy pyspark textblob scikit-learn pandas matplotlib

IMPORTANT : Depuis 2023, l'API Twitter gratuite est limitée.
    Ce script utilise le niveau Elevated Access (ou Free avec limites).
    En mode SIMULATION (--simulate), un générateur de tweets est utilisé.
=====================================================================
"""

import sys
import json
import time
import random
import threading
import argparse
import os
from datetime import datetime

# ─── PySpark Streaming ────────────────────────────────────────────
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, StopWordsRemover
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline

# ─── Machine Learning ─────────────────────────────────────────────
from textblob import TextBlob
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from collections import defaultdict, deque


# ══════════════════════════════════════════════════════════════════
# PARTIE 1 : CONNEXION À L'API TWITTER (Tweepy)
# ══════════════════════════════════════════════════════════════════

class TwitterStreamListener:
    """
    Écoute le flux Twitter et envoie les tweets vers un socket local
    sur lequel PySpark Streaming est connecté.
    """

    def __init__(self, host="localhost", port=9999):
        import tweepy
        import socket

        # ── Credentials Twitter API v2 ───────────────────────────
        # Créer un compte développeur sur https://developer.twitter.com
        # et renseigner vos clés ici ou dans des variables d'environnement
        self.BEARER_TOKEN      = os.getenv("TWITTER_BEARER_TOKEN",      "VOTRE_BEARER_TOKEN")
        self.API_KEY           = os.getenv("TWITTER_API_KEY",           "VOTRE_API_KEY")
        self.API_SECRET        = os.getenv("TWITTER_API_SECRET",        "VOTRE_API_SECRET")
        self.ACCESS_TOKEN      = os.getenv("TWITTER_ACCESS_TOKEN",      "VOTRE_ACCESS_TOKEN")
        self.ACCESS_TOKEN_SECRET = os.getenv("TWITTER_ACCESS_SECRET",   "VOTRE_ACCESS_SECRET")

        self.host   = host
        self.port   = port
        self.socket = None
        self.conn   = None

    def start_socket_server(self):
        """Démarre un serveur socket pour recevoir PySpark"""
        import socket
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind((self.host, self.port))
        self.socket.listen(1)
        print(f"⏳ Attente connexion PySpark sur {self.host}:{self.port}...")
        self.conn, addr = self.socket.accept()
        print(f"✅ PySpark connecté depuis {addr}")

    def stream_to_socket(self, keywords=["python", "bigdata", "AI"]):
        """Stream les tweets filtrés par mots-clés vers le socket"""
        import tweepy

        class TweetHandler(tweepy.StreamingClient):
            def __init__(self, bearer_token, conn, **kwargs):
                super().__init__(bearer_token, **kwargs)
                self.conn = conn

            def on_tweet(self, tweet):
                try:
                    tweet_json = json.dumps({
                        "id":        str(tweet.id),
                        "text":      tweet.text,
                        "timestamp": datetime.now().isoformat()
                    })
                    self.conn.send((tweet_json + "\n").encode("utf-8"))
                except Exception as e:
                    print(f"Erreur envoi tweet : {e}")

            def on_errors(self, errors):
                print(f"Erreur Twitter API : {errors}")
                return True  # Continuer le stream

        self.start_socket_server()

        handler = TweetHandler(self.BEARER_TOKEN, self.conn)

        # Supprimer les règles existantes
        rules = handler.get_rules()
        if rules.data:
            ids = [rule.id for rule in rules.data]
            handler.delete_rules(ids)

        # Ajouter les règles de filtrage
        for kw in keywords:
            handler.add_rules(tweepy.StreamRule(f"{kw} lang:en -is:retweet"))

        print(f"🐦 Streaming tweets avec keywords: {keywords}")
        handler.filter(tweet_fields=["created_at", "lang"])


# ══════════════════════════════════════════════════════════════════
# PARTIE 2 : SIMULATEUR DE TWEETS (mode sans clés API)
# ══════════════════════════════════════════════════════════════════

SAMPLE_TWEETS = [
    # Positifs
    "Python and PySpark are absolutely amazing for big data processing! #bigdata #python",
    "Just finished a machine learning project with Spark MLlib. Incredible results! 🚀",
    "Hadoop + Spark cluster setup was easier than expected. Love the documentation! ✅",
    "Deep learning models are getting better every day! Excited about the future of AI 🤖",
    "Great tutorial on Kafka + Spark streaming. This is revolutionary technology! 👏",
    "Netflix recommendation system using collaborative filtering works perfectly now 😊",
    "Finally understood MapReduce after this amazing tutorial. Highly recommend! ⭐",
    "PySpark DataFrames are so powerful. Processing 10GB in seconds! 💪",
    # Négatifs
    "Hadoop configuration is a nightmare. Spent 3 days on this. Very frustrating 😤",
    "Twitter API rate limits are killing my project. So annoying! 😠",
    "PySpark memory errors again. OutOfMemory exception on the cluster 😡",
    "Docker compose for Hadoop keeps failing. Documentation is terrible 😞",
    "Can't get streaming to work properly. Lots of bugs in the code 😒",
    "Machine learning model overfitting badly. Poor performance on test set 📉",
    # Neutres
    "Reading about Hadoop HDFS architecture for my project",
    "Setting up a new Spark cluster with 3 nodes",
    "Comparing MapReduce vs Spark performance benchmarks",
    "Netflix dataset analysis with PySpark running on YARN",
    "Configuring YARN resource manager for the cluster",
    "Testing different ML algorithms for text classification",
]

def tweet_simulator(host="localhost", port=9999):
    """Envoie des tweets simulés vers le socket"""
    import socket

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((host, port))
    s.listen(1)
    print(f"📡 Simulateur en attente sur {host}:{port}...")
    conn, addr = s.accept()
    print(f"✅ PySpark connecté. Démarrage simulation...")

    try:
        while True:
            tweet_text = random.choice(SAMPLE_TWEETS)
            tweet_json = json.dumps({
                "id":        str(random.randint(1000000, 9999999)),
                "text":      tweet_text,
                "timestamp": datetime.now().isoformat()
            })
            conn.send((tweet_json + "\n").encode("utf-8"))
            time.sleep(random.uniform(0.5, 2.0))  # 1 tweet toutes les ~1 sec
    except Exception as e:
        print(f"Simulateur arrêté : {e}")
    finally:
        conn.close()
        s.close()


# ══════════════════════════════════════════════════════════════════
# PARTIE 3 : MODÈLE ML — ANALYSE DE SENTIMENT
# ══════════════════════════════════════════════════════════════════

def get_sentiment_textblob(text):
    """
    Analyse de sentiment avec TextBlob.
    Retourne : "Positif", "Négatif" ou "Neutre"
    """
    analysis = TextBlob(text)
    polarity = analysis.sentiment.polarity
    if polarity > 0.1:
        return "Positif", polarity
    elif polarity < -0.1:
        return "Négatif", polarity
    else:
        return "Neutre", polarity


def build_spark_ml_model(spark):
    """
    Construit un modèle de classification de sentiment avec Spark MLlib.
    Pipeline : Tokenizer → StopWordsRemover → HashingTF → IDF → LogisticRegression
    """
    # Données d'entraînement étiquetées (0=Négatif, 1=Positif, 2=Neutre)
    training_data = [
        ("I love this amazing product", 1),
        ("This is absolutely wonderful and great", 1),
        ("Excellent work and perfect results", 1),
        ("Best experience ever highly recommend", 1),
        ("Outstanding performance and quality", 1),
        ("This is terrible and horrible", 0),
        ("Awful experience very disappointed", 0),
        ("Worst product I have ever used", 0),
        ("Very bad and frustrating experience", 0),
        ("Complete failure and waste of time", 0),
        ("This is just okay nothing special", 2),
        ("Average product with some features", 2),
        ("It works as expected nothing more", 2),
    ]

    df = spark.createDataFrame(training_data, ["text", "label"])

    # Pipeline ML
    tokenizer      = Tokenizer(inputCol="text", outputCol="words")
    remover        = StopWordsRemover(inputCol="words", outputCol="filtered")
    hashing_tf     = HashingTF(inputCol="filtered", outputCol="raw_features", numFeatures=1000)
    idf            = IDF(inputCol="raw_features", outputCol="features")
    lr             = LogisticRegression(maxIter=20, regParam=0.01)

    pipeline = Pipeline(stages=[tokenizer, remover, hashing_tf, idf, lr])
    model    = pipeline.fit(df)
    print("✅ Modèle ML Spark entraîné avec succès!")
    return model


# ══════════════════════════════════════════════════════════════════
# PARTIE 4 : SPARK STREAMING PRINCIPAL
# ══════════════════════════════════════════════════════════════════

# Stockage pour visualisation temps réel
sentiment_counts  = defaultdict(int)
keyword_counts    = defaultdict(int)
timeline_data     = deque(maxlen=50)  # Fenêtre glissante
results_log       = []

def process_rdd(rdd, spark, ml_model):
    """Traite chaque micro-batch (RDD) du stream"""
    if rdd.isEmpty():
        return

    try:
        records = rdd.collect()
        batch_results = []

        for raw in records:
            try:
                tweet = json.loads(raw.strip())
                text  = tweet.get("text", "")

                if not text:
                    continue

                # ── Analyse de sentiment (TextBlob) ──────────────────
                sentiment, polarity = get_sentiment_textblob(text)
                sentiment_counts[sentiment] += 1

                # ── Extraction des mots-clés ──────────────────────────
                words = [w.lower() for w in text.split()
                         if len(w) > 3 and not w.startswith("#") and not w.startswith("@")]
                for w in words:
                    keyword_counts[w] += 1

                # ── Prédiction avec modèle Spark ML ──────────────────
                tweet_df  = spark.createDataFrame([(text,)], ["text"])
                prediction = ml_model.transform(tweet_df)
                pred_label = prediction.select("prediction").first()[0]
                labels     = {0.0: "Négatif", 1.0: "Positif", 2.0: "Neutre"}
                ml_sentiment = labels.get(pred_label, "Inconnu")

                result = {
                    "text":         text[:80] + "..." if len(text) > 80 else text,
                    "sentiment_tb": sentiment,
                    "sentiment_ml": ml_sentiment,
                    "polarity":     round(polarity, 3),
                    "timestamp":    tweet.get("timestamp", "")
                }

                batch_results.append(result)
                results_log.append(result)

                print(f"  🐦 [{sentiment:8s} | ML:{ml_sentiment:8s}] pol={polarity:+.2f} | {text[:60]}...")

            except json.JSONDecodeError:
                continue
            except Exception as e:
                print(f"Erreur traitement tweet : {e}")
                continue

        if batch_results:
            ts = datetime.now().strftime("%H:%M:%S")
            pos = sum(1 for r in batch_results if r["sentiment_tb"] == "Positif")
            neg = sum(1 for r in batch_results if r["sentiment_tb"] == "Négatif")
            neu = sum(1 for r in batch_results if r["sentiment_tb"] == "Neutre")
            timeline_data.append({"time": ts, "pos": pos, "neg": neg, "neu": neu})
            print(f"\n  📊 Batch [{ts}] : +{pos} / -{neg} / ~{neu} tweets\n")

    except Exception as e:
        print(f"Erreur RDD : {e}")


def start_streaming(host="localhost", port=9999, batch_interval=5):
    """Démarre le job Spark Streaming"""

    spark = SparkSession.builder \
        .appName("Twitter_Sentiment_Streaming") \
        .master("local[2]") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    print("🤖 Entraînement du modèle ML Spark...")
    ml_model = build_spark_ml_model(spark)

    sc  = spark.sparkContext
    ssc = StreamingContext(sc, batch_interval)
    ssc.checkpoint("/tmp/streaming_checkpoint")

    print(f"\n🔌 Connexion au socket {host}:{port}...")
    lines  = ssc.socketTextStream(host, port)
    tweets = lines.filter(lambda x: len(x.strip()) > 0)

    # Traitement par micro-batch
    tweets.foreachRDD(lambda rdd: process_rdd(rdd, spark, ml_model))

    # ── Stats agrégées toutes les 30 secondes (fenêtre glissante) ──
    windowed = tweets.window(windowDuration=30, slideDuration=10)
    windowed.count().pprint()

    print("▶️  Spark Streaming démarré. Ctrl+C pour arrêter.\n")
    ssc.start()

    try:
        ssc.awaitTermination()
    except KeyboardInterrupt:
        print("\n⏹️  Arrêt du streaming...")
        ssc.stop(stopSparkContext=True, stopGraceFully=True)
        save_results()


def save_results():
    """Sauvegarde les résultats et génère les visualisations finales"""
    if not results_log:
        print("Aucun résultat à sauvegarder.")
        return

    os.makedirs("./visualization/output", exist_ok=True)
    df = pd.DataFrame(results_log)
    df.to_csv("./visualization/output/streaming_results.csv", index=False)
    print(f"✅ {len(df)} tweets sauvegardés.")

    # ── Graphique 1 : Distribution des sentiments ─────────────────
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))

    # Pie chart TextBlob
    tb_counts = df["sentiment_tb"].value_counts()
    colors    = {"Positif": "#2ecc71", "Négatif": "#e74c3c", "Neutre": "#3498db"}
    axes[0].pie(
        tb_counts.values,
        labels=tb_counts.index,
        autopct="%1.1f%%",
        colors=[colors.get(l, "gray") for l in tb_counts.index],
        startangle=90
    )
    axes[0].set_title("Sentiment (TextBlob)", fontsize=14)

    # Bar chart ML
    ml_counts = df["sentiment_ml"].value_counts()
    bar_colors = [colors.get(l, "gray") for l in ml_counts.index]
    axes[1].bar(ml_counts.index, ml_counts.values, color=bar_colors, edgecolor="black")
    axes[1].set_title("Sentiment (Spark MLlib)", fontsize=14)
    axes[1].set_ylabel("Nombre de tweets")

    plt.suptitle("Analyse de Sentiment des Tweets en Temps Réel", fontsize=16, fontweight="bold")
    plt.tight_layout()
    plt.savefig("./visualization/output/06_sentiment_distribution.png", dpi=150)
    plt.close()

    # ── Graphique 2 : Top 20 mots-clés ───────────────────────────
    top_kw = sorted(keyword_counts.items(), key=lambda x: -x[1])[:20]
    if top_kw:
        kw_df = pd.DataFrame(top_kw, columns=["mot", "count"])
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.barh(kw_df["mot"][::-1], kw_df["count"][::-1], color="#3498db")
        ax.set_title("Top 20 mots-clés dans les tweets", fontsize=16, fontweight="bold")
        ax.set_xlabel("Fréquence")
        plt.tight_layout()
        plt.savefig("./visualization/output/07_top_keywords.png", dpi=150)
        plt.close()

    # ── Graphique 3 : Évolution temporelle ─────────────────────
    if timeline_data:
        tl = pd.DataFrame(list(timeline_data))
        fig, ax = plt.subplots(figsize=(14, 5))
        ax.plot(range(len(tl)), tl["pos"], color="#2ecc71", label="Positif", linewidth=2)
        ax.plot(range(len(tl)), tl["neg"], color="#e74c3c", label="Négatif", linewidth=2)
        ax.plot(range(len(tl)), tl["neu"], color="#3498db", label="Neutre",  linewidth=2)
        ax.set_title("Évolution du sentiment en temps réel", fontsize=16, fontweight="bold")
        ax.set_xlabel("Batch temporel")
        ax.set_ylabel("Nombre de tweets")
        ax.legend()
        ax.grid(alpha=0.3)
        plt.tight_layout()
        plt.savefig("./visualization/output/08_sentiment_timeline.png", dpi=150)
        plt.close()

    print("✅ Tous les graphiques de streaming sauvegardés !")


# ══════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Twitter Spark Streaming Sentiment Analysis")
    parser.add_argument("--simulate", action="store_true",
                        help="Utiliser le simulateur de tweets (sans clés API)")
    parser.add_argument("--host",     default="localhost", help="Host du socket")
    parser.add_argument("--port",     default=9999, type=int, help="Port du socket")
    parser.add_argument("--interval", default=5,    type=int, help="Intervalle de batch (sec)")
    args = parser.parse_args()

    os.makedirs("./visualization/output", exist_ok=True)

    if args.simulate:
        # Lancer le simulateur dans un thread séparé
        print("🎭 Mode SIMULATION activé (pas besoin de clés API Twitter)")
        sim_thread = threading.Thread(
            target=tweet_simulator,
            args=(args.host, args.port),
            daemon=True
        )
        sim_thread.start()
        time.sleep(1)  # Attendre que le serveur démarre
    else:
        # Lancer le vrai stream Twitter
        print("🐦 Mode TWITTER API activé")
        listener = TwitterStreamListener(args.host, args.port)
        stream_thread = threading.Thread(
            target=listener.stream_to_socket,
            args=(["python", "bigdata", "machinelearning", "AI"],),
            daemon=True
        )
        stream_thread.start()
        time.sleep(2)

    # Démarrer Spark Streaming
    start_streaming(args.host, args.port, args.interval)
