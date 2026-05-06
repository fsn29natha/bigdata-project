"""
=====================================================================
 visualize.py — Dashboard de visualisation des résultats
 Génère un rapport HTML interactif + graphiques statiques
=====================================================================
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
import os
from pathlib import Path

plt.rcParams.update({
    "figure.facecolor": "#0d0d0d",
    "axes.facecolor":   "#1a1a1a",
    "axes.edgecolor":   "#444",
    "text.color":       "white",
    "axes.labelcolor":  "white",
    "xtick.color":      "white",
    "ytick.color":      "white",
    "grid.color":       "#333",
    "grid.alpha":       0.5,
})

OUTPUT_DIR = Path("./visualization/output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def generate_mapreduce_report():
    """Génère des graphiques de démonstration pour les résultats MapReduce"""

    # ── Données simulées (remplacer par les vrais résultats HDFS) ──
    genre_data = {
        "International Movies": 2752, "Dramas": 2427, "Comedies": 1674,
        "Action & Adventure": 1243, "Documentaries": 869, "Independent Movies": 756,
        "International TV Shows": 1351, "Crime TV Shows": 849, "Kids' TV": 831,
        "Romantic Movies": 616
    }

    country_data = {
        "United States": 3690, "India": 1046, "United Kingdom": 628,
        "Japan": 318, "South Korea": 264, "Canada": 230, "Spain": 220,
        "France": 197, "Mexico": 170, "Australia": 153
    }

    duration_data = {
        "Classic Movies": 112.4, "Sports Movies": 101.2,
        "Action & Adventure": 99.8, "Dramas": 98.1,
        "Sci-Fi & Fantasy": 95.7, "Thrillers": 93.2,
        "Comedies": 91.5, "Horror Movies": 88.3,
        "Documentaries": 86.1, "Romantic Movies": 85.9
    }

    fig = plt.figure(figsize=(20, 16))
    fig.suptitle("📊 Résultats MapReduce — Dataset Netflix",
                 fontsize=22, fontweight="bold", color="white", y=0.98)

    # ── Plot 1 : Top genres ───────────────────────────────────────
    ax1 = fig.add_subplot(2, 2, 1)
    genres = list(genre_data.keys())
    counts = list(genre_data.values())
    colors = plt.cm.Reds(np.linspace(0.4, 0.9, len(genres)))
    bars = ax1.barh(genres[::-1], counts[::-1], color=colors[::-1])
    ax1.set_title("Top 10 Genres (MapReduce Job 1)", fontsize=14, color="white")
    ax1.set_xlabel("Nombre de contenus")
    for bar in bars:
        w = bar.get_width()
        ax1.text(w + 10, bar.get_y() + bar.get_height()/2,
                 f"{w:,}", va="center", fontsize=8, color="white")

    # ── Plot 2 : Top pays ─────────────────────────────────────────
    ax2 = fig.add_subplot(2, 2, 2)
    countries = list(country_data.keys())
    counts2   = list(country_data.values())
    wedge_colors = ["#E50914", "#B20710", "#831B1B", "#FF6B6B",
                    "#FF8E8E", "#FFA8A8", "#FFC0C0", "#FFD4D4",
                    "#FFE8E8", "#FFF0F0"]
    ax2.pie(counts2, labels=countries, autopct="%1.1f%%",
            colors=wedge_colors, startangle=90,
            textprops={"fontsize": 8, "color": "white"})
    ax2.set_title("Top 10 Pays Producteurs (MapReduce Job 2)", fontsize=14, color="white")

    # ── Plot 3 : Durée moyenne ────────────────────────────────────
    ax3 = fig.add_subplot(2, 2, 3)
    dur_genres = list(duration_data.keys())
    dur_values = list(duration_data.values())
    bar_colors = ["#E50914" if v >= 95 else "#FF6B6B" if v >= 90 else "#FFB3B3"
                  for v in dur_values]
    bars3 = ax3.bar(range(len(dur_genres)), dur_values, color=bar_colors)
    ax3.set_xticks(range(len(dur_genres)))
    ax3.set_xticklabels(dur_genres, rotation=35, ha="right", fontsize=8)
    ax3.set_title("Durée Moyenne des Films par Genre (MapReduce Job 3)", fontsize=14, color="white")
    ax3.set_ylabel("Minutes")
    ax3.axhline(y=np.mean(dur_values), color="yellow", linestyle="--",
                alpha=0.7, label=f"Moyenne: {np.mean(dur_values):.0f} min")
    ax3.legend(fontsize=9)
    for bar in bars3:
        ax3.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.3,
                 f"{bar.get_height():.0f}m", ha="center", fontsize=7, color="white")

    # ── Plot 4 : Statistiques globales ────────────────────────────
    ax4 = fig.add_subplot(2, 2, 4)
    stats = {
        "Total Contenus\n(après nettoyage)": "8,807",
        "Films\n(Movies)":                   "6,131",
        "Séries TV\n(TV Shows)":             "2,676",
        "Pays distincts":                    "748",
        "Genres distincts":                  "42",
        "Période couverte":                  "1925–2021"
    }
    ax4.axis("off")
    y_pos = 0.95
    ax4.text(0.5, y_pos, "📋 Statistiques du Dataset",
             transform=ax4.transAxes, ha="center", fontsize=14,
             fontweight="bold", color="white")
    y_pos -= 0.1
    for label, value in stats.items():
        ax4.text(0.1, y_pos, f"▸ {label}:", transform=ax4.transAxes,
                 fontsize=10, color="#aaa")
        ax4.text(0.7, y_pos, value, transform=ax4.transAxes,
                 fontsize=11, fontweight="bold", color="#E50914")
        y_pos -= 0.13

    plt.tight_layout(rect=[0, 0, 1, 0.96])
    out_path = OUTPUT_DIR / "00_mapreduce_dashboard.png"
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"✅ Dashboard MapReduce sauvegardé : {out_path}")


def generate_streaming_demo():
    """Génère un graphique de démonstration pour le streaming"""
    np.random.seed(42)
    n_batches = 40
    time_steps = [f"T+{i*5}s" for i in range(n_batches)]

    # Simuler une tendance réaliste
    base_pos = 12 + np.random.randint(-3, 6, n_batches)
    base_neg = 7  + np.random.randint(-2, 4, n_batches)
    base_neu = 5  + np.random.randint(-1, 3, n_batches)

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 10))
    fig.suptitle("📡 Analyse de Sentiment en Temps Réel — Twitter Streaming",
                 fontsize=18, fontweight="bold", color="white")

    # ── Graphique 1 : Évolution temporelle ───────────────────────
    ax1.fill_between(range(n_batches), base_pos, alpha=0.3, color="#2ecc71")
    ax1.fill_between(range(n_batches), base_neg, alpha=0.3, color="#e74c3c")
    ax1.fill_between(range(n_batches), base_neu, alpha=0.3, color="#3498db")
    ax1.plot(base_pos, color="#2ecc71", linewidth=2, label=f"Positif (moy: {np.mean(base_pos):.0f})")
    ax1.plot(base_neg, color="#e74c3c", linewidth=2, label=f"Négatif (moy: {np.mean(base_neg):.0f})")
    ax1.plot(base_neu, color="#3498db", linewidth=2, label=f"Neutre  (moy: {np.mean(base_neu):.0f})")
    ax1.set_title("Nombre de tweets par sentiment (par batch de 5s)", fontsize=13)
    ax1.set_ylabel("Tweets / batch")
    ax1.legend(loc="upper right", fontsize=10)
    ax1.set_xticks(range(0, n_batches, 5))
    ax1.set_xticklabels(time_steps[::5], rotation=30, fontsize=8)
    ax1.grid(True, alpha=0.3)

    # ── Graphique 2 : Distribution cumulée ───────────────────────
    cum_pos = np.cumsum(base_pos)
    cum_neg = np.cumsum(base_neg)
    cum_neu = np.cumsum(base_neu)
    total   = cum_pos + cum_neg + cum_neu
    ax2.stackplot(range(n_batches),
                  cum_pos/total*100, cum_neg/total*100, cum_neu/total*100,
                  labels=["Positif %", "Négatif %", "Neutre %"],
                  colors=["#2ecc71", "#e74c3c", "#3498db"], alpha=0.8)
    ax2.set_title("Distribution relative cumulée du sentiment (%)", fontsize=13)
    ax2.set_ylabel("Pourcentage (%)")
    ax2.set_ylim(0, 100)
    ax2.legend(loc="upper left", fontsize=10)
    ax2.set_xticks(range(0, n_batches, 5))
    ax2.set_xticklabels(time_steps[::5], rotation=30, fontsize=8)

    plt.tight_layout()
    out_path = OUTPUT_DIR / "09_streaming_demo.png"
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"✅ Dashboard Streaming sauvegardé : {out_path}")


def generate_html_report():
    """Génère un rapport HTML interactif complet"""
    html = """<!DOCTYPE html>
<html lang="fr">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Rapport Big Data — Netflix & Twitter</title>
    <style>
        :root {
            --primary: #E50914;
            --bg:      #141414;
            --card:    #1f1f1f;
            --text:    #e5e5e5;
            --muted:   #999;
        }
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body { font-family: 'Segoe UI', sans-serif; background: var(--bg);
               color: var(--text); padding: 2rem; }
        h1 { color: var(--primary); font-size: 2.5rem; margin-bottom: 0.5rem; }
        h2 { color: var(--primary); font-size: 1.5rem; margin: 2rem 0 1rem; }
        h3 { color: #fff; margin-bottom: 0.5rem; }
        .subtitle { color: var(--muted); font-size: 1.1rem; margin-bottom: 2rem; }
        .card { background: var(--card); border-radius: 12px; padding: 1.5rem;
                margin-bottom: 1.5rem; border-left: 4px solid var(--primary); }
        .grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 1.5rem; }
        .badge { display: inline-block; background: var(--primary);
                 color: white; padding: 0.2rem 0.7rem; border-radius: 99px;
                 font-size: 0.8rem; margin: 0.2rem; }
        img { width: 100%; border-radius: 8px; margin-top: 1rem; }
        code { background: #2a2a2a; padding: 0.2rem 0.5rem; border-radius: 4px;
               font-family: monospace; color: #E50914; }
        table { width: 100%; border-collapse: collapse; margin-top: 1rem; }
        th { background: var(--primary); color: white; padding: 0.7rem; text-align: left; }
        td { padding: 0.6rem; border-bottom: 1px solid #333; }
        tr:hover td { background: #2a2a2a; }
        .stat-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 1rem; }
        .stat-box { background: #2a2a2a; border-radius: 8px; padding: 1rem; text-align: center; }
        .stat-num { font-size: 2rem; font-weight: bold; color: var(--primary); }
        .stat-label { color: var(--muted); font-size: 0.85rem; }
        @media (max-width: 768px) { .grid-2, .stat-grid { grid-template-columns: 1fr; } }
    </style>
</head>
<body>
    <h1>🎬 Projet Big Data</h1>
    <p class="subtitle">Analyse Netflix avec Hadoop MapReduce + PySpark + Twitter Streaming</p>

    <div class="stat-grid">
        <div class="stat-box"><div class="stat-num">8,807</div><div class="stat-label">Contenus Netflix analysés</div></div>
        <div class="stat-box"><div class="stat-num">3</div><div class="stat-label">Jobs MapReduce</div></div>
        <div class="stat-box"><div class="stat-num">5</div><div class="stat-label">Analyses PySpark</div></div>
    </div>

    <h2>🐘 Partie I — Cluster Hadoop (Docker)</h2>
    <div class="card">
        <h3>Architecture du Cluster</h3>
        <table>
            <tr><th>Conteneur</th><th>Rôle</th><th>Port</th></tr>
            <tr><td><code>namenode</code></td><td>NameNode HDFS + ResourceManager</td><td>9870, 8088</td></tr>
            <tr><td><code>datanode1</code></td><td>DataNode 1 (stockage répliqué)</td><td>9864</td></tr>
            <tr><td><code>datanode2</code></td><td>DataNode 2 (stockage répliqué)</td><td>9865</td></tr>
        </table>
        <br>
        <p>Démarrer le cluster :</p>
        <code>cd docker && docker-compose up -d</code>
    </div>

    <h2>📊 Partie II.A — MapReduce sur Netflix</h2>
    <div class="grid-2">
        <div class="card">
            <h3>Dataset : Netflix Movies & TV Shows</h3>
            <span class="badge">Kaggle</span>
            <span class="badge">8,807 entrées</span>
            <span class="badge">12 colonnes</span>
            <p style="margin-top:1rem; color:var(--muted)">
                Colonnes : show_id, type, title, director, cast, country,
                date_added, release_year, rating, duration, listed_in, description
            </p>
        </div>
        <div class="card">
            <h3>Objectifs d'analyse</h3>
            <ul style="margin-left:1rem; margin-top:0.5rem; line-height:2">
                <li>Distribution des contenus par genre et type</li>
                <li>Tendances de production par pays et année</li>
                <li>Durée moyenne des films par genre</li>
            </ul>
        </div>
    </div>
    <img src="00_mapreduce_dashboard.png" alt="Dashboard MapReduce">

    <h2>⚡ Partie II.B — Twitter Streaming (PySpark)</h2>
    <div class="card">
        <h3>Architecture du Pipeline</h3>
        <p style="color:var(--muted); margin-top:0.5rem">
            Twitter API v2 → Tweepy StreamingClient → Socket TCP (port 9999)
            → PySpark DStream → TextBlob + MLlib LogisticRegression → Visualisation
        </p>
        <br>
        <table>
            <tr><th>Champ</th><th>Description</th><th>Exemple</th></tr>
            <tr><td><code>id</code></td><td>Identifiant unique du tweet</td><td>1234567890</td></tr>
            <tr><td><code>text</code></td><td>Contenu du tweet</td><td>"PySpark is amazing!"</td></tr>
            <tr><td><code>timestamp</code></td><td>Horodatage ISO 8601</td><td>2023-05-01T14:30:00</td></tr>
            <tr><td><code>sentiment</code></td><td>Positif / Négatif / Neutre</td><td>Positif</td></tr>
            <tr><td><code>polarity</code></td><td>Score TextBlob [-1, 1]</td><td>+0.65</td></tr>
        </table>
    </div>
    <img src="09_streaming_demo.png" alt="Dashboard Streaming">

    <h2>🤖 Algorithme ML — Logistic Regression</h2>
    <div class="card">
        <p><strong>Pipeline Spark MLlib :</strong></p>
        <p style="color:var(--muted); margin-top:0.5rem; line-height:2">
            <code>Tokenizer</code> → <code>StopWordsRemover</code>
            → <code>HashingTF</code> (1000 features)
            → <code>IDF</code> (TF-IDF weighting)
            → <code>LogisticRegression</code> (C=100, maxIter=20)
        </p>
        <br>
        <p>Lancement streaming :</p>
        <code>python spark/streaming_twitter.py --simulate</code>
        <br><br>
        <p>Avec vraies clés API :</p>
        <code>export TWITTER_BEARER_TOKEN="..." && python spark/streaming_twitter.py</code>
    </div>

    <footer style="text-align:center; margin-top:3rem; color:var(--muted)">
        Projet Big Data — Hadoop + Spark + PySpark Streaming — 2023
    </footer>
</body>
</html>"""

    out_path = OUTPUT_DIR / "rapport.html"
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"✅ Rapport HTML sauvegardé : {out_path}")


if __name__ == "__main__":
    print("🎨 Génération des visualisations...")
    generate_mapreduce_report()
    generate_streaming_demo()
    generate_html_report()
    print("\n✅ Toutes les visualisations générées dans ./visualization/output/")
