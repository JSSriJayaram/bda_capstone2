"""
generate_visualizations.py
NYC Yellow Taxi - Big Data Analytics Pipeline Visualizations

Reads directly from pipeline outputs:
  results/mapreduce/hourly_performance.tsv  (Hourly MapReduce job)
  results/mapreduce/zone_performance.tsv    (Zone MapReduce job)
  results/hive/all_queries_output.txt       (Hive 12-query results)

Run from project root:
  python analysis/generate_visualizations.py
"""

import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

BASE_DIR   = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
HOURLY_TSV = os.path.join(BASE_DIR, "results", "mapreduce", "hourly_performance.tsv")
ZONE_TSV   = os.path.join(BASE_DIR, "results", "mapreduce", "zone_performance.tsv")
HIVE_TXT   = os.path.join(BASE_DIR, "results", "hive", "all_queries_output.txt")
OUT_DIR    = os.path.join(BASE_DIR, "visualizations")
os.makedirs(OUT_DIR, exist_ok=True)

plt.style.use("dark_background")
plt.rcParams.update({"font.family": "sans-serif", "font.size": 10,
                      "axes.edgecolor": "#444444", "axes.linewidth": 1.2})

print("[INFO] Generating visualizations from pipeline outputs...")
print(f"       Output dir: {OUT_DIR}\n")


def parse_hive_block(text, header_col):
    """Return a DataFrame for the Hive result block containing header_col."""
    lines = text.splitlines()
    start = None
    for i, line in enumerate(lines):
        if header_col.lower() in line.lower() and "\t" in line:
            start = i
            break
    if start is None:
        return None
    rows = []
    headers = [h.strip() for h in lines[start].split("\t")]
    for line in lines[start + 1:]:
        line = line.strip()
        if not line or line.startswith("Time taken") or "=" in line or "Hive" in line:
            break
        rows.append([c.strip() for c in line.split("\t")])
    if not rows:
        return None
    return pd.DataFrame(rows, columns=headers)


# 1. HOURLY DEMAND & REVENUE
if os.path.exists(HOURLY_TSV):
    cols = ["hour", "total_trips", "total_revenue", "avg_revenue",
            "avg_passengers", "avg_duration", "avg_tip", "tip_pct"]
    df = pd.read_csv(HOURLY_TSV, sep="\t", names=cols, skiprows=1)
    df["hour"]          = df["hour"].astype(str).str.strip().astype(int)
    df["total_revenue"] = pd.to_numeric(df["total_revenue"], errors="coerce")
    df["total_trips"]   = pd.to_numeric(df["total_trips"],   errors="coerce")
    df = df.sort_values("hour")

    fig, ax1 = plt.subplots(figsize=(11, 5))
    ax1.bar(df["hour"], df["total_trips"], color="#00d2ff", alpha=0.75, width=0.6)
    ax1.set_xlabel("Pickup Hour (00-23)", fontsize=11, fontweight="bold")
    ax1.set_ylabel("Trip Volume", color="#00d2ff", fontsize=11, fontweight="bold")
    ax1.tick_params(axis="y", labelcolor="#00d2ff")
    ax1.set_xticks(range(0, 24))
    ax2 = ax1.twinx()
    ax2.plot(df["hour"], df["total_revenue"], color="#ff007f", linewidth=2.5, marker="o", markersize=5)
    ax2.set_ylabel("Total Revenue ($)", color="#ff007f", fontsize=11, fontweight="bold")
    ax2.tick_params(axis="y", labelcolor="#ff007f")
    ax2.yaxis.set_major_formatter(ticker.StrMethodFormatter("${x:,.0f}"))
    plt.title("NYC Taxi - Hourly Demand & Revenue Profile (MapReduce)", fontsize=13, fontweight="bold", pad=15)
    fig.tight_layout()
    out = os.path.join(OUT_DIR, "hourly_demand_revenue.png")
    plt.savefig(out, dpi=200); plt.close()
    print(f"  -> Saved hourly_demand_revenue.png")
else:
    print(f"  [SKIP] {HOURLY_TSV} not found")


# 2. TOP 10 BUSIEST ZONES
if os.path.exists(ZONE_TSV):
    df_zone = pd.read_csv(ZONE_TSV, sep="\t")
    df_zone.columns = ["PULocationID", "total_trips", "total_revenue",
                       "avg_fare", "avg_distance", "avg_duration", "avg_speed", "avg_tip"]
    df_zone["total_trips"] = pd.to_numeric(df_zone["total_trips"], errors="coerce")
    top10 = df_zone.nlargest(10, "total_trips").reset_index(drop=True)
    top10["zone_label"] = "Zone " + top10["PULocationID"].astype(str)

    fig, ax = plt.subplots(figsize=(9, 5))
    bars = ax.barh(top10["zone_label"][::-1], top10["total_trips"][::-1],
                   color="#39ff14", alpha=0.85, height=0.6)
    for bar in bars:
        w = bar.get_width()
        ax.text(w + 1000, bar.get_y() + bar.get_height() / 2,
                f"{int(w):,}", va="center", ha="left", fontsize=9, color="white", fontweight="bold")
    ax.set_xlabel("Total Trip Volume", fontsize=11, fontweight="bold")
    ax.set_title("Top 10 Busiest Taxi Pickup Zones (MapReduce)", fontsize=13, fontweight="bold", pad=15)
    ax.spines["top"].set_visible(False); ax.spines["right"].set_visible(False)
    plt.tight_layout()
    out = os.path.join(OUT_DIR, "top_pickup_zones.png")
    plt.savefig(out, dpi=200); plt.close()
    print(f"  -> Saved top_pickup_zones.png")
else:
    print(f"  [SKIP] {ZONE_TSV} not found")


# Load Hive text output
hive_text = ""
if os.path.exists(HIVE_TXT):
    with open(HIVE_TXT) as f:
        hive_text = f.read()


# 3. PAYMENT TYPE SHARE (Q10)
if hive_text:
    df_q10 = parse_hive_block(hive_text, "payment_method")
    if df_q10 is not None:
        df_q10["trip_count"] = pd.to_numeric(df_q10["trip_count"], errors="coerce")
        colors = ["#00e5ff", "#ff9100", "#ff1744", "#d500f9", "#76ff03"]
        fig, ax = plt.subplots(figsize=(7, 5))
        wedges, texts, autotexts = ax.pie(
            df_q10["trip_count"], labels=df_q10["payment_method"],
            autopct="%1.1f%%", startangle=140,
            colors=colors[:len(df_q10)],
            wedgeprops=dict(width=0.45, edgecolor="#222222", linewidth=2))
        plt.setp(autotexts, size=10, weight="bold", color="black")
        plt.setp(texts, size=10, weight="bold", color="white")
        ax.set_title("Payment Method Market Share (Hive Q10)", fontsize=13, fontweight="bold", pad=15)
        plt.tight_layout()
        out = os.path.join(OUT_DIR, "payment_type_share.png")
        plt.savefig(out, dpi=200); plt.close()
        print(f"  -> Saved payment_type_share.png")


# 4. WEEKDAY vs WEEKEND (Q9)
if hive_text:
    df_q9 = parse_hive_block(hive_text, "day_type")
    if df_q9 is not None:
        df_q9["trip_count"]       = pd.to_numeric(df_q9["trip_count"],       errors="coerce")
        df_q9["avg_total_amount"] = pd.to_numeric(df_q9["avg_total_amount"], errors="coerce")
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10, 4.5))
        ax1.bar(df_q9["day_type"], df_q9["trip_count"], color=["#7c4dff", "#00b0ff"], width=0.5)
        ax1.set_title("Total Trip Count", fontsize=11, fontweight="bold")
        ax1.set_ylabel("Trips")
        for i, v in enumerate(df_q9["trip_count"]):
            ax1.text(i, v + 10000, f"{int(v):,}", ha="center", fontweight="bold", fontsize=10)
        ax2.bar(df_q9["day_type"], df_q9["avg_total_amount"], color=["#00e676", "#ffea00"], width=0.5)
        ax2.set_title("Avg Spend Per Trip ($)", fontsize=11, fontweight="bold")
        ax2.set_ylabel("Total Amount ($)")
        for i, v in enumerate(df_q9["avg_total_amount"]):
            ax2.text(i, v + 0.3, f"${v:.2f}", ha="center", fontweight="bold", fontsize=10)
        fig.suptitle("Weekday vs Weekend Operational Profile (Hive Q9)", fontsize=13, fontweight="bold")
        plt.tight_layout()
        out = os.path.join(OUT_DIR, "weekday_vs_weekend.png")
        plt.savefig(out, dpi=200); plt.close()
        print(f"  -> Saved weekday_vs_weekend.png")


# 5. MONTHLY REVENUE TREND (Q12)
if hive_text:
    df_q12 = parse_hive_block(hive_text, "pickup_month")
    if df_q12 is not None:
        df_q12["total_revenue"] = pd.to_numeric(df_q12["total_revenue"], errors="coerce")
        df_q12["pickup_month"]  = pd.to_numeric(df_q12["pickup_month"],  errors="coerce")
        df_q12 = df_q12[
            (df_q12["pickup_year"].astype(str) == "2026") &
            (df_q12["pickup_month"].isin([1, 2, 3]))
        ].reset_index(drop=True)
        df_q12["month_name"] = df_q12["pickup_month"].map({1: "Jan 2026", 2: "Feb 2026", 3: "Mar 2026"})
        fig, ax = plt.subplots(figsize=(8, 4.5))
        ax.plot(df_q12["month_name"], df_q12["total_revenue"],
                marker="s", color="#ff3d00", linewidth=3, markersize=8)
        ax.yaxis.set_major_formatter(ticker.StrMethodFormatter("${x:,.0f}"))
        for _, row in df_q12.iterrows():
            ax.annotate(f"${row['total_revenue']:,.0f}",
                        (row["month_name"], row["total_revenue"] + 300_000),
                        ha="center", fontweight="bold", color="white", fontsize=10)
        ax.set_title("Q1 2026 Monthly Revenue Growth (Hive Q12)", fontsize=13, fontweight="bold", pad=15)
        ax.set_ylabel("Total Revenue ($)", fontsize=11, fontweight="bold")
        ax.grid(axis="y", linestyle="--", alpha=0.3)
        plt.tight_layout()
        out = os.path.join(OUT_DIR, "monthly_revenue_trend.png")
        plt.savefig(out, dpi=200); plt.close()
        print(f"  -> Saved monthly_revenue_trend.png")


# 6. ZONE SPEED vs AVG FARE SCATTER
if os.path.exists(ZONE_TSV):
    df_zone = pd.read_csv(ZONE_TSV, sep="\t")
    df_zone.columns = ["PULocationID", "total_trips", "total_revenue",
                       "avg_fare", "avg_distance", "avg_duration", "avg_speed", "avg_tip"]
    for col in ["total_trips", "total_revenue", "avg_speed", "avg_fare"]:
        df_zone[col] = pd.to_numeric(df_zone[col], errors="coerce")
    df_zone = df_zone.dropna()
    fig, ax = plt.subplots(figsize=(9, 5))
    sc = ax.scatter(df_zone["avg_speed"], df_zone["avg_fare"],
                    c=df_zone["total_trips"], cmap="plasma", s=60, alpha=0.75, edgecolors="none")
    cbar = plt.colorbar(sc, ax=ax)
    cbar.set_label("Total Trips per Zone", color="white")
    cbar.ax.yaxis.set_tick_params(color="white")
    plt.setp(cbar.ax.yaxis.get_ticklabels(), color="white")
    ax.set_xlabel("Average Speed (mph)", fontsize=11, fontweight="bold")
    ax.set_ylabel("Average Fare ($)", fontsize=11, fontweight="bold")
    ax.set_title("Zone Speed vs Avg Fare - 261 Zones (MapReduce)", fontsize=13, fontweight="bold", pad=15)
    plt.tight_layout()
    out = os.path.join(OUT_DIR, "zone_speed_vs_fare.png")
    plt.savefig(out, dpi=200); plt.close()
    print(f"  -> Saved zone_speed_vs_fare.png")

print("\n[SUCCESS] All visualizations saved to results/visualizations/")
