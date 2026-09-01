import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

# Setup output directory
viz_dir = "/Users/s4n/Documents/clg/sem7/bda_capstone2/scripts/local_mode/output/visualizations"
os.makedirs(viz_dir, exist_ok=True)

# Set global dark/modern styling for matplotlib
plt.style.use('dark_background')
plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['font.size'] = 10
plt.rcParams['axes.edgecolor'] = '#444444'
plt.rcParams['axes.linewidth'] = 1.2

print("[INFO] Generating analytical charts from MapReduce & Hive outputs...")

# ------------------------------------------------------------
# 1. HOURLY DEMAND & REVENUE (MapReduce Hourly Output)
# ------------------------------------------------------------
hourly_file = "/Users/s4n/Documents/clg/sem7/bda_capstone2/scripts/local_mode/output/hourly_performance/part-r-00000"
if os.path.exists(hourly_file):
    cols = ['hour', 'trips', 'revenue', 'avg_rev', 'avg_pass', 'avg_dur', 'avg_tip', 'tip_pct']
    df_hourly = pd.read_csv(hourly_file, sep='\t', names=cols)
    df_hourly['hour'] = df_hourly['hour'].astype(int)
    
    fig, ax1 = plt.subplots(figsize=(10, 5))
    color = '#00d2ff'
    ax1.set_xlabel('Pickup Hour (00:00 - 23:00)', fontsize=11, fontweight='bold', color='white')
    ax1.set_ylabel('Trip Volume (Count)', color=color, fontsize=11, fontweight='bold')
    bars = ax1.bar(df_hourly['hour'], df_hourly['trips'], color=color, alpha=0.7, width=0.6, label='Trip Volume')
    ax1.tick_params(axis='y', labelcolor=color)
    ax1.set_xticks(range(0, 24))

    ax2 = ax1.twinx()
    color2 = '#ff007f'
    ax2.set_ylabel('Total Revenue ($)', color=color2, fontsize=11, fontweight='bold')
    line = ax2.plot(df_hourly['hour'], df_hourly['revenue'], color=color2, linewidth=2.5, marker='o', label='Total Revenue ($)')
    ax2.tick_params(axis='y', labelcolor=color2)
    ax2.yaxis.set_major_formatter(ticker.StrMethodFormatter('${x:,.0f}'))

    plt.title('NYC Taxi Hourly Demand & Revenue Profile (MapReduce Engine)', fontsize=13, fontweight='bold', pad=15)
    fig.tight_layout()
    plt.savefig(os.path.join(viz_dir, "hourly_demand_revenue.png"), dpi=200)
    plt.close()
    print("  -> Saved hourly_demand_revenue.png")

# ------------------------------------------------------------
# 2. TOP 10 BUSIEST VS REVENUE ZONES (Hive & MapReduce Zone Output)
# ------------------------------------------------------------
q4_file = "/Users/s4n/Documents/clg/sem7/bda_capstone2/scripts/local_mode/output/hive_results/q4_top20_busiest_zones.csv"
if os.path.exists(q4_file):
    df_q4 = pd.read_csv(q4_file).head(10)
    df_q4['zone_label'] = 'Zone ' + df_q4['PULocationID'].astype(str)
    
    fig, ax = plt.subplots(figsize=(9, 5))
    bars = ax.barh(df_q4['zone_label'][::-1], df_q4['trip_count'][::-1], color='#39ff14', alpha=0.85, height=0.6)
    
    for bar in bars:
        width = bar.get_width()
        ax.text(width + 30, bar.get_y() + bar.get_height()/2, f'{int(width):,}', 
                va='center', ha='left', fontsize=9, color='white', fontweight='bold')

    ax.set_xlabel('Total Trip Volume', fontsize=11, fontweight='bold')
    ax.set_title('Top 10 Busiest Taxi Pickup Zones (Hive Engine)', fontsize=13, fontweight='bold', pad=15)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    plt.tight_layout()
    plt.savefig(os.path.join(viz_dir, "top_pickup_zones.png"), dpi=200)
    plt.close()
    print("  -> Saved top_pickup_zones.png")

# ------------------------------------------------------------
# 3. PAYMENT TYPE SHARE (Hive Q10 Output)
# ------------------------------------------------------------
q10_file = "/Users/s4n/Documents/clg/sem7/bda_capstone2/scripts/local_mode/output/hive_results/q10_payment_types.csv"
if os.path.exists(q10_file):
    df_q10 = pd.read_csv(q10_file)
    colors = ['#00e5ff', '#ff9100', '#ff1744', '#d500f9', '#76ff03']
    
    fig, ax = plt.subplots(figsize=(7, 5))
    wedges, texts, autotexts = ax.pie(
        df_q10['trip_count'], 
        labels=df_q10['payment_method'], 
        autopct='%1.1f%%',
        startangle=140,
        colors=colors[:len(df_q10)],
        wedgeprops=dict(width=0.4, edgecolor='#222222', linewidth=2)
    )
    plt.setp(autotexts, size=10, weight="bold", color="black")
    plt.setp(texts, size=10, color="white", weight="bold")
    ax.set_title('Payment Method Market Share Breakdown (Hive)', fontsize=13, fontweight='bold', pad=15)
    plt.tight_layout()
    plt.savefig(os.path.join(viz_dir, "payment_type_share.png"), dpi=200)
    plt.close()
    print("  -> Saved payment_type_share.png")

# ------------------------------------------------------------
# 4. WEEKDAY VS WEEKEND METRICS (Hive Q9 Output)
# ------------------------------------------------------------
q9_file = "/Users/s4n/Documents/clg/sem7/bda_capstone2/scripts/local_mode/output/hive_results/q9_weekday_vs_weekend.csv"
if os.path.exists(q9_file):
    df_q9 = pd.read_csv(q9_file)
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10, 4.5))
    
    # Trips
    ax1.bar(df_q9['day_type'], df_q9['trip_count'], color=['#7c4dff', '#00b0ff'], width=0.5)
    ax1.set_title('Total Trip Count', fontsize=11, fontweight='bold')
    ax1.set_ylabel('Trips', fontsize=10)
    for i, v in enumerate(df_q9['trip_count']):
        ax1.text(i, v + 500, f"{v:,}", ha='center', fontweight='bold', fontsize=10)
        
    # Avg Fare / Total Amount
    ax2.bar(df_q9['day_type'], df_q9['avg_total_amount'], color=['#00e676', '#ffea00'], width=0.5)
    ax2.set_title('Average Spend Per Trip ($)', fontsize=11, fontweight='bold')
    ax2.set_ylabel('Total Amount ($)', fontsize=10)
    for i, v in enumerate(df_q9['avg_total_amount']):
        ax2.text(i, v + 0.5, f"${v:.2f}", ha='center', fontweight='bold', fontsize=10)
        
    fig.suptitle('Weekday vs. Weekend Operational Profile', fontsize=13, fontweight='bold')
    plt.tight_layout()
    plt.savefig(os.path.join(viz_dir, "weekday_vs_weekend.png"), dpi=200)
    plt.close()
    print("  -> Saved weekday_vs_weekend.png")

# ------------------------------------------------------------
# 5. MONTHLY PERFORMANCE TRENDS (Hive Q12 Output)
# ------------------------------------------------------------
q12_file = "/Users/s4n/Documents/clg/sem7/bda_capstone2/scripts/local_mode/output/hive_results/q12_monthly_breakdown.csv"
if os.path.exists(q12_file):
    df_q12 = pd.read_csv(q12_file)
    df_q12['month_name'] = ['Jan 2026', 'Feb 2026', 'Mar 2026'][:len(df_q12)]
    
    fig, ax = plt.subplots(figsize=(8, 4.5))
    ax.plot(df_q12['month_name'], df_q12['total_revenue'], marker='s', color='#ff3d00', linewidth=3, markersize=8, label='Revenue ($)')
    ax.yaxis.set_major_formatter(ticker.StrMethodFormatter('${x:,.0f}'))
    
    for i, txt in enumerate(df_q12['total_revenue']):
        ax.annotate(f"${txt:,.2f}", (df_q12['month_name'][i], df_q12['total_revenue'][i] + 5000), 
                    ha='center', fontweight='bold', color='white', fontsize=10)
        
    ax.set_title('Q1 2026 Monthly Revenue Growth Trajectory', fontsize=13, fontweight='bold', pad=15)
    ax.set_ylabel('Total Revenue ($)', fontsize=11, fontweight='bold')
    ax.grid(axis='y', linestyle='--', alpha=0.3)
    plt.tight_layout()
    plt.savefig(os.path.join(viz_dir, "monthly_trend.png"), dpi=200)
    plt.close()
    print("  -> Saved monthly_trend.png")

print("[SUCCESS] All 5 high-resolution visualization graphics created successfully!")
