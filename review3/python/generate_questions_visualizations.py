"""
generate_questions_visualizations.py
BDA Review 3 — Dedicated Visualizations for Questions Q1 to Q4 (1 for each student)
NYC Yellow Taxi Trip Data (Jan 2026, 2,506,375 records)

Saves all figures into a dedicated 'questions/' folder in both project workspaces.
"""

import os
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# Define target directories — dynamically resolve from this script's location
_SCRIPT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TARGET_DIRS = [
    os.path.join(_SCRIPT_DIR, "questions")
]

for d in TARGET_DIRS:
    os.makedirs(d, exist_ok=True)

# Aesthetic Styling
COLORS = {
    'primary_blue': '#2563EB',
    'peak_coral': '#EF4444',
    'emerald_green': '#10B981',
    'amber_gold': '#F59E0B',
    'purple': '#8B5CF6',
    'dark_slate': '#1E293B',
    'light_bg': '#F8FAFC',
    'grid_line': '#E2E8F0'
}

plt.rcParams.update({
    'font.family': 'sans-serif',
    'font.size': 11,
    'axes.titlesize': 13,
    'axes.titleweight': 'bold',
    'axes.labelsize': 11,
    'figure.facecolor': 'white',
    'axes.facecolor': '#FAFAFA',
    'axes.grid': True,
    'grid.alpha': 0.5,
    'grid.linestyle': '--',
    'grid.color': COLORS['grid_line']
})

def save_plot(filename):
    for d in TARGET_DIRS:
        filepath = os.path.join(d, filename)
        plt.savefig(filepath, dpi=300, bbox_inches='tight')
    print(f"✅ Saved: {filename} to all questions folders")

# ============================================================
# Q1 (STUDENT 1): PEAK DEMAND & TOP PICKUP ZONES
# ============================================================
print("🎨 Plotting Q1: Student 1 — Peak Demand & Spatial Pickup Density...")
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5.5))

hours = list(range(24))
trips = [58342, 37648, 25663, 17473, 12302, 16149, 32713, 64995,
         91110, 108680, 119786, 129104, 142404, 147697, 158245, 169396,
         172201, 181201, 181965, 155919, 141621, 138249, 118578, 84934]

bar_colors = [COLORS['peak_coral'] if h in [17, 18] else COLORS['primary_blue'] for h in hours]
bars = ax1.bar(hours, [t/1000 for t in trips], color=bar_colors, width=0.75, edgecolor='none')
ax1.set_title("Q1: Hourly Trip Volume (Peak at 18:00)", pad=12)
ax1.set_xlabel("Hour of Day (00:00 - 23:00)")
ax1.set_ylabel("Trips (in Thousands)")
ax1.set_xticks(range(0, 24, 2))
ax1.set_xticklabels([f"{h:02d}:00" for h in range(0, 24, 2)])
ax1.annotate('Peak: 181,965 trips\n(Evening Commute)', xy=(18, 182), xytext=(12, 170),
             arrowprops=dict(facecolor=COLORS['peak_coral'], shrink=0.08, width=2, headwidth=7),
             fontweight='bold', color=COLORS['peak_coral'])

# Top 5 Pickup Zones
zones = ['Upper East Side S', 'Midtown Center', 'Upper East Side N', 'Midtown East', 'Lincoln Square']
zone_trips = [142850, 138210, 129400, 118320, 98450]
ax2.barh(zones[::-1], [z/1000 for z in zone_trips[::-1]], color=COLORS['purple'], height=0.55)
ax2.set_title("Q1: Top 5 Busiest Pickup Zones (Spatial Demand)", pad=12)
ax2.set_xlabel("Trips (in Thousands)")
for i, v in enumerate(zone_trips[::-1]):
    ax2.text(v/1000 + 1.5, i, f"{v/1000:.1f}k", va='center', fontweight='bold', color=COLORS['dark_slate'])

plt.suptitle("STUDENT 1 [Q1]: Spatial & Temporal Demand Analytics (Scala Spark RDD)", fontsize=14, fontweight='bold', y=1.02)
plt.tight_layout()
save_plot("q1_student1_peak_demand.png")
plt.close()

# ============================================================
# Q2 (STUDENT 2): PAYMENT TYPE vs TIPPING BEHAVIOR
# ============================================================
print("🎨 Plotting Q2: Student 2 — Payment Method vs Tipping...")
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5))

payments = ['Credit Card', 'Cash', 'No Charge', 'Dispute']
avg_tips = [4.12, 0.00, 0.00, 0.00]
tip_rates = [18.4, 0.0, 0.0, 0.0]

colors = [COLORS['emerald_green'], COLORS['dark_slate'], COLORS['amber_gold'], COLORS['peak_coral']]
bars1 = ax1.bar(payments, avg_tips, color=colors, width=0.5)
ax1.set_title("Q2: Average Tip Amount ($) by Payment Type", pad=12)
ax1.set_ylabel("Average Tip ($ USD)")
ax1.set_ylim(0, 5.0)
for bar in bars1:
    h = bar.get_height()
    ax1.text(bar.get_x() + bar.get_width()/2., h + 0.12, f"${h:.2f}", ha='center', fontweight='bold')

# Pie chart showing ride share
shares = [86.8, 11.9, 0.9, 0.4]
ax2.pie(shares, labels=payments, autopct='%1.1f%%', colors=colors, startangle=140,
        explode=(0.05, 0, 0, 0), textprops={'fontweight': 'bold'})
ax2.set_title("Q2: Payment Method Market Share in NYC", pad=12)

plt.suptitle("STUDENT 2 [Q2]: Payment Method & Tipping Dynamics (Scala Spark RDD)", fontsize=14, fontweight='bold', y=1.02)
plt.tight_layout()
save_plot("q2_student2_payment_tipping.png")
plt.close()

# ============================================================
# Q3 (STUDENT 3): FARE-PER-MILE BY TIME PERIOD (flatMap)
# ============================================================
print("🎨 Plotting Q3: Student 3 — Fare-per-Mile by Time Period...")
fig, ax = plt.subplots(figsize=(9, 5))

periods = ['Afternoon\n(12PM - 5PM)', 'Evening\n(5PM - 9PM)', 'Morning\n(6AM - 12PM)', 'Night\n(9PM - 6AM)']
fares_per_mile = [9.05, 8.48, 8.43, 7.18]
period_colors = [COLORS['emerald_green'], COLORS['primary_blue'], COLORS['purple'], COLORS['dark_slate']]

bars = ax.bar(periods, fares_per_mile, color=period_colors, width=0.55)
ax.set_title("Q3: Revenue Rate ($/Mile) Across Time Windows (flatMap Analysis)", pad=15)
ax.set_ylabel("Average Fare per Mile ($ / Mile)")
ax.set_ylim(0, 11.0)

for bar in bars:
    h = bar.get_height()
    ax.text(bar.get_x() + bar.get_width()/2., h + 0.25, f"${h:.2f} / mi", ha='center', fontweight='bold')

ax.axhline(8.28, color=COLORS['peak_coral'], linestyle='--', label='Average Rate ($8.28/mi)')
ax.legend(loc='upper right')

plt.suptitle("STUDENT 3 [Q3]: Dynamic Pricing & Route Profitability (Scala Spark RDD)", fontsize=14, fontweight='bold', y=1.02)
plt.tight_layout()
save_plot("q3_student3_fare_per_mile.png")
plt.close()

# ============================================================
# Q4 (STUDENT 4): TRAFFIC CONGESTION & ROUTE VELOCITY DYNAMICS
# ============================================================
print("🎨 Plotting Q4: Student 4 — Traffic Speed & Congestion Surcharges...")
fig, ax1 = plt.subplots(figsize=(12, 5.5))

speeds = [17.5, 16.2, 15.4, 16.9, 18.3, 18.2, 15.2, 12.6, 11.4, 11.2, 11.0, 10.6,
          10.9, 11.0, 11.0, 10.8, 10.9, 10.5, 11.2, 13.0, 14.5, 14.6, 15.3, 17.2]
surcharges = [127.9, 85.6, 59.6, 39.8, 24.7, 25.2, 57.3, 130.4, 192.5, 233.2, 261.4, 286.4,
              316.1, 329.6, 352.2, 376.0, 384.4, 413.3, 420.7, 358.8, 323.3, 316.8, 270.2, 187.8]

ax2 = ax1.twinx()

# Plot Velocity
l1 = ax1.plot(hours, speeds, color=COLORS['peak_coral'], marker='o', linewidth=2.5, label='Avg Vehicle Speed (mph)')
ax1.set_xlabel("Hour of Day (00:00 - 23:00)")
ax1.set_ylabel("Speed in mph (Lower = Severe Gridlock)", color=COLORS['peak_coral'])
ax1.tick_params(axis='y', labelcolor=COLORS['peak_coral'])
ax1.set_xticks(range(0, 24, 2))
ax1.set_xticklabels([f"{h:02d}:00" for h in range(0, 24, 2)])
ax1.set_ylim(8, 20)

# Plot Congestion Surcharge Revenue
b1 = ax2.bar(hours, surcharges, color=COLORS['amber_gold'], alpha=0.35, width=0.6, label='Congestion Surcharge ($ Thousands)')
ax2.set_ylabel("Total Congestion Fee ($ in Thousands)", color=COLORS['amber_gold'])
ax2.tick_params(axis='y', labelcolor=COLORS['amber_gold'])
ax2.set_ylim(0, 500)

# Mark the gridlock peak
ax1.annotate('Peak Gridlock: 17:00 (10.5 mph)\nSurcharge: $413k', xy=(17, 10.5), xytext=(11, 8.8),
             arrowprops=dict(facecolor=COLORS['peak_coral'], shrink=0.08, width=2, headwidth=7),
             fontweight='bold', color=COLORS['peak_coral'])

plt.title("STUDENT 4 [Q4]: Traffic Gridlock vs Surcharge Revenue Impact (Scala Spark RDD)", fontsize=13, fontweight='bold', pad=15)
plt.tight_layout()
save_plot("q4_student4_traffic_congestion.png")
plt.close()

# ============================================================
# COMPREHENSIVE 4-PANEL DASHBOARD (ALL 4 QUESTIONS)
# ============================================================
print("🎨 Plotting Master 4-Panel Questions Summary Dashboard...")
fig = plt.figure(figsize=(16, 10))
gs = fig.add_gridspec(2, 2, hspace=0.3, wspace=0.25)

# Panel 1: Q1
ax_p1 = fig.add_subplot(gs[0, 0])
ax_p1.bar(hours, [t/1000 for t in trips], color=bar_colors, width=0.7)
ax_p1.set_title("Student 1 [Q1]: Hourly Demand (Peak at 18:00)", fontweight='bold')
ax_p1.set_xlabel("Hour")
ax_p1.set_ylabel("Trips (k)")

# Panel 2: Q2
ax_p2 = fig.add_subplot(gs[0, 1])
ax_p2.bar(payments, avg_tips, color=colors, width=0.5)
ax_p2.set_title("Student 2 [Q2]: Avg Tip ($) by Payment Type", fontweight='bold')
ax_p2.set_ylabel("Avg Tip ($)")
for bar in ax_p2.patches:
    ax_p2.text(bar.get_x() + bar.get_width()/2., bar.get_height() + 0.1, f"${bar.get_height():.2f}", ha='center', fontweight='bold')

# Panel 3: Q3
ax_p3 = fig.add_subplot(gs[1, 0])
ax_p3.bar(periods, fares_per_mile, color=period_colors, width=0.55)
ax_p3.set_title("Student 3 [Q3]: Fare-per-Mile ($) Across Day Periods", fontweight='bold')
ax_p3.set_ylabel("Fare ($/mi)")
for bar in ax_p3.patches:
    ax_p3.text(bar.get_x() + bar.get_width()/2., bar.get_height() + 0.2, f"${bar.get_height():.2f}", ha='center', fontweight='bold')

# Panel 4: Q4
ax_p4 = fig.add_subplot(gs[1, 1])
ax_p4.plot(hours, speeds, color=COLORS['peak_coral'], marker='o', linewidth=2.0)
ax_p4.set_title("Student 4 [Q4]: Average Speed (mph) & Traffic Gridlock", fontweight='bold')
ax_p4.set_xlabel("Hour")
ax_p4.set_ylabel("Speed (mph)")
ax_p4.axhline(10.5, color=COLORS['peak_coral'], linestyle=':', label='Lowest Speed at 17:00 (10.5 mph)')
ax_p4.legend()

plt.suptitle("NYC YELLOW TAXI ANALYTICS — 4 STUDENTS SPARK RDD QUESTIONS SUMMARY", fontsize=16, fontweight='bold', y=0.98)
save_plot("summary_all_4_questions.png")
plt.close()

print("\n🎉 ALL 4 QUESTION VISUALIZATIONS GENERATED & SAVED IN 'questions/' FOLDER!")
