"""
Downstream Analytics & Visualization Module (CS341 Submission 2)
================================================================
ประมวลผลตัวชี้วัดและสร้างกราฟสรุปผลจากชุดข้อมูลที่ผ่านการจัดเตรียมแล้ว (Curated Output)
โดยไม่ต้องกลับไปอ่านไฟล์ Raw Parquet 149.6 ล้านแถวซ้ำ (Decoupled Storage & Compute)
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Dict

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd

from src.config import (
    CURATED_BOROUGH_PARQUET,
    CURATED_MONTHLY_PARQUET,
    DQ_REPORT_CSV_PATH,
    DQ_SUMMARY_JSON_PATH,
    FIGURES_DIR,
    FINAL_ANALYSIS_CSV_PATH,
)


def run_analytics() -> pd.DataFrame:
    """
    อ่านข้อมูลจาก Curated Layer เพื่อตอบโจทย์:
    "Which taxi type was the most used in the first half of 2024?"
    ทั้งในมิติจำนวนเที่ยว (Trip Volume), เวลารวมที่ให้บริการ (Total Ride Hours),
    ระยะทางรวม (Total Distance), และพื้นที่ให้บริการ (Borough Distribution)
    """
    t0 = time.time()
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)

    if not CURATED_MONTHLY_PARQUET.exists():
        raise FileNotFoundError(
            f"Curated dataset not found at {CURATED_MONTHLY_PARQUET}. Run `python -m src.pipeline` first."
        )

    monthly_df = pd.read_parquet(CURATED_MONTHLY_PARQUET)
    borough_df = pd.read_parquet(CURATED_BOROUGH_PARQUET)
    dq_df = pd.read_csv(DQ_REPORT_CSV_PATH)

    # 1. สรุปตัวชี้วัดหลักแยกตามประเภทแท็กซี่ (H1 2024 Summary)
    summary_rows = []
    for taxi_type, grp in monthly_df.groupby("taxi_type"):
        raw_trips = int(grp["raw_rows"].sum())
        sub1_trips = int(grp["baseline_clean_rows"].sum())
        sub2_trips = int(grp["strict_clean_rows"].sum())
        dropped_trips = int(grp["strict_dropped_rows"].sum())
        total_hours = float(grp["total_duration_hours"].sum())

        # Weighted average duration (minutes)
        avg_dur_min = (total_hours * 60.0 / sub2_trips) if sub2_trips > 0 else 0.0

        # Distance & Fare (available for yellow, green, fhvhv; NULL for fhv)
        if grp["total_distance_miles"].notna().any():
            total_dist = round(float(grp["total_distance_miles"].sum()), 2)
            avg_dist = round(total_dist / sub2_trips, 2) if sub2_trips > 0 else None
        else:
            total_dist = None
            avg_dist = None

        if grp["total_fare_usd"].notna().any():
            total_fare = round(float(grp["total_fare_usd"].sum()), 2)
            avg_fare = round(total_fare / sub2_trips, 2) if sub2_trips > 0 else None
        else:
            total_fare = None
            avg_fare = None

        # Top Borough from curated borough dataset
        b_sub = borough_df[borough_df["taxi_type"] == taxi_type]
        if not b_sub.empty:
            top_b = (
                b_sub.groupby("pickup_borough")["clean_trips"]
                .sum()
                .sort_values(ascending=False)
                .index[0]
            )
        else:
            top_b = "Unknown"

        summary_rows.append(
            {
                "taxi_type": taxi_type,
                "raw_trips": raw_trips,
                "sub1_baseline_clean_trips": sub1_trips,
                "sub2_strict_clean_trips": sub2_trips,
                "dropped_anomalies": dropped_trips,
                "retention_rate_pct": round((sub2_trips / raw_trips) * 100.0, 4),
                "total_duration_hours": round(total_hours, 2),
                "avg_duration_minutes": round(avg_dur_min, 2),
                "total_distance_miles": total_dist,
                "avg_distance_miles": avg_dist,
                "total_fare_usd": total_fare,
                "avg_fare_usd": avg_fare,
                "top_pickup_borough": top_b,
            }
        )

    summary_df = pd.DataFrame(summary_rows).sort_values(
        by="sub2_strict_clean_trips", ascending=False
    ).reset_index(drop=True)

    total_clean_all = summary_df["sub2_strict_clean_trips"].sum()
    total_hours_all = summary_df["total_duration_hours"].sum()
    summary_df["trip_market_share_pct"] = (
        summary_df["sub2_strict_clean_trips"] / total_clean_all * 100.0
    ).round(2)
    summary_df["duration_market_share_pct"] = (
        summary_df["total_duration_hours"] / total_hours_all * 100.0
    ).round(2)
    summary_df["rank"] = range(1, len(summary_df) + 1)

    # Reorder columns nicely
    cols_order = [
        "rank",
        "taxi_type",
        "sub2_strict_clean_trips",
        "trip_market_share_pct",
        "sub1_baseline_clean_trips",
        "raw_trips",
        "dropped_anomalies",
        "retention_rate_pct",
        "total_duration_hours",
        "duration_market_share_pct",
        "avg_duration_minutes",
        "total_distance_miles",
        "avg_distance_miles",
        "total_fare_usd",
        "avg_fare_usd",
        "top_pickup_borough",
    ]
    summary_df = summary_df[cols_order]
    summary_df.to_csv(FINAL_ANALYSIS_CSV_PATH, index=False)

    # 2. Generate Visualizations
    _plot_most_used_comparison(summary_df)
    _plot_monthly_trends(monthly_df)
    _plot_dq_anomalies(dq_df)
    _plot_borough_distribution(borough_df)

    elapsed = time.time() - t0
    winner = summary_df.iloc[0]
    print("\n" + "=" * 75)
    print("🏆 FINAL ANALYTICAL RESULT (FROM CURATED DATASET)")
    print("=" * 75)
    print(summary_df.to_string(index=False))
    print("-" * 75)
    print(
        f"🥇 MOST USED TAXI TYPE IN H1 2024: '{winner['taxi_type'].upper()}'\n"
        f"   • Clean Trips (Sub 2 Strict): {winner['sub2_strict_clean_trips']:,} trips "
        f"({winner['trip_market_share_pct']:.2f}% market share)\n"
        f"   • Clean Trips (Sub 1 Baseline): {winner['sub1_baseline_clean_trips']:,} trips\n"
        f"   • Total Passenger Ride Time: {winner['total_duration_hours']:,.2f} hours "
        f"({winner['duration_market_share_pct']:.2f}% of total duration)\n"
        f"   • Analytics query & plotting time from Curated Layer: {elapsed:.2f}s"
    )
    print("=" * 75)
    return summary_df


def _plot_most_used_comparison(summary_df: pd.DataFrame) -> None:
    """Figure 1: เปรียบเทียบจำนวนเที่ยว (Total Trips) และเวลารวม (Total Hours) ของแท็กซี่ 4 ประเภท"""
    plt.style.use("seaborn-v0_8-whitegrid" if "seaborn-v0_8-whitegrid" in plt.style.available else "default")
    fig, axes = plt.subplots(1, 2, figsize=(14, 5.5), dpi=160)

    palette = {"fhvhv": "#1f77b4", "yellow": "#f1c40f", "fhv": "#7f8c8d", "green": "#2ecc71"}
    labels = [t.upper() for t in summary_df["taxi_type"]]
    colors = [palette.get(t, "#333333") for t in summary_df["taxi_type"]]

    # Left panel: Total Clean Trips (Millions)
    trips_m = summary_df["sub2_strict_clean_trips"] / 1e6
    bars1 = axes[0].bar(labels, trips_m, color=colors, edgecolor="#222222", width=0.6)
    axes[0].set_title("Total Clean Trips in H1 2024 (Millions)", fontsize=12, fontweight="bold")
    axes[0].set_ylabel("Trips (Millions)", fontsize=11)
    axes[0].set_ylim(0, max(trips_m) * 1.18)
    for bar, share, exact in zip(bars1, summary_df["trip_market_share_pct"], summary_df["sub2_strict_clean_trips"]):
        axes[0].text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + max(trips_m) * 0.02,
            f"{exact/1e6:.2f}M\n({share:.2f}%)",
            ha="center",
            va="bottom",
            fontsize=9.5,
            fontweight="bold",
        )

    # Right panel: Total Active Ride Hours (Millions of Hours)
    hours_m = summary_df["total_duration_hours"] / 1e6
    bars2 = axes[1].bar(labels, hours_m, color=colors, edgecolor="#222222", width=0.6)
    axes[1].set_title("Total Passenger Ride Duration in H1 2024 (Million Hours)", fontsize=12, fontweight="bold")
    axes[1].set_ylabel("Ride Hours (Millions)", fontsize=11)
    axes[1].set_ylim(0, max(hours_m) * 1.18)
    for bar, share, exact in zip(bars2, summary_df["duration_market_share_pct"], summary_df["total_duration_hours"]):
        axes[1].text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + max(hours_m) * 0.02,
            f"{exact/1e6:.2f}M hrs\n({share:.2f}%)",
            ha="center",
            va="bottom",
            fontsize=9.5,
            fontweight="bold",
        )

    fig.suptitle(
        "NYC Taxi Usage Comparison — First Half of 2024 (Jan–Jun 2024)",
        fontsize=14,
        fontweight="bold",
        y=1.02,
    )
    plt.tight_layout()
    fig.savefig(FIGURES_DIR / "fig1_most_used_taxi_h1_2024.png", bbox_inches="tight")
    plt.close(fig)


def _plot_monthly_trends(monthly_df: pd.DataFrame) -> None:
    """Figure 2: แนวโน้มจำนวนเที่ยวรายเดือน (ม.ค. - มิ.ย. 2024) แยกตามประเภทแท็กซี่"""
    fig, ax = plt.subplots(figsize=(10.5, 5.5), dpi=160)
    palette = {"fhvhv": "#1f77b4", "yellow": "#d4ac0d", "fhv": "#5d6d7e", "green": "#27ae60"}
    markers = {"fhvhv": "o", "yellow": "s", "fhv": "^", "green": "D"}

    for taxi_type in ["fhvhv", "yellow", "fhv", "green"]:
        sub = monthly_df[monthly_df["taxi_type"] == taxi_type].sort_values("year_month")
        ax.plot(
            sub["year_month"],
            sub["strict_clean_rows"] / 1e6,
            marker=markers[taxi_type],
            linewidth=2.5,
            markersize=7,
            color=palette[taxi_type],
            label=f"{taxi_type.upper()} (Total: {sub['strict_clean_rows'].sum()/1e6:.2f}M)",
        )

    ax.set_title("Monthly Clean Trip Volume by Taxi Type (Jan – Jun 2024)", fontsize=13, fontweight="bold")
    ax.set_xlabel("Year-Month", fontsize=11)
    ax.set_ylabel("Clean Trips (Millions)", fontsize=11)
    ax.legend(frameon=True, fontsize=10, loc="center right")
    ax.grid(True, linestyle="--", alpha=0.5)
    plt.tight_layout()
    fig.savefig(FIGURES_DIR / "fig2_monthly_trend_h1_2024.png", bbox_inches="tight")
    plt.close(fig)


def _plot_dq_anomalies(dq_df: pd.DataFrame) -> None:
    """Figure 3: สรุปจำนวนแถวที่ถูกคัดออกจากการตรวจสอบคุณภาพข้อมูล (Data Quality Gate) แยกรายสาเหตุ"""
    grouped = (
        dq_df.groupby("taxi_type")[
            [
                "dropped_null_timestamp",
                "dropped_negative_duration",
                "dropped_negative_dist_or_fare",
                "dropped_out_of_period",
                "dropped_extreme_duration",
            ]
        ]
        .sum()
        .reindex(["yellow", "fhvhv", "green", "fhv"])
    )

    fig, ax = plt.subplots(figsize=(11, 5.5), dpi=160)
    categories = [
        ("dropped_null_timestamp", "Null Timestamps (Sub 1+2)", "#95a5a6"),
        ("dropped_negative_duration", "Dropoff < Pickup (Sub 1+2)", "#e74c3c"),
        ("dropped_negative_dist_or_fare", "Negative/Null Distance or Fare (Sub 1+2)", "#f39c12"),
        ("dropped_out_of_period", "Out of H1-2024 Date Range (New in Sub 2)", "#8e44ad"),
        ("dropped_extreme_duration", "Extreme Duration > 24h (New in Sub 2)", "#2980b9"),
    ]

    x = np.arange(len(grouped.index))
    bottom = np.zeros(len(grouped.index))
    for col, label, color in categories:
        vals = grouped[col].values
        ax.bar(x, vals, bottom=bottom, label=label, color=color, width=0.55, edgecolor="#222222")
        bottom += vals

    ax.set_xticks(x)
    ax.set_xticklabels([t.upper() for t in grouped.index], fontsize=11, fontweight="bold")
    ax.set_ylabel("Dropped Rows (Count)", fontsize=11)
    ax.set_title("Data Quality Gate: Anomalous Rows Filtered Out by Taxi Type & Rule", fontsize=13, fontweight="bold")
    ax.yaxis.set_major_formatter(mticker.StrMethodFormatter("{x:,.0f}"))

    for idx, total_val in enumerate(bottom):
        ax.text(
            idx,
            total_val + max(bottom) * 0.02,
            f"{int(total_val):,}",
            ha="center",
            va="bottom",
            fontsize=10,
            fontweight="bold",
        )

    ax.set_ylim(0, max(bottom) * 1.15)
    ax.legend(frameon=True, fontsize=9.5)
    ax.grid(True, axis="y", linestyle="--", alpha=0.5)
    plt.tight_layout()
    fig.savefig(FIGURES_DIR / "fig3_data_quality_breakdown.png", bbox_inches="tight")
    plt.close(fig)


def _plot_borough_distribution(borough_df: pd.DataFrame) -> None:
    """Figure 4: สัดส่วนพื้นที่ขึ้นรถ (Pickup Borough) จากการบูรณาการกับไฟล์ taxi_zone_lookup.csv"""
    piv = (
        borough_df.groupby(["taxi_type", "pickup_borough"])["clean_trips"]
        .sum()
        .unstack(fill_value=0)
    )
    # Keep major boroughs + group others
    main_boroughs = ["Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island", "EWR"]
    existing_main = [b for b in main_boroughs if b in piv.columns]
    other_cols = [c for c in piv.columns if c not in existing_main]
    if other_cols:
        piv["Unknown/Other"] = piv[other_cols].sum(axis=1)
        cols_to_plot = existing_main + ["Unknown/Other"]
    else:
        cols_to_plot = existing_main

    piv = piv[cols_to_plot].reindex(["fhvhv", "yellow", "fhv", "green"])
    piv_pct = piv.div(piv.sum(axis=1), axis=0) * 100.0

    fig, ax = plt.subplots(figsize=(11, 5.5), dpi=160)
    b_colors = {
        "Manhattan": "#2c3e50",
        "Brooklyn": "#2980b9",
        "Queens": "#27ae60",
        "Bronx": "#e67e22",
        "Staten Island": "#8e44ad",
        "EWR": "#c0392b",
        "Unknown/Other": "#bdc3c7",
    }

    bottom = np.zeros(len(piv_pct.index))
    x = np.arange(len(piv_pct.index))
    for col in cols_to_plot:
        vals = piv_pct[col].values
        ax.bar(
            x,
            vals,
            bottom=bottom,
            label=col,
            color=b_colors.get(col, "#7f8c8d"),
            width=0.55,
            edgecolor="white",
        )
        bottom += vals

    ax.set_xticks(x)
    ax.set_xticklabels([t.upper() for t in piv_pct.index], fontsize=11, fontweight="bold")
    ax.set_ylabel("Share of Clean Trips (%)", fontsize=11)
    ax.set_ylim(0, 100)
    ax.set_title(
        "Pickup Borough Distribution by Taxi Type (Enriched via taxi_zone_lookup.csv)",
        fontsize=13,
        fontweight="bold",
    )
    ax.legend(bbox_to_anchor=(1.01, 1), loc="upper left", frameon=True, title="NYC Borough")
    plt.tight_layout()
    fig.savefig(FIGURES_DIR / "fig4_borough_distribution.png", bbox_inches="tight")
    plt.close(fig)


if __name__ == "__main__":
    run_analytics()
