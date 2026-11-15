"""
NYC Taxi H1 2024 Reproducible Data Pipeline (CS341 Submission 2)
================================================================
พัฒนางานจาก Submission 1 (Data Lifecycle Baseline) ให้เป็น Data Pipeline ที่:
1. เชื่อมและประมวลผลข้อมูลจากหลายแหล่ง/หลายรูปแบบ (4 Parquet Schemas + 1 CSV Taxi Zone Lookup)
2. ตรวจสอบคุณภาพข้อมูล (Data Quality Gate) แยกรายสาเหตุ พร้อมเปรียบเทียบกับ Baseline เดิม
3. สร้างชุดข้อมูลพร้อมใช้ (Curated Output) เก็บลง Storage เพื่อนำไปวิเคราะห์ต่อโดยไม่ต้องอ่าน Raw ซ้ำ
4. รองรับการรันซ้ำ (Idempotent) และอัปเดตเมื่อมีข้อมูลใหม่ (Incremental Update) ผ่าน State Manifest
5. บันทึกหลักฐานการทำงาน (Execution Log & Data Quality Report) ทุกขั้นตอน
"""

from __future__ import annotations

import argparse
import concurrent.futures
import datetime as dt
import json
import logging
import os
import time
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Tuple

import duckdb
import pandas as pd

from src.config import (
    COLAB_DRIVE_PATH,
    CURATED_BOROUGH_CSV,
    CURATED_BOROUGH_PARQUET,
    CURATED_BOROUGH_PARTS_DIR,
    CURATED_DIR,
    CURATED_MONTHLY_CSV,
    CURATED_MONTHLY_PARQUET,
    CURATED_SAMPLE_PARTS_DIR,
    DQ_MAX_DATETIME,
    DQ_MAX_DURATION_MINUTES,
    DQ_MIN_DATETIME,
    DQ_REPORT_CSV_PATH,
    DQ_SUMMARY_JSON_PATH,
    OUTPUTS_DIR,
    PIPELINE_LOG_PATH,
    PIPELINE_RULE_VERSION,
    PROJECT_ROOT,
    RAW_DIR,
    REFERENCE_DIR,
    SAMPLE_CURATED_TRIPS_CSV,
    SAMPLE_CURATED_TRIPS_PARQUET,
    SCHEMA_MAPPING,
    STATE_MANIFEST_PATH,
    TARGET_MONTHS,
    TAXI_TYPES,
    TLC_TRIP_BASE_URL,
    TLC_ZONE_LOOKUP_URL,
    ZONE_LOOKUP_PATH,
)


def setup_logger() -> logging.Logger:
    """ตั้งค่า Logger สำหรับแสดงผลทางหน้าจอและบันทึกลงไฟล์ outputs/pipeline_run.log"""
    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("NYCTaxiPipeline")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-7s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # File Handler (append mode เพื่อเก็บประวัติการรันทั้งรอบแรกและรอบรันซ้ำ)
    fh = logging.FileHandler(PIPELINE_LOG_PATH, mode="a", encoding="utf-8")
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    # Console Handler
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    return logger


def ensure_directories() -> None:
    """สร้างโฟลเดอร์สำหรับเก็บข้อมูลแต่ละชั้น (Reference, Curated, Partitions, Outputs)"""
    for d in [
        RAW_DIR,
        REFERENCE_DIR,
        CURATED_DIR,
        CURATED_BOROUGH_PARTS_DIR,
        CURATED_SAMPLE_PARTS_DIR,
        OUTPUTS_DIR,
    ]:
        d.mkdir(parents=True, exist_ok=True)


def ensure_zone_lookup(logger: logging.Logger) -> pd.DataFrame:
    """
    ตรวจสอบและดาวน์โหลดไฟล์อ้างอิง taxi_zone_lookup.csv (Multi-source / Multi-format integration)
    """
    if not ZONE_LOOKUP_PATH.exists():
        logger.info(f"[INGEST-REF] Downloading reference CSV from {TLC_ZONE_LOOKUP_URL}")
        urllib.request.urlretrieve(TLC_ZONE_LOOKUP_URL, ZONE_LOOKUP_PATH)
    else:
        logger.info(f"[INGEST-REF] Using cached reference CSV at {ZONE_LOOKUP_PATH}")

    zone_df = pd.read_csv(ZONE_LOOKUP_PATH)
    required_cols = {"LocationID", "Borough", "Zone", "service_zone"}
    missing = required_cols - set(zone_df.columns)
    if missing:
        raise ValueError(f"Reference CSV missing required columns: {missing}")

    logger.info(
        f"[VALIDATED-REF] taxi_zone_lookup.csv loaded successfully ({len(zone_df):,} zones)."
    )
    return zone_df


def resolve_source_uri(taxi_type: str, year_month: str) -> Tuple[str, str]:
    """
    ค้นหาแหล่งข้อมูลตามลำดับความสำคัญ (Hybrid Source Resolution):
    1. โฟลเดอร์ Local (data/raw/)
    2. โฟลเดอร์ Google Drive (กรณีรันบน Google Colab ต่อจาก Submission 1)
    3. Official NYC TLC CloudFront URL (อ่านผ่าน HTTP Range Request โดยตรง)
    """
    file_name = f"{taxi_type}_tripdata_{year_month}.parquet"

    # 1. Check local raw folder
    local_candidate = RAW_DIR / file_name
    if local_candidate.exists():
        return str(local_candidate), "local_file"

    # 2. Check Google Colab Drive folder (if mounted)
    if COLAB_DRIVE_PATH.exists():
        for root, _, files in os.walk(COLAB_DRIVE_PATH):
            if file_name in files:
                return str(Path(root) / file_name), "google_drive"

    # 3. Fallback to official NYC TLC CloudFront CDN
    remote_url = f"{TLC_TRIP_BASE_URL}/{file_name}"
    return remote_url, "nyc_tlc_cdn"


def load_state() -> Dict[str, Any]:
    """โหลดสถานะการทำงานของ Pipeline จากไฟล์ outputs/pipeline_state.json"""
    if STATE_MANIFEST_PATH.exists():
        try:
            with open(STATE_MANIFEST_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {
        "pipeline_name": "NYC_Taxi_H1_2024_Reproducible_Pipeline",
        "rule_version": PIPELINE_RULE_VERSION,
        "last_updated": None,
        "partitions": {},
    }


def save_state(state: Dict[str, Any]) -> None:
    """บันทึกสถานะการทำงานของ Pipeline ลงไฟล์ outputs/pipeline_state.json"""
    state["last_updated"] = dt.datetime.now(dt.timezone.utc).isoformat()
    with open(STATE_MANIFEST_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, ensure_ascii=False)


def process_single_partition(
    taxi_type: str,
    year_month: str,
    source_uri: str,
    source_kind: str,
    zone_csv_path: str,
) -> Dict[str, Any]:
    """
    ประมวลผลข้อมูล 1 Partition (1 ประเภทแท็กซี่ x 1 เดือน):
    - ตรวจสอบ Schema ของไฟล์ Parquet
    - แปลงชื่อคอลัมน์ให้เป็นมาตรฐานเดียวกัน (Schema Harmonization)
    - ตรวจสอบคุณภาพข้อมูล (Data Quality Gate) ทั้งเกณฑ์ Baseline (Sub 1) และ Strict (Sub 2)
    - เชื่อมข้อมูลกับตารางโซน (taxi_zone_lookup.csv) เพื่อสรุปยอดระดับ Borough และดึง Sample Trips
    """
    t0 = time.time()
    spec = SCHEMA_MAPPING[taxi_type]
    p_col = spec["pickup_col"]
    d_col = spec["dropoff_col"]
    pu_col = spec["pu_loc_col"]
    do_col = spec["do_loc_col"]
    dist_col = spec["distance_col"]
    fare_col = spec["fare_col"]
    has_df = spec["has_distance_fare"]

    con = duckdb.connect()
    try:
        # 1. Schema Validation: ตรวจสอบว่ามีคอลัมน์ที่จำเป็นครบหรือไม่
        schema_df = con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{source_uri}')"
        ).df()
        actual_cols = set(schema_df["column_name"].tolist())
        expected_cols = {p_col, d_col, pu_col, do_col}
        if has_df:
            expected_cols.update({dist_col, fare_col})

        missing_cols = expected_cols - actual_cols
        if missing_cols:
            raise ValueError(
                f"Schema validation failed for {taxi_type}_{year_month}: missing {missing_cols}"
            )

        # 2. สร้างนิพจน์ SQL สำหรับแปลง Schema ให้เป็นมาตรฐานเดียวกัน (Unified Schema)
        dist_expr = f"CAST({dist_col} AS DOUBLE)" if has_df else "NULL::DOUBLE"
        fare_expr = f"CAST({fare_col} AS DOUBLE)" if has_df else "NULL::DOUBLE"
        dist_fare_valid_cond = (
            f"({dist_col} IS NOT NULL AND {dist_col} >= 0 AND {fare_col} IS NOT NULL AND {fare_col} >= 0)"
            if has_df
            else "TRUE"
        )
        dist_fare_invalid_cond = (
            f"({dist_col} IS NULL OR {dist_col} < 0 OR {fare_col} IS NULL OR {fare_col} < 0)"
            if has_df
            else "FALSE"
        )

        # โหลดเฉพาะคอลัมน์ที่ต้องใช้ (Columnar Projection Pushdown) ครั้งเดียวเข้า Temp Table
        con.execute(
            f"""
            CREATE TEMP TABLE harmonized AS
            SELECT
                CAST({p_col} AS TIMESTAMP) AS pickup_datetime,
                CAST({d_col} AS TIMESTAMP) AS dropoff_datetime,
                CAST({pu_col} AS INTEGER) AS pu_location_id,
                CAST({do_col} AS INTEGER) AS do_location_id,
                {dist_expr} AS trip_distance_miles,
                {fare_expr} AS fare_amount_usd,
                epoch(CAST({d_col} AS TIMESTAMP) - CAST({p_col} AS TIMESTAMP)) / 60.0 AS duration_minutes,
                {dist_fare_valid_cond} AS is_dist_fare_valid,
                {dist_fare_invalid_cond} AS is_dist_fare_invalid
            FROM read_parquet('{source_uri}')
            """
        )

        # 3. ประมวลผล Data Quality Metrics + Aggregated Monthly Metrics ในครั้งเดียว (Single-Pass Scan)
        query = f"""
        WITH flagged AS (
            SELECT
                *,
                (pickup_datetime IS NULL OR dropoff_datetime IS NULL) AS is_null_ts,
                (pickup_datetime IS NOT NULL AND dropoff_datetime IS NOT NULL AND dropoff_datetime < pickup_datetime) AS is_neg_duration,
                (pickup_datetime IS NOT NULL AND dropoff_datetime IS NOT NULL AND dropoff_datetime >= pickup_datetime AND is_dist_fare_invalid) AS is_bad_dist_fare,
                (pickup_datetime IS NOT NULL AND dropoff_datetime IS NOT NULL AND dropoff_datetime >= pickup_datetime AND is_dist_fare_valid) AS is_baseline_clean,
                (
                    pickup_datetime IS NOT NULL
                    AND dropoff_datetime IS NOT NULL
                    AND dropoff_datetime >= pickup_datetime
                    AND is_dist_fare_valid
                    AND (pickup_datetime < TIMESTAMP '{DQ_MIN_DATETIME}' OR pickup_datetime >= TIMESTAMP '{DQ_MAX_DATETIME}')
                ) AS is_out_of_period,
                (
                    pickup_datetime IS NOT NULL
                    AND dropoff_datetime IS NOT NULL
                    AND dropoff_datetime >= pickup_datetime
                    AND is_dist_fare_valid
                    AND pickup_datetime >= TIMESTAMP '{DQ_MIN_DATETIME}'
                    AND pickup_datetime < TIMESTAMP '{DQ_MAX_DATETIME}'
                    AND duration_minutes > {DQ_MAX_DURATION_MINUTES}
                ) AS is_extreme_duration,
                (
                    pickup_datetime IS NOT NULL
                    AND dropoff_datetime IS NOT NULL
                    AND dropoff_datetime >= pickup_datetime
                    AND is_dist_fare_valid
                    AND pickup_datetime >= TIMESTAMP '{DQ_MIN_DATETIME}'
                    AND pickup_datetime < TIMESTAMP '{DQ_MAX_DATETIME}'
                    AND duration_minutes <= {DQ_MAX_DURATION_MINUTES}
                ) AS is_strict_clean
            FROM harmonized
        )
        SELECT
            COUNT(*) AS raw_rows,
            COUNT(*) FILTER (WHERE is_null_ts) AS dropped_null_timestamp,
            COUNT(*) FILTER (WHERE is_neg_duration) AS dropped_negative_duration,
            COUNT(*) FILTER (WHERE is_bad_dist_fare) AS dropped_negative_dist_or_fare,
            COUNT(*) FILTER (WHERE is_baseline_clean) AS baseline_clean_rows,
            COUNT(*) FILTER (WHERE is_out_of_period) AS dropped_out_of_period,
            COUNT(*) FILTER (WHERE is_extreme_duration) AS dropped_extreme_duration,
            COUNT(*) FILTER (WHERE is_strict_clean) AS strict_clean_rows,
            COALESCE(SUM(duration_minutes) FILTER (WHERE is_strict_clean) / 60.0, 0.0) AS total_duration_hours,
            COALESCE(AVG(duration_minutes) FILTER (WHERE is_strict_clean), 0.0) AS avg_duration_minutes,
            SUM(trip_distance_miles) FILTER (WHERE is_strict_clean) AS total_distance_miles,
            AVG(trip_distance_miles) FILTER (WHERE is_strict_clean) AS avg_distance_miles,
            SUM(fare_amount_usd) FILTER (WHERE is_strict_clean) AS total_fare_usd,
            AVG(fare_amount_usd) FILTER (WHERE is_strict_clean) AS avg_fare_usd
        FROM flagged
        """
        metrics_row = con.execute(query).df().iloc[0].to_dict()

        # 4. บูรณาการกับตารางโซน (taxi_zone_lookup.csv) เพื่อสรุปยอดระดับ Borough
        borough_query = f"""
        WITH clean_trips AS (
            SELECT
                pu_location_id,
                duration_minutes,
                trip_distance_miles,
                fare_amount_usd
            FROM harmonized
            WHERE pickup_datetime IS NOT NULL
              AND dropoff_datetime IS NOT NULL
              AND dropoff_datetime >= pickup_datetime
              AND is_dist_fare_valid
              AND pickup_datetime >= TIMESTAMP '{DQ_MIN_DATETIME}'
              AND pickup_datetime < TIMESTAMP '{DQ_MAX_DATETIME}'
              AND duration_minutes <= {DQ_MAX_DURATION_MINUTES}
        ),
        zones AS (
            SELECT LocationID, Borough, Zone, service_zone
            FROM read_csv_auto('{zone_csv_path}')
        )
        SELECT
            '{taxi_type}' AS taxi_type,
            '{year_month}' AS year_month,
            COALESCE(z.Borough, 'Unknown') AS pickup_borough,
            COUNT(*) AS clean_trips,
            ROUND(COALESCE(SUM(c.duration_minutes) / 60.0, 0.0), 2) AS total_duration_hours,
            ROUND(COALESCE(AVG(c.duration_minutes), 0.0), 2) AS avg_duration_minutes,
            ROUND(SUM(c.trip_distance_miles), 2) AS total_distance_miles,
            ROUND(SUM(c.fare_amount_usd), 2) AS total_fare_usd
        FROM clean_trips c
        LEFT JOIN zones z ON c.pu_location_id = z.LocationID
        GROUP BY 1, 2, 3
        ORDER BY clean_trips DESC
        """
        borough_df = con.execute(borough_query).df()

        # 5. ดึงตัวอย่างข้อมูลระดับแถวที่ผ่านการทำความสะอาดและเชื่อมชื่อโซนแล้ว (Sample Curated Trips: 50 แถวต่อไฟล์)
        sample_query = f"""
        WITH zones AS (
            SELECT LocationID, Borough, Zone
            FROM read_csv_auto('{zone_csv_path}')
        )
        SELECT
            '{taxi_type}' AS taxi_type,
            '{year_month}' AS year_month,
            t.pickup_datetime,
            t.dropoff_datetime,
            ROUND(t.duration_minutes, 2) AS trip_duration_minutes,
            t.pu_location_id,
            COALESCE(z_pu.Borough, 'Unknown') AS pickup_borough,
            COALESCE(z_pu.Zone, 'Unknown') AS pickup_zone,
            t.do_location_id,
            COALESCE(z_do.Borough, 'Unknown') AS dropoff_borough,
            ROUND(t.trip_distance_miles, 2) AS trip_distance_miles,
            ROUND(t.fare_amount_usd, 2) AS fare_amount_usd
        FROM harmonized t
        LEFT JOIN zones z_pu ON t.pu_location_id = z_pu.LocationID
        LEFT JOIN zones z_do ON t.do_location_id = z_do.LocationID
        WHERE t.pickup_datetime IS NOT NULL
          AND t.dropoff_datetime IS NOT NULL
          AND t.dropoff_datetime >= t.pickup_datetime
          AND t.is_dist_fare_valid
          AND t.pickup_datetime >= TIMESTAMP '{DQ_MIN_DATETIME}'
          AND t.pickup_datetime < TIMESTAMP '{DQ_MAX_DATETIME}'
          AND t.duration_minutes <= {DQ_MAX_DURATION_MINUTES}
        LIMIT 50
        """
        sample_df = con.execute(sample_query).df()
        for dt_col in ["pickup_datetime", "dropoff_datetime"]:
            sample_df[dt_col] = sample_df[dt_col].astype(str)

    finally:
        con.close()

    elapsed_sec = round(time.time() - t0, 2)
    top_borough = (
        borough_df.iloc[0]["pickup_borough"] if not borough_df.empty else "Unknown"
    )

    raw_rows = int(metrics_row["raw_rows"])
    baseline_clean = int(metrics_row["baseline_clean_rows"])
    strict_clean = int(metrics_row["strict_clean_rows"])
    baseline_dropped = raw_rows - baseline_clean
    strict_dropped = raw_rows - strict_clean

    # 6. บันทึกผลลัพธ์ที่คลีนแล้วของไฟล์นี้ลง Curated Partition Storage ทีละไฟล์ (ตามแนวคิด #idea การแก้ไข)
    p_key = f"{taxi_type}_{year_month}"
    b_path = CURATED_BOROUGH_PARTS_DIR / f"{p_key}.parquet"
    s_path = CURATED_SAMPLE_PARTS_DIR / f"{p_key}.parquet"
    borough_df.to_parquet(b_path, index=False)
    sample_df.to_parquet(s_path, index=False)

    partition_result = {
        "partition_key": p_key,
        "file_name": f"{taxi_type}_tripdata_{year_month}.parquet",
        "taxi_type": taxi_type,
        "year_month": year_month,
        "source_uri": source_uri,
        "source_kind": source_kind,
        "status": "COMPLETED",
        "rule_version": PIPELINE_RULE_VERSION,
        "processed_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "elapsed_seconds": elapsed_sec,
        "schema_valid": True,
        "raw_rows": raw_rows,
        "dropped_null_timestamp": int(metrics_row["dropped_null_timestamp"]),
        "dropped_negative_duration": int(metrics_row["dropped_negative_duration"]),
        "dropped_negative_dist_or_fare": int(metrics_row["dropped_negative_dist_or_fare"]),
        "baseline_dropped_rows": baseline_dropped,
        "baseline_clean_rows": baseline_clean,
        "dropped_out_of_period": int(metrics_row["dropped_out_of_period"]),
        "dropped_extreme_duration": int(metrics_row["dropped_extreme_duration"]),
        "strict_dropped_rows": strict_dropped,
        "strict_clean_rows": strict_clean,
        "retention_rate_pct": round((strict_clean / raw_rows) * 100.0, 4) if raw_rows else 0.0,
        "total_duration_hours": round(float(metrics_row["total_duration_hours"]), 2),
        "avg_duration_minutes": round(float(metrics_row["avg_duration_minutes"]), 2),
        "total_distance_miles": (
            round(float(metrics_row["total_distance_miles"]), 2)
            if pd.notna(metrics_row["total_distance_miles"])
            else None
        ),
        "avg_distance_miles": (
            round(float(metrics_row["avg_distance_miles"]), 2)
            if pd.notna(metrics_row["avg_distance_miles"])
            else None
        ),
        "total_fare_usd": (
            round(float(metrics_row["total_fare_usd"]), 2)
            if pd.notna(metrics_row["total_fare_usd"])
            else None
        ),
        "avg_fare_usd": (
            round(float(metrics_row["avg_fare_usd"]), 2)
            if pd.notna(metrics_row["avg_fare_usd"])
            else None
        ),
        "top_pickup_borough": top_borough,
        "curated_borough_partition": str(b_path.relative_to(PROJECT_ROOT)),
        "curated_sample_partition": str(s_path.relative_to(PROJECT_ROOT)),
    }
    return partition_result


def materialize_curated_outputs(
    state: Dict[str, Any], logger: logging.Logger
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    นำไฟล์ Curated รายไฟล์ที่เก็บไว้ใน data/curated/partitions/ มารวมเป็นชุดข้อมูลพร้อมวิเคราะห์:
    1. data/curated/curated_monthly_summary.parquet & .csv
    2. data/curated/curated_borough_summary.parquet & .csv
    3. data/curated/sample_curated_trips.parquet & .csv
    4. outputs/data_quality_report.csv & outputs/data_quality_summary.json
    """
    partitions = list(state.get("partitions", {}).values())
    if not partitions:
        raise RuntimeError("No completed partitions found in state manifest.")

    # เรียงลำดับตามเดือนและประเภทแท็กซี่
    partitions.sort(key=lambda x: (x["year_month"], x["taxi_type"]))

    # 1. สร้าง Curated Monthly Summary DataFrame
    monthly_cols = [
        "partition_key",
        "year_month",
        "taxi_type",
        "raw_rows",
        "baseline_clean_rows",
        "strict_clean_rows",
        "strict_dropped_rows",
        "retention_rate_pct",
        "total_duration_hours",
        "avg_duration_minutes",
        "total_distance_miles",
        "avg_distance_miles",
        "total_fare_usd",
        "avg_fare_usd",
        "top_pickup_borough",
    ]
    monthly_df = pd.DataFrame([{k: p[k] for k in monthly_cols} for p in partitions])
    monthly_df.to_parquet(CURATED_MONTHLY_PARQUET, index=False)
    monthly_df.to_csv(CURATED_MONTHLY_CSV, index=False)
    logger.info(
        f"[CURATED] Saved monthly curated dataset ({len(monthly_df)} rows) -> {CURATED_MONTHLY_PARQUET}"
    )

    # 2. อ่านไฟล์ Curated รายไฟล์จาก data/curated/partitions/borough_by_file/ มารวมกัน
    borough_frames = []
    for p in partitions:
        b_file = PROJECT_ROOT / p["curated_borough_partition"]
        if b_file.exists():
            borough_frames.append(pd.read_parquet(b_file))
    borough_df = pd.concat(borough_frames, ignore_index=True) if borough_frames else pd.DataFrame()
    if not borough_df.empty:
        borough_df.to_parquet(CURATED_BOROUGH_PARQUET, index=False)
        borough_df.to_csv(CURATED_BOROUGH_CSV, index=False)
        logger.info(
            f"[CURATED] Aggregated 24 per-file partitions into zone-enriched borough dataset ({len(borough_df)} rows) -> {CURATED_BOROUGH_PARQUET}"
        )

    # 3. อ่านไฟล์ Sample Curated รายไฟล์จาก data/curated/partitions/sample_by_file/ มารวมกัน
    sample_frames = []
    for p in partitions:
        s_file = PROJECT_ROOT / p["curated_sample_partition"]
        if s_file.exists():
            sample_frames.append(pd.read_parquet(s_file))
    sample_df = pd.concat(sample_frames, ignore_index=True) if sample_frames else pd.DataFrame()
    if not sample_df.empty:
        sample_df.to_parquet(SAMPLE_CURATED_TRIPS_PARQUET, index=False)
        sample_df.to_csv(SAMPLE_CURATED_TRIPS_CSV, index=False)
        logger.info(
            f"[CURATED] Aggregated 24 per-file partitions into sample row-level curated trips ({len(sample_df)} rows) -> {SAMPLE_CURATED_TRIPS_CSV}"
        )

    # 4. สร้างรายงาน Data Quality Report (CSV & JSON)
    dq_cols = [
        "file_name",
        "taxi_type",
        "year_month",
        "schema_valid",
        "raw_rows",
        "dropped_null_timestamp",
        "dropped_negative_duration",
        "dropped_negative_dist_or_fare",
        "baseline_dropped_rows",
        "baseline_clean_rows",
        "dropped_out_of_period",
        "dropped_extreme_duration",
        "strict_dropped_rows",
        "strict_clean_rows",
        "retention_rate_pct",
        "elapsed_seconds",
    ]
    dq_df = pd.DataFrame([{k: p[k] for k in dq_cols} for p in partitions])
    dq_df.to_csv(DQ_REPORT_CSV_PATH, index=False)

    dq_summary = {
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "rule_version": PIPELINE_RULE_VERSION,
        "total_files_checked": len(dq_df),
        "all_schemas_valid": bool(dq_df["schema_valid"].all()),
        "total_raw_rows": int(dq_df["raw_rows"].sum()),
        "submission1_baseline_clean_rows": int(dq_df["baseline_clean_rows"].sum()),
        "submission1_baseline_dropped_rows": int(dq_df["baseline_dropped_rows"].sum()),
        "submission2_strict_clean_rows": int(dq_df["strict_clean_rows"].sum()),
        "submission2_strict_dropped_rows": int(dq_df["strict_dropped_rows"].sum()),
        "additional_anomalies_caught_in_sub2": int(
            dq_df["strict_dropped_rows"].sum() - dq_df["baseline_dropped_rows"].sum()
        ),
        "anomaly_breakdown_by_reason": {
            "null_timestamps": int(dq_df["dropped_null_timestamp"].sum()),
            "negative_duration_dropoff_lt_pickup": int(
                dq_df["dropped_negative_duration"].sum()
            ),
            "negative_or_null_distance_or_fare": int(
                dq_df["dropped_negative_dist_or_fare"].sum()
            ),
            "out_of_h1_2024_date_range": int(dq_df["dropped_out_of_period"].sum()),
            "extreme_duration_gt_24h": int(dq_df["dropped_extreme_duration"].sum()),
        },
    }
    with open(DQ_SUMMARY_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(dq_summary, f, indent=2, ensure_ascii=False)

    logger.info(
        f"[DQ-REPORT] Saved Data Quality report -> {DQ_REPORT_CSV_PATH} & {DQ_SUMMARY_JSON_PATH}"
    )
    return monthly_df, borough_df, dq_df


def run_pipeline(
    months: List[str] = TARGET_MONTHS,
    taxi_types: List[str] = TAXI_TYPES,
    force: bool = False,
    max_workers: int = 4,
) -> Dict[str, Any]:
    """
    ฟังก์ชันหลักสำหรับรัน Pipeline แบบ End-to-End:
    - ตรวจสอบ State Manifest เพื่อข้ามไฟล์ที่เคยประมวลผลไปแล้ว (Idempotent / Incremental)
    - ประมวลผลเฉพาะ Partition ที่ยังไม่ได้ทำ หรือเมื่อสั่ง --force
    - สร้าง Curated Outputs และรายงาน Data Quality
    """
    ensure_directories()
    logger = setup_logger()
    run_start = time.time()

    logger.info("=" * 75)
    logger.info(
        f"🚀 STARTING NYC TAXI DATA PIPELINE (rule_version={PIPELINE_RULE_VERSION}, force={force})"
    )
    logger.info("=" * 75)

    # Step 1: Load & validate reference CSV (taxi_zone_lookup.csv)
    ensure_zone_lookup(logger)

    # Step 2: Check state manifest for incremental / idempotent execution
    state = load_state()
    tasks_to_run: List[Tuple[str, str, str, str]] = []
    skipped_count = 0

    for ym in months:
        for t_type in taxi_types:
            p_key = f"{t_type}_{ym}"
            existing = state.get("partitions", {}).get(p_key)
            if (
                not force
                and existing
                and existing.get("status") == "COMPLETED"
                and existing.get("rule_version") == PIPELINE_RULE_VERSION
            ):
                skipped_count += 1
                logger.info(
                    f"[SKIP-IDEMPOTENT] {p_key:<15} | Already processed at {existing['processed_at']} "
                    f"(clean_rows={existing['strict_clean_rows']:,})"
                )
            else:
                uri, kind = resolve_source_uri(t_type, ym)
                tasks_to_run.append((t_type, ym, uri, kind))

    logger.info(
        f"[PLAN] Total partitions requested: {len(months) * len(taxi_types)} | "
        f"To process: {len(tasks_to_run)} | Skipped (cached): {skipped_count}"
    )

    # Step 3: Process pending partitions
    if tasks_to_run:
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_key = {
                executor.submit(
                    process_single_partition,
                    t_type,
                    ym,
                    uri,
                    kind,
                    str(ZONE_LOOKUP_PATH),
                ): f"{t_type}_{ym}"
                for (t_type, ym, uri, kind) in tasks_to_run
            }

            for future in concurrent.futures.as_completed(future_to_key):
                p_key = future_to_key[future]
                try:
                    res = future.result()
                    state.setdefault("partitions", {})[p_key] = res
                    save_state(state)
                    logger.info(
                        f"[PROCESSED] {p_key:<15} | raw={res['raw_rows']:>10,} | "
                        f"sub1_clean={res['baseline_clean_rows']:>10,} | "
                        f"sub2_clean={res['strict_clean_rows']:>10,} | "
                        f"dropped={res['strict_dropped_rows']:>6,} | "
                        f"time={res['elapsed_seconds']}s"
                    )
                except Exception as exc:
                    logger.error(f"[FAILED] Partition {p_key} raised error: {exc}")
                    raise

    # Step 4: Materialize Curated Datasets & Data Quality Reports
    monthly_df, _, dq_df = materialize_curated_outputs(state, logger)

    total_elapsed = round(time.time() - run_start, 2)
    logger.info("-" * 75)
    logger.info(
        f"✅ PIPELINE COMPLETED SUCCESSFULLY in {total_elapsed:.2f}s "
        f"(Processed: {len(tasks_to_run)}, Skipped: {skipped_count})"
    )
    logger.info(
        f"📊 Total Raw Rows: {dq_df['raw_rows'].sum():,} | "
        f"Sub1 Baseline Clean: {dq_df['baseline_clean_rows'].sum():,} | "
        f"Sub2 Strict Clean: {dq_df['strict_clean_rows'].sum():,}"
    )
    logger.info("=" * 75)
    return state


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the NYC Taxi H1 2024 Reproducible Data Pipeline (Submission 2)."
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-run all partitions from scratch even if already recorded in pipeline_state.json",
    )
    parser.add_argument(
        "--months",
        nargs="+",
        default=TARGET_MONTHS,
        help="Target year-months to process (default: 2024-01 .. 2024-06)",
    )
    parser.add_argument(
        "--taxi-types",
        nargs="+",
        default=TAXI_TYPES,
        help="Taxi types to process (default: yellow green fhv fhvhv)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Number of concurrent partition workers (default: 4)",
    )
    args = parser.parse_args()
    run_pipeline(
        months=args.months,
        taxi_types=args.taxi_types,
        force=args.force,
        max_workers=args.workers,
    )


if __name__ == "__main__":
    main()
