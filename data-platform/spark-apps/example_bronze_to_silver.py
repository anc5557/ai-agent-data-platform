#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bronze to Silver 변환 예시

브론즈 레이어의 원천 데이터를 읽어 스키마 정의에 따라 실버 테이블로 변환하는 스크립트

실버 테이블:
1. silver_usage - 사용량 데이터
2. silver_contacts - 담당자 정보  
3. silver_contracts - 계약 정보
"""

from __future__ import annotations

import argparse
import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.client import Config
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import (
    col, regexp_replace, when, lit, to_date,
    trim, upper, coalesce, row_number, desc,
    max as spark_max
)
from pyspark.sql.types import StringType, DateType


# -----------------------------------------------------------------------------
# 환경 감지 및 설정
# -----------------------------------------------------------------------------
def is_running_in_docker() -> bool:
    """컨테이너 환경(Docker/K8s) 실행 여부 감지"""
    if os.path.exists("/.dockerenv"):
        return True
    return os.getenv("RUNNING_IN_DOCKER", "").lower() in ("1", "true", "yes")


def get_default_s3_endpoint() -> str:
    """실행 환경에 맞춘 기본 S3(MinIO) 엔드포인트 결정"""
    return "http://minio:9000" if is_running_in_docker() else "http://localhost:9000"


def create_spark_session(app_name: str) -> SparkSession:
    """Spark 세션 생성 - Submit에서 넘긴 conf/env 사용"""
    builder = SparkSession.builder.appName(app_name)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("INFO")
    return spark


def setup_s3_client(target_bucket: str | None = None):
    """S3 클라이언트 설정"""
    s3_endpoint = os.getenv("S3_ENDPOINT") or get_default_s3_endpoint()
    access_key = os.getenv("MINIO_ROOT_USER", "admin")
    secret_key = os.getenv("MINIO_ROOT_PASSWORD", "password")

    s3_client = boto3.client(
        "s3",
        endpoint_url=s3_endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )
    return s3_client


# -----------------------------------------------------------------------------
# 데이터 정제 함수
# -----------------------------------------------------------------------------
def safe_count(df: DataFrame, label: str = "") -> Optional[int]:
    """안전한 count() 래퍼: 실패 시 None 반환"""
    try:
        return df.count()
    except Exception as e:
        print(f"  ⚠️ [{label}] 건수 계산 실패: {e}")
        return None


def clean_usage_data(spark: SparkSession, df: DataFrame) -> DataFrame:
    """사용량 데이터 정제 -> silver_usage"""
    print("🔧 사용량 데이터 정제 시작")
    print(f"  📊 원본 데이터: {safe_count(df, 'usage')}행")

    # 스키마 정의에 따른 컬럼 매핑 및 변환
    df_cleaned = df.select([
        # 날짜 변환: '25. 07. 31' -> '2025-07'
        regexp_replace(
            regexp_replace(col("데이터_추출일자"), r"^(\d{2})\.\s*(\d{1,2})\.\s*\d{1,2}$", r"20$1-$2"),
            r"-(\d)$", r"-0$1"
        ).cast(StringType()).alias("usage_month"),

        # 도메인명 정제
        trim(col("도메인명")).cast(StringType()).alias("domain"),

        # 숫자 필드 정제: 쉼표 제거 후 타입 변환
        regexp_replace(col("메일개수"), ",", "").cast("int").alias("mail_count"),
        regexp_replace(col("용량_Bytes"), ",", "").cast("long").alias("usage_bytes"),
        regexp_replace(col("사용자수_active"), ",", "").cast("int").alias("active_users"),
    ])

    # Null 값 및 빈 문자열 필터링
    df_cleaned = df_cleaned.filter(
        col("domain").isNotNull() &
        (col("domain") != "") &
        col("usage_month").isNotNull()
    )

    # 월×도메인 기준 중복 제거 (max로 정규화)
    df_cleaned = (
        df_cleaned
        .groupBy("usage_month", "domain")
        .agg(
            spark_max("mail_count").alias("mail_count"),
            spark_max("usage_bytes").alias("usage_bytes"),
            spark_max("active_users").alias("active_users"),
        )
    )

    print(f"  ✅ 정제 완료: {safe_count(df_cleaned, 'usage')}행")
    return df_cleaned


def clean_contracts_data(spark: SparkSession, df: DataFrame) -> DataFrame:
    """계약 정보 정제 -> silver_contracts"""
    print("🔧 계약 정보 정제 시작")
    print(f"  📊 원본 데이터: {safe_count(df, 'contracts')}행")

    # 컬럼명에서 줄바꿈 문자 제거
    for old_col in df.columns:
        new_col = old_col.replace('\n', '').replace('\r', '')
        if old_col != new_col:
            df = df.withColumnRenamed(old_col, new_col)

    df_cleaned = df.select([
        # ID 처리
        when(col("번호").rlike(r"^\d+$"), col("번호").cast("int")).alias("id"),

        # 계약 상태 결정
        when(col("번호") == "종료", "TERMINATED")
        .when(col("번호") == "예정", "SCHEDULED")
        .otherwise("ACTIVE").alias("contract_status"),

        # 기본 정보
        trim(col("고객명")).cast(StringType()).alias("customer_name"),
        trim(col("도메인명")).cast(StringType()).alias("domain"),
        trim(col("공공분류")).cast(StringType()).alias("public_sector_type"),
        trim(col("CSP")).cast(StringType()).alias("csp"),

        # 용량 및 사용자 수
        regexp_replace(coalesce(col("계약_용량_GB"), lit("0")), ",", "").cast("int").alias("contract_gb"),
        regexp_replace(coalesce(col("계약_USER"), lit("0")), ",", "").cast("int").alias("contract_users"),

        # 날짜 필드 정제
        to_date(
            regexp_replace(coalesce(col("최초_오픈일"), lit("")), r"(\d{2})\.\s*(\d{1,2})\.\s*(\d{1,2})", r"20$1-$2-$3"),
            "yyyy-M-d"
        ).alias("open_date"),
    ])

    # 유효한 계약만 필터링
    df_cleaned = df_cleaned.filter(
        col("domain").isNotNull() &
        (col("domain") != "") &
        col("customer_name").isNotNull() &
        (col("customer_name") != "")
    )

    print(f"  ✅ 정제 완료: {safe_count(df_cleaned, 'contracts')}행")
    return df_cleaned


# -----------------------------------------------------------------------------
# Iceberg 저장
# -----------------------------------------------------------------------------
def save_to_silver(
    spark: SparkSession,
    df: DataFrame,
    table_name: str,
    silver_bucket: str,
) -> None:
    """실버 테이블을 Iceberg 테이블로 저장"""
    print(f"💾 실버 테이블 저장: {table_name}")

    namespace = "silver"
    target_table = f"iceberg.{namespace}.{table_name}"

    # 네임스페이스 생성
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS iceberg.{namespace}")

    # 테이블별 파티셔닝 전략
    if table_name == "silver_usage":
        partition_cols = ["usage_month"]
    else:
        # ingest_date 파티셔닝
        current_date_str = datetime.now().strftime("%Y-%m-%d")
        df = df.withColumn("ingest_date", to_date(lit(current_date_str)))
        partition_cols = ["ingest_date"]

    # 테이블 존재 여부에 따라 생성/추가
    if spark.catalog.tableExists(target_table):
        print("  🔄 기존 테이블에 파티션 덮어쓰기")
        df.writeTo(target_table).overwritePartitions()
    else:
        print("  🆕 새 테이블 생성")
        writer = df.writeTo(target_table)
        for col_name in partition_cols:
            writer = writer.partitionedBy(col_name)
        writer.create()

    print(f"  ✅ 저장 완료: {table_name}")


# -----------------------------------------------------------------------------
# 메인 실행
# -----------------------------------------------------------------------------
def run_bronze_to_silver_job(
    bronze_bucket: str,
    silver_bucket: str,
    dataset: str = "example",
) -> None:
    """브론즈 to 실버 변환 메인 작업"""
    print("=" * 60)
    print("🚀 Bronze to Silver 변환 시작")
    print(f"  - Bronze 버킷: {bronze_bucket}")
    print(f"  - Silver 버킷: {silver_bucket}")
    print(f"  - 데이터셋: {dataset}")
    print("=" * 60)

    spark = create_spark_session(app_name="Bronze-to-Silver")

    # 브론즈 테이블에서 데이터 읽기
    usage_df = spark.table(f"iceberg.bronze.{dataset}_usage")
    contracts_df = spark.table(f"iceberg.bronze.{dataset}_contracts")

    # 데이터 정제
    silver_usage = clean_usage_data(spark, usage_df)
    silver_contracts = clean_contracts_data(spark, contracts_df)

    # 실버 테이블 저장
    save_to_silver(spark, silver_usage, f"{dataset}_silver_usage", silver_bucket)
    save_to_silver(spark, silver_contracts, f"{dataset}_silver_contracts", silver_bucket)

    print("=" * 60)
    print("✅ Bronze to Silver 변환 완료")
    print("=" * 60)
    spark.stop()


def parse_args():
    parser = argparse.ArgumentParser(description="Bronze to Silver ETL")
    parser.add_argument("--bronze-bucket", required=True, help="Bronze data bucket")
    parser.add_argument("--silver-bucket", required=True, help="Silver data bucket")
    parser.add_argument("--dataset", default="example", help="Dataset name")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_bronze_to_silver_job(
        args.bronze_bucket,
        args.silver_bucket,
        args.dataset,
    )
