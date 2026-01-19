#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Silver to Gold 변환 예시

실버 레이어의 정제된 데이터를 읽어 비즈니스 지표 테이블(골드)로 변환하는 스크립트

골드 테이블:
1. gold_customer_master - 고객 마스터 (통합 고객 정보)
2. gold_usage_analytics - 사용량 분석 (월별 트렌드, 증감률)
3. gold_revenue_analytics - 매출 분석 (고객별/서비스별 매출)
"""

from __future__ import annotations

import argparse
import os
from datetime import datetime
from typing import Optional

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import (
    col, lit, to_date, coalesce,
    sum as spark_sum, avg as spark_avg, max as spark_max, min as spark_min,
    count, row_number, desc, dense_rank,
    when, lag, round as spark_round,
    concat_ws, collect_list
)
from pyspark.sql.types import StringType


# -----------------------------------------------------------------------------
# Spark 세션 설정
# -----------------------------------------------------------------------------
def create_spark_session(app_name: str) -> SparkSession:
    """Spark 세션 생성"""
    builder = SparkSession.builder.appName(app_name)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("INFO")
    return spark


# -----------------------------------------------------------------------------
# 골드 테이블 생성 함수
# -----------------------------------------------------------------------------
def create_customer_master(
    spark: SparkSession,
    contracts_df: DataFrame,
    contacts_df: DataFrame,
    usage_df: DataFrame,
) -> DataFrame:
    """고객 마스터 테이블 생성
    
    여러 소스를 통합하여 고객별 핵심 정보를 하나의 테이블로 구성
    """
    print("🏗️ 고객 마스터 테이블 생성")

    # 계약 정보에서 고객별 최신 계약 추출
    contract_window = Window.partitionBy("customer_name").orderBy(desc("contract_date"))
    latest_contracts = (
        contracts_df
        .withColumn("rn", row_number().over(contract_window))
        .filter(col("rn") == 1)
        .drop("rn")
        .select(
            col("customer_name"),
            col("domain"),
            col("contract_status"),
            col("csp"),
            col("public_sector_type"),
            col("contract_gb"),
            col("contract_users"),
            col("contract_date"),
            col("open_date"),
        )
    )

    # 담당자 정보 집계 (고객별 담당자 수)
    contact_counts = (
        contacts_df
        .groupBy("domain")
        .agg(
            count("*").alias("total_contacts"),
            spark_sum(when(col("is_current_contact") == True, 1).otherwise(0)).alias("active_contacts"),
        )
    )

    # 사용량 정보 집계 (최근 월 기준)
    usage_window = Window.partitionBy("domain").orderBy(desc("usage_month"))
    latest_usage = (
        usage_df
        .withColumn("rn", row_number().over(usage_window))
        .filter(col("rn") == 1)
        .drop("rn")
        .select(
            col("domain"),
            col("usage_month").alias("latest_usage_month"),
            col("usage_bytes").alias("latest_usage_bytes"),
            col("active_users").alias("latest_active_users"),
        )
    )

    # 조인하여 마스터 테이블 구성
    customer_master = (
        latest_contracts
        .join(contact_counts, "domain", "left")
        .join(latest_usage, "domain", "left")
        .select(
            col("customer_name"),
            col("domain"),
            col("contract_status"),
            col("csp"),
            col("public_sector_type"),
            col("contract_gb"),
            col("contract_users"),
            col("open_date"),
            col("contract_date"),
            coalesce(col("total_contacts"), lit(0)).alias("total_contacts"),
            coalesce(col("active_contacts"), lit(0)).alias("active_contacts"),
            col("latest_usage_month"),
            col("latest_usage_bytes"),
            col("latest_active_users"),
            lit(datetime.now().strftime("%Y-%m-%d")).alias("snapshot_date"),
        )
    )

    print(f"  ✅ 고객 마스터 생성 완료: {customer_master.count()}개 고객")
    return customer_master


def create_usage_analytics(
    spark: SparkSession,
    usage_df: DataFrame,
) -> DataFrame:
    """사용량 분석 테이블 생성
    
    월별 트렌드, 전월 대비 증감률 계산
    """
    print("📊 사용량 분석 테이블 생성")

    # 월별 윈도우 정의
    monthly_window = Window.partitionBy("domain").orderBy("usage_month")

    usage_analytics = (
        usage_df
        .withColumn("prev_usage_bytes", lag("usage_bytes", 1).over(monthly_window))
        .withColumn("prev_active_users", lag("active_users", 1).over(monthly_window))
        .withColumn(
            "usage_growth_rate",
            when(
                col("prev_usage_bytes").isNotNull() & (col("prev_usage_bytes") > 0),
                spark_round(
                    (col("usage_bytes") - col("prev_usage_bytes")) / col("prev_usage_bytes") * 100,
                    2
                )
            ).otherwise(None)
        )
        .withColumn(
            "user_growth_rate",
            when(
                col("prev_active_users").isNotNull() & (col("prev_active_users") > 0),
                spark_round(
                    (col("active_users") - col("prev_active_users")) / col("prev_active_users") * 100,
                    2
                )
            ).otherwise(None)
        )
        .select(
            col("domain"),
            col("usage_month"),
            col("usage_bytes"),
            col("active_users"),
            col("mail_count"),
            col("usage_growth_rate"),
            col("user_growth_rate"),
            # 용량 등급 분류
            when(col("usage_bytes") >= 1e12, "Enterprise")
            .when(col("usage_bytes") >= 1e11, "Large")
            .when(col("usage_bytes") >= 1e10, "Medium")
            .otherwise("Small").alias("usage_tier"),
        )
    )

    print(f"  ✅ 사용량 분석 생성 완료: {usage_analytics.count()}행")
    return usage_analytics


def create_revenue_analytics(
    spark: SparkSession,
    contracts_df: DataFrame,
) -> DataFrame:
    """매출 분석 테이블 생성
    
    고객별/서비스별 매출 집계
    """
    print("💰 매출 분석 테이블 생성")

    # CSP별 매출 집계
    revenue_by_csp = (
        contracts_df
        .filter(col("contract_status") == "ACTIVE")
        .groupBy("csp")
        .agg(
            count("*").alias("customer_count"),
            spark_sum("contract_gb").alias("total_contract_gb"),
            spark_sum("contract_users").alias("total_contract_users"),
            spark_avg("contract_gb").alias("avg_contract_gb"),
        )
    )

    # 공공분류별 매출 집계
    revenue_by_sector = (
        contracts_df
        .filter(col("contract_status") == "ACTIVE")
        .groupBy("public_sector_type")
        .agg(
            count("*").alias("customer_count"),
            spark_sum("contract_gb").alias("total_contract_gb"),
            spark_sum("contract_users").alias("total_contract_users"),
        )
    )

    # 고객별 매출 순위
    revenue_window = Window.orderBy(desc("contract_gb"))
    customer_revenue_rank = (
        contracts_df
        .filter(col("contract_status") == "ACTIVE")
        .withColumn("revenue_rank", dense_rank().over(revenue_window))
        .select(
            col("customer_name"),
            col("domain"),
            col("csp"),
            col("contract_gb"),
            col("contract_users"),
            col("revenue_rank"),
            when(col("revenue_rank") <= 10, "Top 10")
            .when(col("revenue_rank") <= 50, "Top 50")
            .otherwise("Others").alias("revenue_tier"),
        )
    )

    print(f"  ✅ 매출 분석 생성 완료")
    return customer_revenue_rank


# -----------------------------------------------------------------------------
# 저장 함수
# -----------------------------------------------------------------------------
def save_to_gold(
    spark: SparkSession,
    df: DataFrame,
    table_name: str,
) -> None:
    """골드 테이블 저장"""
    print(f"💾 골드 테이블 저장: {table_name}")

    namespace = "gold"
    target_table = f"iceberg.{namespace}.{table_name}"

    # 네임스페이스 생성
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS iceberg.{namespace}")

    # 스냅샷 날짜로 파티셔닝
    current_date_str = datetime.now().strftime("%Y-%m-%d")
    df_with_partition = df.withColumn("snapshot_date", to_date(lit(current_date_str)))

    # 테이블 저장
    if spark.catalog.tableExists(target_table):
        df_with_partition.writeTo(target_table).overwritePartitions()
    else:
        df_with_partition.writeTo(target_table).partitionedBy("snapshot_date").create()

    print(f"  ✅ 저장 완료: {table_name}")


# -----------------------------------------------------------------------------
# 메인 실행
# -----------------------------------------------------------------------------
def run_silver_to_gold_job(
    dataset: str = "example",
) -> None:
    """Silver to Gold 변환 메인 작업"""
    print("=" * 60)
    print("🚀 Silver to Gold 변환 시작")
    print(f"  - 데이터셋: {dataset}")
    print("=" * 60)

    spark = create_spark_session(app_name="Silver-to-Gold")

    # 실버 테이블에서 데이터 읽기
    contracts_df = spark.table(f"iceberg.silver.{dataset}_silver_contracts")
    contacts_df = spark.table(f"iceberg.silver.{dataset}_silver_contacts")
    usage_df = spark.table(f"iceberg.silver.{dataset}_silver_usage")

    # 골드 테이블 생성
    customer_master = create_customer_master(spark, contracts_df, contacts_df, usage_df)
    usage_analytics = create_usage_analytics(spark, usage_df)
    revenue_analytics = create_revenue_analytics(spark, contracts_df)

    # 골드 테이블 저장
    save_to_gold(spark, customer_master, f"{dataset}_gold_customer_master")
    save_to_gold(spark, usage_analytics, f"{dataset}_gold_usage_analytics")
    save_to_gold(spark, revenue_analytics, f"{dataset}_gold_revenue_analytics")

    print("=" * 60)
    print("✅ Silver to Gold 변환 완료")
    print("=" * 60)
    spark.stop()


def parse_args():
    parser = argparse.ArgumentParser(description="Silver to Gold ETL")
    parser.add_argument("--dataset", default="example", help="Dataset name")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_silver_to_gold_job(args.dataset)
