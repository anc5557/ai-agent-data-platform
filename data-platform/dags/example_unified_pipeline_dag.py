"""Airflow DAG 예시 - 통합 ETL 파이프라인

GitHub 데이터를 수집/변환/적재하는 전체 파이프라인을 하나의 DAG로 통합합니다.

Pipeline:
1. Extract: GitHub API → S3 (NDJSON)
   - GraphQL: Project Items 전체 조회
   - REST: Issues, PRs, Comments, Timeline
2. Transform: S3 → Iceberg
   - JSONL → Bronze
   - Bronze → Silver
   - Silver → Gold
3. Load: Gold → Milvus (벡터 임베딩)
"""
from __future__ import annotations

from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.task_group import TaskGroup

# 공통 설정 import (환경별 분리)
# from common.environment import iceberg_conf_overrides
# from common.spark_conf import base_conf_low_memory, with_overrides


# -----------------------------------------------------------------------------
# DAG 기본 설정
# -----------------------------------------------------------------------------
DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

KST = pendulum.timezone("Asia/Seoul")

# Spark 공통 설정
SPARK_CONF = {
    "spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.iceberg.type": "rest",
    "spark.sql.catalog.iceberg.uri": "http://iceberg-rest:8181",
    "spark.sql.catalog.iceberg.s3.endpoint": "http://minio:9000",
    "spark.sql.catalog.iceberg.warehouse": "s3a://lakehouse/",
}


# -----------------------------------------------------------------------------
# Python Task 함수
# -----------------------------------------------------------------------------
def extract_github_data(**context) -> dict:
    """GitHub API에서 데이터 추출 (GraphQL + REST)
    
    실제 구현에서는 GitHub API를 호출하여 데이터를 S3에 저장합니다.
    """
    from airflow.models import Variable
    
    dag_run_conf = context.get("dag_run").conf if context.get("dag_run") else {}
    
    # GitHub Token은 Airflow Variable에서 가져옴
    github_token = dag_run_conf.get("github_token") or Variable.get("GITHUB_TOKEN")
    org = dag_run_conf.get("org", "mycompany-cloud")
    repos = dag_run_conf.get("repos", ["service-mail", "service-addon"])
    
    # TODO: 실제 GitHub API 호출 로직
    # - GraphQL로 Project Items 페이지네이션 조회
    # - REST API로 Issues/PRs/Comments/Timeline 수집
    # - S3에 NDJSON 형태로 저장
    
    ingest_date = datetime.now().strftime("%Y-%m-%d")
    
    return {
        "org": org,
        "repos": repos,
        "ingest_date": ingest_date,
        "status": "extracted",
    }


def upload_to_s3(**context) -> dict:
    """추출된 데이터를 S3에 NDJSON으로 업로드
    
    Airflow S3Hook을 사용하여 MinIO에 업로드합니다.
    """
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    
    ti = context["ti"]
    extract_result = ti.xcom_pull(task_ids="extract.extract_github_data")
    
    # S3Hook으로 MinIO 연결
    s3_hook = S3Hook(aws_conn_id="minio_default")
    bucket = "github"
    ingest_date = extract_result.get("ingest_date", datetime.now().strftime("%Y-%m-%d"))
    
    # TODO: 실제 NDJSON 데이터 업로드
    # s3_hook.load_string(
    #     string_data=ndjson_content,
    #     key=f"project_items/date={ingest_date}/data.jsonl",
    #     bucket_name=bucket,
    #     replace=True,
    # )
    
    return {
        "bucket": bucket,
        "ingest_date": ingest_date,
        "status": "uploaded",
    }


def embed_and_sync_to_milvus(**context) -> dict:
    """Gold 레이어 청크를 벡터화하여 Milvus에 동기화
    
    주요 로직:
    1. Trino에서 임베딩 대상 청크 조회
    2. Ollama API로 Dense 임베딩 생성
    3. BM25 스타일 Sparse 벡터 생성
    4. Milvus에 upsert
    5. 동기화 상태 테이블 업데이트
    """
    from pymilvus import connections, Collection
    
    dag_run_conf = context.get("dag_run").conf or {}
    
    batch_size = int(dag_run_conf.get("batch_size", 64))
    embed_model = dag_run_conf.get("embed_model", "bge-m3")
    
    # TODO: 실제 임베딩 및 Milvus 동기화 로직
    # 1. Trino에서 github_doc_chunks_gd 테이블 조회
    # 2. Ollama /api/embeddings 호출로 Dense 벡터 생성
    # 3. BM25 스타일 토큰화로 Sparse 벡터 생성
    # 4. collection.upsert() 호출
    # 5. github_vector_sync_gd 테이블 업데이트
    
    return {
        "batch_size": batch_size,
        "embed_model": embed_model,
        "status": "synced",
    }


# -----------------------------------------------------------------------------
# DAG 정의
# -----------------------------------------------------------------------------
with DAG(
    dag_id="unified_etl_pipeline",
    default_args=DEFAULT_ARGS,
    description="GitHub 데이터 통합 ETL 파이프라인 (Extract → Transform → Load)",
    schedule_interval="0 3 * * *",  # 매일 오전 3시 (KST 기준)
    start_date=datetime(2025, 1, 1, tzinfo=KST),
    catchup=False,
    max_active_runs=1,
    tags=["github", "etl", "iceberg", "milvus"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # =========================================================================
    # Extract Phase - GitHub API에서 데이터 수집
    # =========================================================================
    with TaskGroup(group_id="extract") as extract_group:
        extract_task = PythonOperator(
            task_id="extract_github_data",
            python_callable=extract_github_data,
        )
        
        upload_task = PythonOperator(
            task_id="upload_to_s3",
            python_callable=upload_to_s3,
        )
        
        extract_task >> upload_task

    # =========================================================================
    # Transform Phase - Spark ETL (Bronze → Silver → Gold)
    # =========================================================================
    with TaskGroup(group_id="transform") as transform_group:
        
        # JSONL → Bronze
        jsonl_to_bronze = SparkSubmitOperator(
            task_id="jsonl_to_bronze",
            application="/opt/spark-apps/jsonl_to_bronze.py",
            conn_id="spark_default",
            conf=SPARK_CONF,
            application_args=["--bucket", "github", "--target-date", "{{ ds }}"],
        )
        
        # Bronze → Silver
        bronze_to_silver = SparkSubmitOperator(
            task_id="bronze_to_silver",
            application="/opt/spark-apps/bronze_to_silver.py",
            conn_id="spark_default",
            conf=SPARK_CONF,
            application_args=[
                "--bronze-bucket", "bronze",
                "--silver-bucket", "silver",
                "--dataset", "github",
            ],
        )
        
        # Silver → Gold
        silver_to_gold = SparkSubmitOperator(
            task_id="silver_to_gold",
            application="/opt/spark-apps/silver_to_gold.py",
            conn_id="spark_default",
            conf=SPARK_CONF,
            application_args=[
                "--silver-bucket", "silver",
                "--gold-bucket", "gold",
                "--dataset", "github",
            ],
        )
        
        jsonl_to_bronze >> bronze_to_silver >> silver_to_gold

    # =========================================================================
    # Load Phase - Gold → Milvus 벡터 임베딩
    # =========================================================================
    with TaskGroup(group_id="load") as load_group:
        embed_task = PythonOperator(
            task_id="embed_and_sync_to_milvus",
            python_callable=embed_and_sync_to_milvus,
        )

    # =========================================================================
    # Task 의존성
    # =========================================================================
    start >> extract_group >> transform_group >> load_group >> end
