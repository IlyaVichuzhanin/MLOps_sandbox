#!/bin/bash
export PATH="/home/airflow/.local/bin:$PATH"
exec airflow celery worker