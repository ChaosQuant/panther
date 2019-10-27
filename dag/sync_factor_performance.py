# -*- coding: utf-8 -*-
#

from builtins import range
from datetime import timedelta

import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator

args = {
    'owner': 'zzh',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0),
    'email': ['775665615@qq.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'adhoc':False,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'trigger_rule': u'all_success'
}

dag = DAG(
    dag_id='sync_factor_performance',
    default_args=args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=15),
)

run_rebuild_factor_basic_derivation_performance = BashOperator(
    task_id='run_rebuild_factor_basic_derivation_performance',
    bash_command='cd /app/wjh/panther && python client.py --factor_name="factor_basic_derivation"',
    dag=dag,
)

run_rebuild_factor_earning_expectation_performance = BashOperator(
    task_id='run_rebuild_factor_earning_expectation_performance',
    bash_command='cd /app/wjh/panther && python client.py --factor_name="factor_earning_expectation"',
    dag=dag,
)

run_rebuild_factor_capital_structure_performance = BashOperator(
    task_id='run_rebuild_factor_capital_structure_performance',
    bash_command='cd /app/wjh/panther && python client.py --factor_name="factor_capital_structure"',
    dag=dag,
)
run_rebuild_factor_cash_flow_performance = BashOperator(
    task_id='run_rebuild_factor_cash_flow_performance',
    bash_command='cd /app/wjh/panther && python client.py --factor_name="factor_cash_flow"',
    dag=dag,
)
run_rebuild_factor_earning_performance = BashOperator(
    task_id='run_rebuild_factor_earning_performance',
    bash_command='cd /app/wjh/panther && python client.py --factor_name="factor_earning"',
    dag=dag,
)
run_rebuild_factor_historical_growth_performance = BashOperator(
    task_id='run_rebuild_factor_historical_growth_performance',
    bash_command='cd /app/wjh/panther && python client.py --factor_name="factor_historical_growth"',
    dag=dag,
)
run_rebuild_factor_operation_capacity_performance = BashOperator(
    task_id='run_rebuild_factor_operation_capacity_performance',
    bash_command='cd /app/wjh/panther && python client.py --factor_name="factor_operation_capacity"',
    dag=dag,
)
run_rebuild_factor_per_share_indicators_performance = BashOperator(
    task_id='run_rebuild_factor_per_share_indicators_performance',
    bash_command='cd /app/wjh/panther && python client.py --factor_name="factor_per_share_indicators"',
    dag=dag,
)
run_rebuild_factor_revenue_quality_performance = BashOperator(
    task_id='run_rebuild_factor_revenue_quality_performance',
    bash_command='cd /app/wjh/panther && python client.py --factor_name="factor_revenue_quality"',
    dag=dag,
)
run_rebuild_factor_solvency_performance = BashOperator(
    task_id='run_rebuild_factor_solvency_performance',
    bash_command='cd /app/wjh/panther && python client.py --factor_name="factor_solvency"',
    dag=dag,
)

run_rebuild_factor_momentum_performance = BashOperator(
    task_id='run_rebuild_factor_momentum_performance',
    bash_command='cd /app/wjh/panther && python client.py --factor_name="factor_momentum"',
    dag=dag,
)
run_rebuild_factor_power_volume_performance = BashOperator(
    task_id='run_rebuild_factor_power_volume_performance',
    bash_command='cd /app/wjh/panther && python client.py --factor_name="factor_power_volume"',
    dag=dag,
)
run_rebuild_factor_price_volume_performance = BashOperator(
    task_id='run_rebuild_factor_price_volume_performance',
    bash_command='cd /app/wjh/panther && python client.py --factor_name="factor_price_volume"',
    dag=dag,
)
run_rebuild_factor_reversal_performance = BashOperator(
    task_id='run_rebuild_factor_reversal_performance',
    bash_command='cd /app/wjh/panther && python client.py --factor_name="factor_reversal"',
    dag=dag,
)
run_rebuild_factor_sentiment_performance = BashOperator(
    task_id='run_rebuild_factor_sentiment_performance',
    bash_command='cd /app/wjh/panther && python client.py --factor_name="factor_sentiment"',
    dag=dag,
)
run_rebuild_factor_volume_performance = BashOperator(
    task_id='run_rebuild_factor_volume_performance',
    bash_command='cd /app/wjh/panther && python client.py --factor_name="factor_volume"',
    dag=dag,
)

run_rebuild_factor_valuation_estimation_performance = BashOperator(
    task_id='run_rebuild_factor_valuation_estimation_performance',
    bash_command='cd /app/wjh/panther && python client.py --factor_name="factor_valuation_estimation"',
    dag=dag,
)

run_rebuild_factor_alpha101_performance = BashOperator(
    task_id='run_rebuild_factor_alpha101_performance',
    bash_command='cd /app/wjh/panther && python client.py --factor_name="factor_alpha101"',
    dag=dag,
)
run_rebuild_factor_alpha191_performance = BashOperator(
    task_id='run_rebuild_factor_alpha191_performance',
    bash_command='cd /app/wjh/panther && python client.py --factor_name="factor_alpha191"',
    dag=dag,
)

run_rebuild_factor_volatility_value_performance = BashOperator(
    task_id='run_rebuild_factor_volatility_value_performance',
    bash_command='cd /app/wjh/panther && python client.py --factor_name="factor_volatility_value"',
    dag=dag,
)

if __name__ == "__main__":
    dag.cli()
