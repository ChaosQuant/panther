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
    'retries': 0,
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
    dag_id='rebuild_factor_data',
    default_args=args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=15),
)

run_update_factor_earning_expectation = BashOperator(
    task_id='run_update_factor_earning_expectation',
    bash_command='cd ~/app/panther && python client.py --rebuild=True --packet_name="earning_expectation.factor_earning_expectation" --class_name="FactorEarningExpectation"',
    dag=dag,
)

run_update_factor_capital_structure = BashOperator(
    task_id='run_update_factor_capital_structure',
    bash_command='cd ~/app/panther && python client.py --rebuild=True --packet_name="financial.factor_capital_structure" --class_name="FactorCapitalStructure"',
    dag=dag,
)
run_update_factor_cash_flow = BashOperator(
    task_id='run_update_factor_cash_flow',
    bash_command='cd ~/app/panther && python client.py --rebuild=True --packet_name="financial.factor_cash_flow" --class_name="FactorCashFlow"',
    dag=dag,
)
run_update_factor_earning = BashOperator(
    task_id='run_update_factor_earning',
    bash_command='cd ~/app/panther && python client.py --rebuild=True --packet_name="financial.factor_earning" --class_name="FactorEarning"',
    dag=dag,
)
run_update_factor_historical_growth = BashOperator(
    task_id='run_update_factor_historical_growth',
    bash_command='cd ~/app/panther && python client.py --rebuild=True --packet_name="financial.factor_historical_growth" --class_name="FactorHistoricalGrowth"',
    dag=dag,
)
run_update_factor_operation_capacity = BashOperator(
    task_id='run_update_factor_operation_capacity',
    bash_command='cd ~/app/panther && python client.py --rebuild=True --packet_name="financial.factor_operation_capacity" --class_name="FactorOperationCapacity"',
    dag=dag,
)
run_update_factor_per_share_indicators = BashOperator(
    task_id='run_update_factor_per_share_indicators',
    bash_command='cd ~/app/panther && python client.py --rebuild=True --packet_name="financial.factor_per_share_indicators" --class_name="FactorPerShareIndicators"',
    dag=dag,
)
run_update_factor_revenue_quality = BashOperator(
    task_id='run_update_factor_revenue_quality',
    bash_command='cd ~/app/panther && python client.py --rebuild=True --packet_name="financial.factor_revenue_quality" --class_name="FactorRevenueQuality"',
    dag=dag,
)
run_update_factor_solvency = BashOperator(
    task_id='run_update_factor_solvency',
    bash_command='cd ~/app/panther && python client.py --rebuild=True --packet_name="financial.factor_solvency" --class_name="FactorSolvency"',
    dag=dag,
)

run_update_factor_momentum = BashOperator(
    task_id='run_update_factor_momentum',
    bash_command='cd ~/app/panther && python client.py --rebuild=True --packet_name="technical.factor_momentum" --class_name="FactorMomentum"',
    dag=dag,
)
run_update_factor_power_volume = BashOperator(
    task_id='run_update_factor_power_volume',
    bash_command='cd ~/app/panther && python client.py --rebuild=True --packet_name="technical.factor_power_volume" --class_name="FactorPowerVolume"',
    dag=dag,
)
run_update_factor_price_volume = BashOperator(
    task_id='run_update_factor_price_volume',
    bash_command='cd ~/app/panther && python client.py --rebuild=True --packet_name="technical.factor_price_volume" --class_name="FactorPriceVolume"',
    dag=dag,
)
run_update_factor_reversal = BashOperator(
    task_id='run_update_factor_reversal',
    bash_command='cd ~/app/panther && python client.py --rebuild=True --packet_name="technical.factor_reversal" --class_name="FactorReversal"',
    dag=dag,
)
run_update_factor_sentiment = BashOperator(
    task_id='run_update_factor_sentiment',
    bash_command='cd ~/app/panther && python client.py --rebuild=True --packet_name="technical.factor_sentiment" --class_name="FactorSentiment"',
    dag=dag,
)
run_update_factor_volume = BashOperator(
    task_id='run_update_factor_volume',
    bash_command='cd ~/app/panther && python client.py --rebuild=True --packet_name="technical.factor_volume" --class_name="FactorVolume"',
    dag=dag,
)

run_update_factor_valuation = BashOperator(
    task_id='run_update_factor_valuation',
    bash_command='cd ~/app/panther && python client.py --rebuild=True --packet_name="valuation_estimation.factor_valuation_estimation" --class_name="FactorValuationEstimation"',
    dag=dag,
)

run_update_factor_alpha101 = BashOperator(
    task_id='run_update_factor_alpha101',
    bash_command='cd ~/app/panther && python client.py --rebuild=True --packet_name="alphax.factor_alpha101" --class_name="FactorAlpha101"',
    dag=dag,
)
run_update_factor_alpha191 = BashOperator(
    task_id='run_update_factor_alpha191',
    bash_command='cd ~/app/panther && python client.py --rebuild=True --packet_name="alphax.factor_alpha191" --class_name="FactorAlpha191"',
    dag=dag,
)

run_update_factor_volatility_value = BashOperator(
    task_id='run_update_factor_volatility_value',
    bash_command='cd ~/app/panther && python client.py --rebuild=True --packet_name="alphax.factor_volatility_value" --class_name="FactorVolatilityValue"',
    dag=dag,
)

# run_update_balance_report >> run_update_balance_mrq >> run_update_balance_ttm
# run_update_cash_flow_report >> run_update_cash_flow_mrq >> run_update_cash_flow_ttm
# run_update_income_report >> run_update_income_mrq >> run_update_income_ttm
# run_update_indicator_report >> run_update_indicator_mrq >> run_update_indicator_ttm

if __name__ == "__main__":
    dag.cli()
