from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta

@dag(start_date=datetime(2024, 2, 2), schedule_interval='*/5 * * * *', catchup=False)
def test_night_dag():

    # Define tasks
    task_1 = BashOperator(task_id='brush_teeth', bash_command='echo "Brushed teeth"', retries=3, retry_delay=timedelta(minutes=5))
    task_2 = BashOperator(task_id='eat_dinner', bash_command='echo "Ate a healthy dinner"', retries=3, retry_delay=timedelta(minutes=5))
    task_3 = BashOperator(task_id='cocktail', bash_command='echo "Drank a cocktail"', retries=3, retry_delay=timedelta(minutes=5))

    # Define Python tasks using @task decorator
    @task
    def read_night_news():
        return 'Read the nighttime news headlines'

    @task
    def work_night_tasks():
        return 'Completed important night work tasks'

    @task
    def close_eyes():
        return 'closed my eyes and went night night'

    # Define the final tasks
    @task
    def review_night(news, work, relaxation):
        print(f"News: {news}")
        print(f"Work: {work}")
        print(f"Relaxation: {relaxation}")

    # Set task dependencies
    task_1 >> task_2 >> task_3
    task_2 >> task_3

    # Set Python task dependencies
    news_result = read_night_news()
    work_result = work_night_tasks()
    relax_result = close_eyes()

    # Set final task dependency
    review_night(news_result, work_result, relax_result)

test_night_dag()
