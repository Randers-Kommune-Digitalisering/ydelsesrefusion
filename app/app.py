from flask import Flask
from healthcheck import HealthCheck
from prometheus_client import generate_latest
from apscheduler.schedulers.background import BackgroundScheduler

from utils.logging import get_logger, APP_RUNNING
from utils.config import DEBUG, POD_NAME
from sftp import list_all_files, handle_files
from utils.custom_data_api import post_to_custom_data_connector


def create_app():
    app = Flask(__name__)
    health = HealthCheck()
    app.add_url_rule("/healthz", "healthcheck", view_func=lambda: health.run())
    app.add_url_rule('/metrics', "metrics", view_func=generate_latest)
    APP_RUNNING.labels(POD_NAME).set(1)
    return app


def get_files_job():
    file_list, sftp_conn = list_all_files()
    handle_files(file_list, sftp_conn)

logger = get_logger(__name__)
app = create_app()
scheduler = BackgroundScheduler()
# Get files every monday at noon
scheduler.add_job(get_files_job, 'cron', day_of_week='mon', hour=12)

if __name__ == "__main__":  # pragma: no cover
    
    # Kør ved første deployment (før tidsplan)
    get_files_job()

    # Tidsplan
    scheduler.start()
    app.run(debug=DEBUG, host='0.0.0.0', port=8080)


