from celery import Celery, group
from core.master_data_updater import UpdateMasterDatabase

app = Celery('tasks')
app.config_from_object('celery_config')

@app.task
def call_all_apis():
    obj = UpdateMasterDatabase()
    obj.run_updates()