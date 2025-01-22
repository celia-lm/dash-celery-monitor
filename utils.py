from dash import dcc, html
import dash_bootstrap_components as dbc
import datetime

app_description_text = """
The purpose of this app is to allow the user to interact with celery tasks
and see how we can get insights abour the different phases
of the task completition process programmatically (i.e. by using the celery python library).\n
The complete code can be reviewed in the [GitHub repository](https://github.com/celia-lm/dash-celery-monitor), but here's a summary
of the main actions and takeaways from the design process of this app.\n

Main reference: https://docs.celeryq.dev/en/latest/reference/celery.app.control.html

**Set-up**
```
celery_app = Celery(
    __name__,
    broker=f"{os.environ['REDIS_URL']}/1",
    backend=f"{os.environ['REDIS_URL']}/2",
)

# code to send a task inside a callback
task_id = celery_app.send_task(task_name, kwargs=task_kwargs)
```
To save this task id (for example, in a dcc.Store) we will need to convert it to str with `str(task_id)`. 
In the rest of this explanation, when `task_id` is used, it refers to the str version.

**Check the status of a task**\n
Option A (more general):
```
celery_inspector.query_task(task_id)
```
Example of result:
```
{'celery@27a8ec03a114': {'94be3755-011c-424e-a583-5fc73fd627ef': ['active', {'id': '94be3755-011c-424e-a583-5fc73fd627ef', 'name': 'my_task_1', 'args': [], 'kwargs': {'n_clicks': 1}, 'type': 'my_task_1', 'hostname': 'celery@27a8ec03a114', 'time_start': 1737474277.8087084, 'acknowledged': True, 'delivery_info': {'exchange': '', 'routing_key': 'celery', 'priority': 0, 'redelivered': False}, 'worker_pid': 4385}]}}
```
For this app, we are mainly interested in the task state, which can be: `active` or `reserved`. \n
When a task has been revoked/cancelled, the output will be a dict with an emmpty dict for value, like this:
```
{'celery@27a8ec03a114': {}}
```

Option B ([ref](https://docs.celeryq.dev/en/latest/reference/celery.result.html#celery.result.AsyncResult))
```
res = celery_app.AsyncResult(task_id)
print(res.status)
res.ready() # true when task has finished
res.get() # to retrieve the output of the task
```

**Commands to inspect the current tasks** ([ref](https://docs.celeryq.dev/en/v5.4.0/reference/celery.app.control.html#celery.app.control.Inspect.query_task))
```
celery_inspector = celery_app.control.inspect()
# tasks that are currently being executed by a worker
active_tasks = celery_inspector.active()
# tasks that have been assigned to a worker but have not started yet
reserved_tasks = celery_inspector.reserved()
# tasks that have been cancelled with 
cancelled_tasks = celery_inspector.revoked()
```

Examples of outputs:
```
# active_tasks
{'celery@27a8ec03a114': [{'id': '94be3755-011c-424e-a583-5fc73fd627ef', 'name': 'my_task_1', 'args': [], 'kwargs': {'n_clicks': 1}, 'type': 'my_task_1', 'hostname': 'celery@27a8ec03a114', 'time_start': 1737474277.8087084, 'acknowledged': True, 'delivery_info': {'exchange': '', 'routing_key': 'celery', 'priority': 0, 'redelivered': False}, 'worker_pid': 4385}]}
# reserved/queued tasks
{'celery@27a8ec03a114': [{'id': 'ab9eb6c7-643c-405d-a5ff-8aeb5ce6dac4', 'name': 'my_task_1', 'args': [], 'kwargs': {'n_clicks': 3}, 'type': 'my_task_1', 'hostname': 'celery@27a8ec03a114', 'time_start': None, 'acknowledged': False, 'delivery_info': {'exchange': '', 'routing_key': 'celery', 'priority': 0, 'redelivered': False}, 'worker_pid': None}, {'id': '44568f06-c78a-4c2a-9701-349a48ae9d47', 'name': 'my_task_2', 'args': [], 'kwargs': {'n_clicks': 1, 'len_min': 1}, 'type': 'my_task_2', 'hostname': 'celery@27a8ec03a114', 'time_start': None, 'acknowledged': False, 'delivery_info': {'exchange': '', 'routing_key': 'celery', 'priority': 0, 'redelivered': False}, 'worker_pid': None}]}
# cancelled/revoked tasks
{'celery@27a8ec03a114': ['210d6fe0-3c7c-4df2-8ec0-dc84d46869be', '6dfb3d24-2fca-4f27-98e4-1eb1832f4bc4']}
```

**Cancelling a task**
```
celery_app.control.revoke(task_id, terminate=True)
```
After we cancel a task, its id will appear in the list of revoked tasks, but its res.status might take a moment (1-2 seconds)
to change from "PENDING" to "REVOKED". 
"""

app_description = html.Div(
    dbc.Accordion(
        [
            dbc.AccordionItem(
                dcc.Markdown(app_description_text),
                title="Read more about this app",
            )
        ],
        start_collapsed=True
    )
)

task_description = dcc.Markdown("""
You can:
- Start two types of tasks and see how their status changes in the table. The celery process has two workers, so only two tasks (max) should show the 'Running' status at the same time.
- Select one task and cancel it by clicking on the 'Cancel' button.
- Get an overview of the tasks Celery is handling at any moment by clicking the 'Check Celery status and update table' button (the update can take up to 10 seconds and it will appear at the bottom of the window - scroll down if you don't see it). The table will get updated automatically every 5 seconds, and the 'Cancelled' status will appear immediately, but the 'Complete' and 'Running' status will only be updated by the interval or the button click.
""")

def celery_status_summary(celery_status_text):
    update_time = datetime.datetime.now()
    return html.Div(
    dbc.Accordion(
        [
            dbc.AccordionItem(
                dcc.Markdown(celery_status_text),
                title=f"Celery status (last update: {update_time})",
            )
        ],
        start_collapsed=True
    )
)

def update_row_value(row_dict: dict, updated_values: dict):
    updatedRow = row_dict.copy()
    for k, v in updated_values.items():
        updatedRow[k] = v
    return updatedRow

def get_celery_active_and_reserved(celery_inspector, celery_hostname):
    celery_data = []
    active_tasks = celery_inspector.active()
    reserved_tasks = celery_inspector.reserved()
    if active_tasks:
        for t in active_tasks.get(celery_hostname, []):
            if t != {}:
                t["status"] = "Running"
                if t["time_start"]:
                    try :
                        t["time_start"] = t["time_start"].strftime("%H:%M:%S")
                    except : # if it's float the above code will fail
                        t["time_start"] = datetime.datetime.fromtimestamp(t["time_start"]).strftime("%H:%M:%S")
                celery_data.append(t)
    if reserved_tasks:
        for t in reserved_tasks.get(celery_hostname, []):
            if t != {}:
                t["status"] = "Queued"
                if t["time_start"]:
                    try :
                        t["time_start"] = t["time_start"].strftime("%H:%M:%S")
                    except : # if it's float the above code will fail
                        t["time_start"] = datetime.datetime.fromtimestamp(t["time_start"]).strftime("%H:%M:%S")
                celery_data.append(t)
    
    return celery_data
