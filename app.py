import time
import os
import datetime
from dash import Dash, Input, Output, State, ctx, html, dcc, callback, set_props, no_update
from celery import Celery, worker
import dash_ag_grid as dag
from icecream import ic
import utils
import dash_bootstrap_components as dbc

REDIS_NUM = 1 if "workspace" in os.environ.get("DASH_REQUESTS_PATHNAME_PREFIX") else 3

celery_app = Celery(
    __name__,
    broker=f"{os.environ['REDIS_URL']}/{REDIS_NUM}",
    backend=f"{os.environ['REDIS_URL']}/{REDIS_NUM + 1}",
)
celery_inspector = celery_app.control.inspect()
# CELERY_HOSTNAME = worker.worker.WorkController(app=celery_app).hostname

app = Dash(update_title=None, external_stylesheets=[dbc.themes.BOOTSTRAP])
server = app.server

def layout():

    # populate the table with tasks that have been sent prior to the page load
    initial_celery_data = utils.get_celery_active_and_reserved(celery_inspector)
    
    return dbc.Container(
        [
            html.H2("Celery Monitor App"),
            utils.app_description,
            # components to trigger celery tasks
            html.H4("Tasks to test celery", style={"padding-top":"2px"}),
            utils.task_description,
            dbc.Row([
                dbc.Col(dbc.Card(dbc.CardBody([
                    dbc.Button(id="button_1", children="Run task 1! (2 min)", style={"margin":"2px"}),
                    html.P(id="paragraph_1", children=["Button for task 1 not clicked"])
                ]), style={"height":"175px"})),
                dbc.Col(dbc.Card(dbc.CardBody([
                    html.Span("Select duration (in minutes) for task 2:"),
                    dcc.Input(id="task_2_len", type="number", min=1, step=1, value=1), html.Br(),
                    dbc.Button(id="button_2", children="Run task 2!", style={"margin":"2px"}),
                    html.P(id="paragraph_2", children=["Button for task 2 not clicked"]),
                ]), style={"height":"175px"}
                ))
            ], style={"margin-botton":"5px"}), 
            # table to display the task checks with interval to update it every 5 seconds
            dcc.Interval(
                id="interval", interval=1000 * 5, disabled=True
            ),  
            dbc.Button(
                id="check_celery", children="Check celery status and update table", style={"margin":"2px"}
            ),
            dbc.Checklist(
                options=[
                    {"label": "Include tasks triggered by other users (slower)", "value": 1},
                ],
                value=[1],
                id="include_other_users",
                switch=True,
                inline=True,
            ),
            html.Div(id="check_celery_output"),
            dag.AgGrid(
                id="dag_celery",
                rowData=initial_celery_data,
                columnDefs=[
                    {"field": c, "label": c}
                    # fields from TASK_INFO: https://docs.celeryq.dev/en/latest/reference/celery.app.control.html#celery.app.control.Inspect.query_task
                    # you could add your own custom fields and updates, for example: "triggered_by", or "cancelled_at"
                    # "time_start" indicates the time the task was SENT to celery, not the time it actually started running
                    for c in [
                        "id",
                        "name",
                        "args",
                        "kwargs",
                        "time_start",
                        "time_end",
                        "status",
                    ]
                ],
                getRowId="params.data.id",
                dashGridOptions={"rowSelection": "single"},
            ),
            dbc.Button(id="cancel_task", children="Cancel selected task", disabled=True),
        ], style={"padding":"10px"}
    )

app.layout = layout

# structure: task_name : {"kwargs":[arg1, arg2], "output"}
# this is not necessary for the monitor itself; it's for updating the layout when the tasks finish running
# the monitor will work too for tasks with no output
TASK_OUTPUTS = {
    "my_task_1": {"component_id": "paragraph_1", "component_prop": "children"},
    "my_task_2": {"component_id": "paragraph_2", "component_prop": "children"},
}

# I've created two simple tasks
@celery_app.task(name="my_task_1")
def mytask1_wrapped(n_clicks):
    time.sleep(60 * 2)
    # print to check if the task keeps running after being cancelled
    # ic is not celery-friendly
    print(f"Finishing task 1 with {n_clicks}")
    return f"task 1: Clicked {n_clicks} times completed at {datetime.datetime.now()}"


@celery_app.task(name="my_task_2")
def mytask2_wrapped(n_clicks, len_min):
    # prints to check if the task keeps running after being cancelled
    for i in range(len_min):
        if i == 0 : 
            print(f"my_task_2 started with expected duration of {len_min} min. Current time is: {datetime.datetime.now()}")
        else : 
            print(f"{i} min have passed at {datetime.datetime.now()}")
        time.sleep(60)

    print(f"Finishing task 2 with n_clicks={n_clicks} and len={len_min}")
    return f"task 2: Clicked {n_clicks} times completed at {datetime.datetime.now()}"


# callback with no output for improved performance
# https://dash.plotly.com/advanced-callbacks#callbacks-with-no-outputs
@callback(
    Input("button_1", "n_clicks"),
    Input("button_2", "n_clicks"),
    State("task_2_len", "value"),
    prevent_initial_call=True,
)
def update_clicks(n_clicks_1, n_clicks_2, len_min):
    if ctx.triggered:
        k, v = list(ctx.triggered_prop_ids.items())[0]  # there will only be one item

        # I haven't found a smart way to automate this since every task (in a more complex app)
        # could have very different kwargs and the objects passed as callback args different names
        # (e.g. n_clicks_1 for the n_clicks argument)
        task_kwargs = {}
        if v == "button_1":
            task_name = "my_task_1"
            task_kwargs = {"n_clicks": n_clicks_1}
        elif v == "button_2":
            task_name = "my_task_2"
            task_kwargs = {"n_clicks": n_clicks_2, "len_min":len_min}
        # for error handling:
        else:
            task_name = None

        if task_name:
            task_id = celery_app.send_task(task_name, kwargs=task_kwargs)
            task_id_str = str(task_id)
            triggered_at = datetime.datetime.now()
            # https://dash.plotly.com/dash-ag-grid/client-side#transaction-updates
            newRows = [
                {
                    "id": task_id_str,
                    "name": task_name,
                    "kwargs": str(task_kwargs),
                    "time_start": triggered_at.strftime("%H:%M:%S"),
                    "time_end": None,
                    "status": "Queued",
                }
            ]
            set_props("dag_celery", {"rowTransaction": {"add": newRows}})

    # no return statement


# this updates the "disabled" property of the interval, making it start running or stop
# other updates to the grid with the task info are done via set_props
@callback(
    Output("interval", "disabled"),
    Input("dag_celery", "rowData"),
    Input("interval", "n_intervals"),
    Input("check_celery", "n_clicks"),
    State("interval", "disabled"),
    State("include_other_users", "value"),
    prevent_initial_call=True,
)
def check_task_status(current_tasks, _intervals, _check_celery, _disabled, include_other_users):
    # if there are tasks and none of them is pending, stop interval
    # all(...) will return true if current_tasks is empty too, that's why we add "if current_tasks and"
    if current_tasks and all(
        [task["status"] in ["Cancelled", "Complete"] for task in current_tasks]
    ) and (ctx.triggered_id != "check_celery"):
        return True  # stop interval
    elif ctx.triggered_id == "dag_celery":
        # start interval when a record is added to the table if it isn't running yet
        return False if _disabled else no_update
    # if it's the interval what triggers the callback, run the check for tasks' status
    elif ctx.triggered_id in ["interval", "check_celery"]:
        if include_other_users: # possible values: [], [True]
            active_and_reserved = utils.get_celery_active_and_reserved(celery_inspector)
            in_table = [t["id"] for t in current_tasks]
            new_tasks = []
            for t in active_and_reserved:
                if t["id"] in in_table:
                    continue
                else :
                    new_tasks.append(t)
                    current_tasks.append(t)
               
            # add them to the table too
            if new_tasks:
                set_props("dag_celery", {"rowTransaction": {"add": new_tasks}})

        for task_dict in current_tasks:
            # don't do anything with tasks that have already been cancelled or completed
            if task_dict["status"] in ["Cancelled", "Complete"]:
                # for checks
                # task_id = task_dict["task_id"]
                # res = celery_app.AsyncResult(task_id)
                # ic(task_id, celery_inspector.query_task(task_id), res.status)
                continue
            # if task is Queued or Running
            else:
                task_id = task_dict["id"]
                res = celery_app.AsyncResult(task_id)
                # ic(task_id, celery_inspector.query_task(task_id), res.status)
                # double check in case of concurrent callbacks
                ic(task_id, res.status)
                if res.status == "REVOKED":
                    continue
                # task finished
                elif res.ready():
                    # more info about disable_sync_subtasks: https://docs.celeryq.dev/en/latest/userguide/tasks.html#avoid-launching-synchronous-subtasks
                    result = res.get(disable_sync_subtasks=False)
                    output_info = TASK_OUTPUTS.get(task_dict["name"])
                    set_props(
                        output_info.get("component_id"),
                        {output_info.get("component_prop"): result},
                    )

                    set_props(
                        "dag_celery",
                        {
                            "rowTransaction": {
                                "update": [
                                    utils.update_row_value(
                                        task_dict,
                                        {
                                            "status": "Complete",
                                            "time_end": datetime.datetime.now().strftime(
                                                "%H:%M:%S"
                                            ),
                                        },
                                    )
                                ]
                            }
                        },
                    )
                else:
                    # task_state is one of: "active", "reserved"
                    # it's different from res.status, which can be ACTIVE, REVOKED, PENDING
                    queried_task = celery_inspector.query_task(task_id)
                    print(queried_task)
                    task_state = [task_info[task_id][0] for task_info in queried_task.values()]
                    print(task_state)
                    if task_state == "reserved":
                        continue
                    # only update the grid if it hasn't been updated yet
                    elif task_dict["status"] != "Running":
                        set_props(
                            "dag_celery",
                            {
                                "rowTransaction": {
                                    "update": [
                                        utils.update_row_value(
                                            task_dict, {"status": "Running"}
                                        )
                                    ]
                                }
                            },
                        )
                    else:
                        continue

        return no_update

@callback(
    Input("cancel_task", "n_clicks"),
    State("dag_celery", "selectedRows"),
    prevent_initial_row=True,
)
def cancel_job(click, selectedRows):
    task_dict = selectedRows[0]
    res1 = celery_app.AsyncResult(task_dict["id"])
    celery_app.control.revoke(task_dict["id"], terminate=True)
    set_props(
        "dag_celery",
        {
            "rowTransaction": {
                "update": [utils.update_row_value(task_dict, {"status": "Cancelled"})]
            }
        }
    )

# callbacks for performing checks (it takes 3-10 seconds, depending on the amount of tasks)
@callback(
    Output("check_celery_output", "children"),
    Input("check_celery", "n_clicks"),
    State("dag_celery", "rowData"),
    prevent_initial_call=True,
)
def celery_status(_, current_tasks):
    task_queries = [
        celery_inspector.query_task(task_dict["id"]) for task_dict in current_tasks
    ]
    text = f"""
    **Active tasks:** 
    ```
    {celery_inspector.active()}
    ```
    **Revoked tasks:** 
    ```
    {celery_inspector.revoked()}
    ```
    **Reserved tasks:** 
    ```
    {celery_inspector.reserved()}
    ```
    ## Information by task_id
    ```
    {task_queries}
    ```
    """
    return utils.celery_status_summary(text)

# auxiliar callback to disable the cancel button if no row is selected
@callback(
    Output("cancel_task", "disabled"),
    Input("dag_celery", "selectedRows"),
    prevent_initial_call=True
)
def disable_button(selectedRows):
    if selectedRows:
        return False
    else :
        return True

if __name__ == "__main__":
    app.run(debug=True)