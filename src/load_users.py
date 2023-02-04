from datetime import datetime
from apache_beam.io.gcp.internal.clients import bigquery


def get_user_data(db, PROJECT_ID, DATA_SET_ID):
    user_model = db.user
    table_spec = bigquery.TableReference(
        projectId=f"{PROJECT_ID}",
        datasetId=f"{DATA_SET_ID}",
        tableId='user'
    )

    user_data_to_save = []

    # extra data
    users = user_model.find()
    # transform
    for user in users:
        date = None
        if user["lastAt"] is not None:
            date = user["lastAt"].isoformat().replace("T", " ")

        user_data_to_save.append({
            'user_id': str(user["_id"]),
            'company': user["company"],
            'status': user["status"],
            'email': user["email"],
            'last_login': date
        })

    table_schema = {
        'fields': [
            {'name': 'user_id', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'email', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'company', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'status', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'last_login', 'type': 'DATETIME', 'mode': 'NULLABLE'},
        ]
    }

    result = dict()

    result["spec"] = table_spec
    result["schema"] = table_schema
    result["data"] = user_data_to_save

    return result
