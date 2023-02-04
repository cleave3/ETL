from apache_beam.io.gcp.internal.clients import bigquery
from bson.objectid import ObjectId


def get_user_goals_data(db, PROJECT_ID, DATA_SET_ID):
    user_goals_model = db.userGoals
    user_model = db.user
    table_spec = bigquery.TableReference(
        projectId=f"{PROJECT_ID}",
        datasetId=f"{DATA_SET_ID}",
        tableId='user_goals'
    )

    user_goals_data_to_save = []

    user_goals = user_goals_model.find()

    for user_goal in user_goals:
        user = user_model.find_one({"_id": ObjectId(user_goal["iui"])})
        user_goals_data_to_save.append({
            'user_goal_id': str(user_goal["_id"]),
            'user_id': user_goal["iui"],
            'total_activities': user_goal["numActivitiesTotal"],
            'activities_completed': user_goal["numActivitiesCompleted"],
            'goal_name': user_goal['goalDefExtRefId'],
            'company': user["company"],
        })

    table_schema = {
        'fields': [
                   {'name': 'user_goal_id', 'type': 'STRING', 'mode': 'REQUIRED'},
                   {'name': 'user_id', 'type': 'STRING', 'mode': 'REQUIRED'},
                   {'name': 'total_activities', 'type': 'INT64', 'mode': 'REQUIRED'},
                   {'name': 'activities_completed',
                       'type': 'INT64', 'mode': 'REQUIRED'},
                   {'name': 'goal_name', 'type': 'STRING', 'mode': 'REQUIRED'},
                   {'name': 'company', 'type': 'STRING', 'mode': 'REQUIRED'},
        ]
    }
    result = dict()

    result["spec"] = table_spec
    result["schema"] = table_schema
    result["data"] = user_goals_data_to_save

    return result
