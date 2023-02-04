from apache_beam.io.gcp.internal.clients import bigquery
from bson.objectid import ObjectId


def get_user_module_data(db, PROJECT_ID, DATA_SET_ID):
    user_module_model = db.userModules
    user_model = db.user
    table_spec = bigquery.TableReference(
        projectId=f"{PROJECT_ID}",
        datasetId=f"{DATA_SET_ID}",
        tableId='user_module'
    )

    user_module_data_to_save = []

    # extra data
    user_modules = user_module_model.find()

    # transform
    for user_module in user_modules:
        user = user_model.find_one({"_id": ObjectId(user_module["iui"])})
        user_module_data_to_save.append({
                'user_module_id': str(user_module["_id"]), 
                'user_id': user_module["iui"], 
                'total_activities': len(user_module["userActivityHolderIds"]), 
                'activities_completed': user_module["numActivitiesCompleted"],
                'user_goal_id': str(user_module["userGoalId"]),
                'module_name': user_module["moduleDefExtRefId"],
                'company': user["company"],
            })

    table_schema = {
        'fields': [
                   {'name': 'user_module_id', 'type': 'STRING', 'mode': 'REQUIRED'},
                   {'name': 'user_id', 'type': 'STRING', 'mode': 'REQUIRED'},
                   {'name': 'total_activities', 'type': 'INT64', 'mode': 'REQUIRED'},
                   {'name': 'activities_completed', 'type': 'INT64', 'mode': 'REQUIRED'},
                   {'name': 'user_goal_id', 'type': 'STRING', 'mode': 'REQUIRED'},
                   {'name': 'module_name', 'type': 'STRING', 'mode': 'REQUIRED'},
                   {'name': 'company', 'type': 'STRING', 'mode': 'REQUIRED'},
        ]
    }

    result = dict()

    result["spec"] = table_spec
    result["schema"] = table_schema
    result["data"] = user_module_data_to_save

    return result
