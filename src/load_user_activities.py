from apache_beam.io.gcp.internal.clients import bigquery
from bson.objectid import ObjectId

feedback_map = {
    "N/A": 0,
    "Not the best": 1,
    "OK": 2,
    "Great": 3,
    "Amazing": 4,
    "Transformative": 5
}


def find_feedback(activity_lines) -> int:
    feed_back = 0
    for line in activity_lines:
        if 'who' not in line and line["lineAddress"] == "RATE":
            feed_back = feedback_map[line["answerAsText"]]
    return feed_back


def check_if_activity_exist(activities, current_activity) -> bool:
    for activity in activities:
        if activity["email"] == current_activity["email"] and activity["activity"] == current_activity["activity"]:
            return True
    return False


def get_user_activities_data(db, PROJECT_ID, DATA_SET_ID):
    user_activities_model = db.userActivities
    user_model = db.user
    table_spec = bigquery.TableReference(
        projectId=f"{PROJECT_ID}",
        datasetId=f"{DATA_SET_ID}",
        tableId='user_activities'
    )

    user_activities_data_to_save = []

    user_activities = user_activities_model.find()

    for user_activity in user_activities:
        user = user_model.find_one({"_id": ObjectId(user_activity["iui"])})
        feed_back = find_feedback(
            user_activity["content"]["dialogue"]["lines"])

        data = {
            'user_activity_id': str(user_activity["_id"]),
            'user_id': user_activity["iui"],
            'company': user["company"],
            'email': user["email"],
            'module': user_activity["moduleDefExtRefId"],
            'activity': user_activity["activityDefExtRefId"],
            'feedback': feed_back
        }

        is_already_exist = check_if_activity_exist(
            user_activities_data_to_save, data)

        if is_already_exist:
            continue

        user_activities_data_to_save.append(data)

    table_schema = {
        'fields': [
                   {'name': 'user_activity_id', 'type': 'STRING', 'mode': 'REQUIRED'},
                   {'name': 'user_id', 'type': 'STRING', 'mode': 'REQUIRED'},
                   {'name': 'company', 'type': 'STRING', 'mode': 'REQUIRED'},
                   {'name': 'email', 'type': 'STRING', 'mode': 'REQUIRED'},
                   {'name': 'module', 'type': 'STRING', 'mode': 'REQUIRED'},
                   {'name': 'activity', 'type': 'STRING', 'mode': 'REQUIRED'},
                   {'name': 'feedback', 'type': 'INT64', 'mode': 'REQUIRED'},
        ]
    }
    
    result = dict()

    result["spec"] = table_spec
    result["schema"] = table_schema
    result["data"] = user_activities_data_to_save

    return result
