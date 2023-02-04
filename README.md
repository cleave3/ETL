# ETL


##### Setup
1. Install any version of python between 3.7 - 3.9
2. Install Dependencies
```
pip install dnspython
```

```
pip install python-dotenv
```
```
pip install apache-beam[gcp]
```
3. Create a .env file in the project root directory and add the following

```
DB_USERNAME=
DB_PASSWORD=
DB_HOST=
DB_NAME=
PROJECT_ID=
DATA_SET_ID=
TEMP_LOCATION=
```
* Note: 
```
DB_USERNAME= // target DB user name 
DB_PASSWORD=  // target DB password 
DB_HOST=  // target DB host 
DB_NAME=  // target DB name 
PROJECT_ID= // project ID on GCP
DATA_SET_ID= // DataSet ID on Big Query
TEMP_LOCATION= // A temporatory storage bucket path on the project. SHould be create on GCP storage bucket
```

4. Set cloud compute service account credentials to your env variables
```
 export GOOGLE_APPLICATION_CREDENTIALS="path_to_your_file_name.json"
```

5. Deploy pipeline
```
 python main.py \
 — runner DataflowRunner \
 — region europe-central2 \
 — project PROJECT_ID \
 — temp_location TEMP_LOCATION
```