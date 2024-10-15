import requests, json

api_url = "http://127.0.0.1:8000/home"

#Request data
response = requests.post(api_url, json=json.dumps({"filename":"inputs/pg-grimm.txt",
                                "mapper_task_count":6, "reducer_task_count":4}))

print(response)