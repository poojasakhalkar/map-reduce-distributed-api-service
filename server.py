from flask import Flask, jsonify, request 
import json, glob, os, threading, requests

# creating a Flask app 
app = Flask(__name__) 

client_port_details = []

def task_set_up(data, task_name):
    """Input - Task Name and Metadata
       Function check the Task Name, Task Count and start the processing accordingly. 
    """
    try:
        count = 0
        thread_list = []
        if task_name == "map":
            task_count = int(data['mapper_task_count'])
            files = glob.glob('intermediate/*.txt')
            print(f"File exist in intermediate folder from previous task - {files}. Removing the files.")
            for f in files:
                os.remove(f)
        elif task_name == "reduce":
            files = glob.glob('out/*.txt')
            print(f"File exist in out folder from previous task - {files}. Removing the files.")
            task_count = int(data['reducer_task_count'])
            for f in files:
                os.remove(f)
        for i in range(0, task_count):
            if len(client_port_details) > i or count <= i:
                if task_name == "map": 
                    data["mapper_task_id"] = str(i)
                    data["task"] = "map"
                    print(f"Task set as map with ID - {str(i)}")
                elif task_name == "reduce":
                    data["reducer_task_id"] = str(i)
                    data["task"] = "reduce"
                    print(f"Task set as reducer with ID - {str(i)}")
                if len(client_port_details) > i:
                    api_url = "http://127.0.0.1:"+str(client_port_details[i])+"/mapper_reducer"
                else:
                    api_url = "http://127.0.0.1:"+str(client_port_details[0])+"/mapper_reducer"
                print(f"Sending request to url - {api_url}")
                thread_1 = threading.Thread(target=lambda:requests.post(api_url, json=json.dumps(data)))
                thread_1.start()
                print(f"Thread created for {data['task']}.")
                thread_list.append(thread_1)
                count = count+1    
        print(f"Waiting for all task to complete for {task_name}")            
        for my_thread in thread_list:
            my_thread.join()
        print(f"Task - {task_name} completed.\n")
        return json.dumps({"status":"success"})
    except Exception as e:
        print(e)
        print(f"Error in task_set_up function - {e}")
        return json.dumps({"status":"failed"})


@app.route('/home', methods=['POST']) 
def home(): 
    """Server get the metadata about the request and start the processing accordingly."""
    try:
        data = json.loads(request.get_json())
        print(f"Active Client port details are - {client_port_details}")
        print(f"Map function Task Count - {data['mapper_task_count']}")
        print(f"Reduce function Task Count - {data['reducer_task_count']}")
        with open(data["filename"], "r+") as file1:
            text = file1.read()
        len_doc = len(text.split(" "))
        print(f"Total length of document = {len_doc}")
        map_chunk_size = int(len_doc)/int(data["mapper_task_count"])
        print(f"Map Chunk Size - {map_chunk_size}")
        data["map_chunk_size"] = map_chunk_size
        print("Total length of document = ")
        response = json.loads(task_set_up(data, "map"))
        if response["status"] == "success":
            response = json.loads(task_set_up(data, "reduce"))
        return "True"
    except Exception as e:
        print(e)
        return str(e)

@app.route('/get_client_details', methods=['POST']) 
def get_client_details(): 
    """Function check and save port details from client and 
        update the response to respective client."""

    try:
        details = json.loads(request.get_json())
        print(f"\nI am active worker. My port details are - {details}")
        if details["port"] in client_port_details:
            print(f"Same port details are not available. Please update the port details")
            return json.dumps({"status":"not received", 
                "message":"Please change port number", "code":"302"})
        else:
            client_port_details.append(details["port"])
            print(f"Client port details are updated at server- {details['port']}")
            return json.dumps({"status":"received", "message":"success", "code":"200"})
    except Exception as e:
        print("Error in get_client_details api -", e)
        return json.dumps({"status":"not received"})

# driver function 
if __name__ == '__main__':  
    app.run(debug = True, port=8000, use_reloader=False)
