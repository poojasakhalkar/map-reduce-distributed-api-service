from flask import Flask, jsonify, request 
import glob, datetime, random, requests, socket, json, string, time

# creating a Flask app 
app = Flask(__name__) 
   
def mapper(data):
    """Function Map each word with count -1 in intermediate folder.
       The file name = mr-{MAPPER TASK ID}-{MODULAS OF (First letter of the word, MAPPER TASK ID)}
    """

    print(f"Mapper Task started for mapper task Id {data['mapper_task_id']}")
    with open(data["filename"], "r+") as file1:
        text = file1.read()
    words = text.split()
    if  data['mapper_task_id'] == 0:
        words = words[0:data["map_chunk_size"]]
    else:
        start_value = int(data["map_chunk_size"])*int(data['mapper_task_id'])
        stop_value = int(start_value) + int(data["map_chunk_size"])
        print("Start value ",start_value," - ", stop_value)
        words = words[start_value:stop_value]
    for word in words:
        filename = "mr-"+str(data["mapper_task_id"])+"-"+str(ord(word[0].upper())%int(data["mapper_task_count"]))+".txt"
        word = word.translate(str.maketrans('', '',
                                    string.punctuation))
        with open("intermediate/"+filename, "a") as f:
            f.write("%s - 1\n" % word.lower())
    return "True"

def reducer(data):
    """ Function get file from BUCKET ID = REDUCER TASK ID.
        Count the occurance of the word and 
        save the details in out folder with file name as out-{REDUCER TASK ID}.
    """

    print(f"Reducer Task started for Reducer task Id {data['reducer_task_id']}")
    list_of_files = glob.glob("intermediate/*.txt")
    for file in list_of_files:
        final_dict = {}
        if (file.split(".")[0]).split("-")[-1] == data['reducer_task_id']:
            with open(file, "r+") as file1:
                list_words = file1.read()
            list_words = list_words.split("\n")
            for i in list_words:
                if i in final_dict.keys():
                    final_dict[i] = final_dict[i]+1
                else:
                    final_dict[i] = 1
            filename = "out-"+str(data['reducer_task_id'])
            with open("out/"+filename, "w+") as f:
                for key1, value1 in final_dict.items():
                    f.write(f'{key1.split("-")[0]} - {value1}\n')
            return "True"
        

@app.route('/mapper_reducer', methods = ['POST']) 
def home(): 
    """Client receives the metadata about the task and start the processing accordingly."""

    print("Request received", datetime.datetime.now())
    data = json.loads(request.get_json())
    if data["task"] == "map":
        mapper(data)
    elif data["task"] == "reduce":
        reducer(data)
    time.sleep(20)
    print("request processed successfully. ")
    return "200"
    
# driver function 
if __name__ == '__main__': 
    """ Client Initaite connection with server. Update port Details to Server. 
        Server check port details and if not available, update client to change the port details. 
        Client can assign itself any port details from 5000-5010 randomly. 
    """

    print("Initiated Client.")
    response_data = {}
    response_data["status"] = None
    client_port = 5000    
    while response_data["status"] != "received":
        try:
            api_url = "http://127.0.0.1:8000/get_client_details"
            response = requests.post(api_url, json=json.dumps({"port":client_port}))
            response_data = response.json()
            print(f"You are able to connect to server successfully. Response from server is - {response_data}")
            if response_data["code"] == "302":
                print(f"Same port details are not available. Please update the port details")
                client_port = random.randrange(5001, 5010)
                time.sleep(2)
                pass
        except Exception as e:
            print("Error in checking the server availability -", e)
            continue
    app.run(debug = True, port=client_port, use_reloader=False)
