# map-reduce-distributed-api-service
The project consist of Python codebase which is able to create client-server comminucation using FLASK REST API's. User can instantiate multiple instance of client which are creating distributed network to solve word count problem using map-reduce. 

### Architecture
Multiple clients can connect with server which is running on port 8000. When server received request from user to solve word count problem for big text file, server divide's the map task between available clients. Clients run separately and create intermediate files in 'intermediate' folder, following naming convention as <mr-map_task_id-bucket_id>. When all mapper function executes, server distibutes the reducer task to available clients. Reducer function creates output files in folder 'out', following naming convention as <out-reducer_task_id>. The request metadata is present in the file **request.py**.

### Installation
Require Python version greatter than equal to 3.11.3. 
The list of required packages is given in file requirements.txt. To install packages use command:
  **pip install -r requirements.txt**

### Instructions
The server.py file consist of server architecture. Run it using command:**python server.py**. 
The default port for client is 5000, for other clients, code can select max random 10 ports from 5001-5010. We can instantiate different clients using command-line. The command for client instantiation is: **python client.py**.
User can send the request by using command: **python request.py**.
