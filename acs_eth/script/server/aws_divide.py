import boto3
import json

regions = ["us-east-2"]
access_key = ""
secret_key = ""

total = {}
total["nodes"] = []
clients = {}
clients["nodes"] = []
server_id = 0
client_id = 0
for region in regions:
    print("region:",region)
    ec2 = boto3.client('ec2',aws_access_key_id=access_key, aws_secret_access_key=secret_key,region_name=region)
    Tags = [{'Key': 'Name', 'Value': 'Free'}]
    Filter = [
        {
            'Name': 'key-name',
            'Values': [
                'eth_parallel',
            ]
        }
    ]
    response = ec2.describe_instances(Filters=Filter)
    instances = []
    for i in range(len(response['Reservations'])):
        instances += response['Reservations'][i]['Instances']
    print(len(instances))

    # --------------------------------
    # --------nodes-----------------
    for i in range(len(instances)):
        status = instances[i]['State']['Name']
        if status != "running":
            continue
        if instances[i]['InstanceType'] == 'm6i.4xlarge':
            instance = {}
            instance['Id'] = client_id
            client_id += 1
            instance['InstanceId'] = instances[i]['InstanceId']
            instance['InstanceType'] = instances[i]['InstanceType']
            instance['PublicIpAddress'] = instances[i]['PublicIpAddress']
            instance['PrivateIpAddress'] = instances[i]['PrivateIpAddress']
            instance['ServerURL'] = "http://" + instances[i]['PublicIpAddress'] +":6000/client"
            clients['nodes'].append(instance)
        
        if instances[i]['InstanceType'] == 't3.2xlarge':
            instance = {}
            instance['Id'] = server_id
            server_id += 1
            instance['InstanceId'] = instances[i]['InstanceId']
            instance['InstanceType'] = instances[i]['InstanceType']
            instance['PublicIpAddress'] = instances[i]['PublicIpAddress']
            instance['PrivateIpAddress'] = instances[i]['PrivateIpAddress']
            instance['ServerURL'] = "http://" + instances[i]['PublicIpAddress'] +":6000/client"
            total['nodes'].append(instance)

print("----- begin to load----")
file = "./nodes.json"
with open(file,"w") as f:
    json.dump(total,f)
print("----- load success ----")

for item in range(len(total['nodes'])):
    clients['nodes'][item]['ServerURL'] = "http://" + total['nodes'][item]['PublicIpAddress'] +":6000/client"

print("----- begin to load ----")
file = "../client/clients.json"
with open(file,"w") as f:
    json.dump(clients,f)
print("----- load success ----")

print("----- begin to load ----")
file = "../execlayer/clients.json"
with open(file,"w") as f:
    json.dump(clients,f)
print("----- load success ----")
