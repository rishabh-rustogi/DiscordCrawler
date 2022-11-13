import functions_framework
import requests
import threading
import json

URL = "https://testing-1-zpwidwaj2q-uc.a.run.app/"

channelIDsOLD = ["123456789", "987654321", "123987456", "456789123", "789456123",
                "258963147", "741852963", "369852147", "147852369", "963852741"]

channelIDsNEW = ["123", "234", "345", "456", "567", "678", "789", "890", "901", "012",
                    "1234", "2345", "3456", "4567", "5678", "6789", "7890", "8901", "9012", "0123"]

def request_task(url, json=None):
    requests.get(url, json)

def fire_and_forget(url, json):
    # Send the request using get
    threading.Thread(target=request_task, args=(url, json)).start()

def getNextOldChannelID(currentID=None):
    if currentID is None:
        return channelIDsOLD[0]
    for i in range(len(channelIDsOLD)):
        if channelIDsOLD[i] == currentID:
            if i == len(channelIDsOLD) - 1:
                return None
            else:
                return channelIDsOLD[i + 1]
    
    return None

def getNextNewChannelID(currentID=None):
    if currentID is None:
        return channelIDsNEW[0]
    for i in range(len(channelIDsNEW)):
        if channelIDsNEW[i] == currentID:
            if i == len(channelIDsNEW) - 1:
                return None
            else:
                return channelIDsNEW[i + 1]
    return None

def update():
    # Create a new request object
    body = {
        "mode": "extractOld",
    }

    body["channelID"] = getNextOldChannelID()

    if body["channelID"] is None:
        return "Done"

    # Send the request
    fire_and_forget(URL, body)
    return 'Working'

def extractOld(request_json):
    body = {
        "mode": "extractOld",
    }

    channelID = getNextOldChannelID(request_json["channelID"])
    if channelID is None:
        body["mode"] = "extractNew"
        channelID = getNextNewChannelID()
    
    body["channelID"] = channelID

    # Send the request using get
    fire_and_forget(URL, body)
    return 'Working'

def extractNew(request_json):

    body = {
        "mode": "extractNew",
    }

    channelID = getNextNewChannelID(request_json["channelID"])
    if channelID is None:
        return "Done"
    
    body["channelID"] = channelID

    # Send the request
    fire_and_forget(URL, body)
    return 'Working'
    

@functions_framework.http
def handler(request):
    """HTTP Cloud Function.
    Args:
        request (flask.Request): The request object.
        <https://flask.palletsprojects.com/en/1.1.x/api/#incoming-request-data>
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
        <https://flask.palletsprojects.com/en/1.1.x/api/#flask.make_response>.
    """

    request_json = request.get_json(silent=True)

    if not request_json:
        request_json = {}

    if request.args and 'mode' in request.args:
        request_json['mode'] = request.args.get('mode')

    if request.args and 'channelID' in request.args:
        request_json['channelID'] = request.args.get('channelID')

    if 'mode' in request_json:
        mode = request_json['mode']
    else:
        return 'Invalid request (no mode)'
    
    entry = dict(
        severity="NOTICE",
        message="request_json: " + str(request_json),
        component="arbitrary-property",
    )

    print(json.dumps(entry))

    if mode == 'update':
        return update()
    elif mode == 'extractOld':
        return extractOld(request_json)
    elif mode == 'extractNew':
        return extractNew(request_json)
    else:
        return 'Invalid request'