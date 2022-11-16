import functions_framework
from datetime import datetime
import time
import requests
import os
import json
import glob

'''
Importing GCP libraries
'''
from google.cloud import storage

###############################################
#####               CONSTANTS             ##### 
###############################################
BASE_URL = 'https://discord.com/api/v9/'
GLOBAL_RATE_LIMIT_PER_SEC = 50
DATA_FOLDER = 'data/{}/'.format(datetime.now().strftime('%Y-%m-%d'))
DATA_FOLDER_MEDIA = DATA_FOLDER + 'media/'
BUCKET_NAME = 'discord-chat-data'

# Define global variables
requests_per_second = 0
start_time = time.time_ns()

# URL endpoints for Discord API
urls = {
    'guilds': 'users/@me/guilds',
    'channels': 'guilds/{}/channels',
    'messages': 'channels/{}/messages'
}

# Endpoint parameters
url_params = {
    'guilds': {},
    'channels': {},
    'messages': {'limit': 100,
                 'before': None}
}

'''
Implements Global Rate Limit with while reuesting URL response
@params url: URL to request
@params token: Discord token
@params params: Parameters to send with the request
@returns: Response from the request
'''
def requestURLResponse(url, token, params):

    # Import global variables
    global requests_per_second
    global start_time
    requests_per_second += 1

    # Check if global rate limit is reached
    # If so, wait until the next second
    while requests_per_second > GLOBAL_RATE_LIMIT_PER_SEC and time.time_ns() - start_time < 1e9:
        continue

    # If the next second has started, reset the counter
    if time.time_ns() - start_time >= 1e9:
        requests_per_second = 0
        start_time = time.time_ns()
    
    # Calculate the average RPS and print it
    print("RPS: {}".format(requests_per_second), end=f"\r")

    # Request the URL with the given parameters and headers
    headers = {'Authorization': token}
    response = requests.get(url, headers=headers, params=params)

    # Check if somehow the global rate limit was exceeded, if so, wait until the retry-after time
    while response.status_code == 429:
        time.sleep(int(response.json()['retry_after']))
        requests_per_second = 0
        start_time = time.time_ns()

        # Request the URL with the given parameters and headers again
        response = requests.get(url, headers=headers, params=params)
    
    # Check if the request was not successful, if so, log the error, upload the logs and exit
    if response.status_code != 200:
        # uploadLogs()
        exit(1)

    # Return the response
    return response.json()


'''
Uploads a Folder recursilvely to GCP Storage
@params bucketName: Name of the bucket to upload the folder to
@params prefix: Prefix of the folder to upload
@params source: Source of the folder to upload
'''
def uploadFolder(bucketName, prefix, source):
    uploadFolderRecursively(bucketName, prefix, source)


'''
Uploads a File to GCP Storage with level deeper than 1
@params bucketName: Name of the bucket to upload the file to
@params sourceFile: Source of the file to upload
@params destinationFile: Destination of the file to upload
'''
def uploadFolderRecursively(bucketName, prefix, source):
    relative_paths = glob.glob(source + '**', recursive=True)
    for relative_path in relative_paths:
        if os.path.isfile(relative_path):
            uploadFile(bucketName, relative_path, prefix + relative_path.replace(source, ''))


'''
Upload a single file to GCP Storage
@params bucketName: Name of the bucket to upload the file to
@params sourceFile: Source of the file to upload
@params destinationFile: Destination of the file to upload
'''
def uploadFile(bucketName, sourceFile, destinationFile):
    
    # Create the storage client
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucketName)
    blob = bucket.blob(destinationFile)

    # Upload the file
    blob.upload_from_filename(sourceFile)

'''
Create a folder if it doesn't exist
@params folderName: Name of the folder to create
@requires: OS Permissions to create the folder
'''
def createFolder(folderName):
    # Check if the folder exists and create it if it doesn't
    if not os.path.exists(folderName):
        os.makedirs(folderName)

'''
Dump json data to a file
@params path: Path to the file to dump the data to
@ param json_data: JSON data to dump to the file
'''
def writeFile(path, json_data):
    file_route = path.split('/')
    createFolder('/'.join(file_route[:-1]))
    try:
        with open(path, 'w') as f:
            json.dump(json_data, 
                        f,
                        indent=4, 
                        separators=(',', ': '))
    except Exception as e:
        # uploadLogs()
        exit(1)

'''
Convert a snowflake string to a datetime object
@param snowflake: Snowflake string
@returns: Datetime object
'''
def twitterSnowflakeToDatetime(snowflake):
    return datetime.fromtimestamp(((int(snowflake) >> 22) + 1420070400000) / 1000)

'''
Get guild and channel data from Discord API
@params auth_token: Discord token
@params channel_id: Channel ID to get the messages from
'''
def getGuildAndChannelData(auth_token, channel_id):
    # Get guild data
    guilds = requestURLResponse(BASE_URL + urls['guilds'], auth_token, url_params['guilds'])

    for guild in guilds:
        # Get channel data
        channels = requestURLResponse(BASE_URL + urls['channels'].format(guild['id']), auth_token, url_params['channels'])
        for channel in channels:
            if channel['id'] == channel_id:
                return (guild, channel)
'''
Download messages from a channel
@param auth_token: Discord auth token
@param channel_id: Channel ID to download messages from
@param last_message_id: Last message ID to download messages from
'''
def downloadMessages(auth_token, channel_id, last_message_id):

    # Get guild and channel data
    guild, channel = getGuildAndChannelData(auth_token, channel_id)

    # Check if last message ID is not None
    if last_message_id is not None:
        last_message_id_timestamp = twitterSnowflakeToDatetime(last_message_id)
    else:
        last_message_id_timestamp = datetime.fromtimestamp(0)

    # Create a new JSON object for the channel
    messages_JSON = {
        'guild': guild,
        'channel': channel,
        "messages": []
    }

    # Request the latest messages from the channel
    messages = requestURLResponse(BASE_URL + urls['messages'].format(channel_id), 
                                    auth_token, 
                                    {'limit': 1})

    # If the latest message timestamp is <= than the last message processed, ignore the channel
    current_timestamp = twitterSnowflakeToDatetime(messages[0]['id'])
    if current_timestamp <= last_message_id_timestamp:
        return "IGNORE"
    
    # SKIP ATTACHMENTS FOR NOW

    # Update the JSON object with the messages
    messages_JSON['messages'] = messages

    # Set the BEFORE param to the timestamp of the latest message processed
    last_message_processed = messages_JSON['messages'][0]['id']

    # Also, save the latest message ID to store in the config file
    latest_message_processed = messages_JSON['messages'][-1]['id']

    check = True
    while (check):

        # Get the 'message' endpoint params and set the before param to the last message processed
        params = url_params['messages'].copy()
        params['before'] = last_message_processed
        
        # Request the messages before the BEFORE param
        messages = requestURLResponse(BASE_URL + urls['messages'].format(channel_id), 
                                    auth_token, 
                                    params)
        
        # Loop through every message and check if the timestamp is <= than the last message processed timestamp
        temporary_messages_holder = []
        for message in messages:

            current_timestamp = twitterSnowflakeToDatetime(message['id'])
            if current_timestamp <= last_message_id_timestamp:
                # If a message is found smaller than the last message processed timestamp, break both 
                # the loops
                check = False
                break

            # SKIP ATTACHMENTS FOR NOW
    
            temporary_messages_holder.append(message)
        
        # Also, check if the messages list is empty, if it is, break the loop
        if len(temporary_messages_holder) == 0:
            check = False
            break
        else:
            messages = temporary_messages_holder
        
        # Update the BEFORE param to the last message processed
        last_message_processed = messages[-1]['id']

        # Update the JSON object with the messages
        messages_JSON['messages'] += messages

    # Reverse the messages in the JSON object so that the messages are in chronological order
    messages_JSON['messages'].reverse()

    # Write the JSON object to a file
    writeFile(DATA_FOLDER + '{}.json'.format(channel_id), messages_JSON)

    # Upload the file to GCP Storage
    uploadFolder(BUCKET_NAME, DATA_FOLDER, DATA_FOLDER)

    return "SUCCESS"

'''
Handle the download of messages
@request: HTTP request

TODO: 
    - Add media download (script ready, just need to add it to the downloadMessages function)
    - Update last_message_id in firestore after the download
    - Stop downloading message at 50 mins mark 
'''
@functions_framework.http
def handler(request):
    """HTTP Cloud Function.
    Args:
        request (flask.Request): The request object.
        <https://flask.palletsprojects.com/en/1.1.x/api/#incoming-request-data>
    Returns:
        String: The response text, or any set of values that can be turned into a
        Response object using `make_response`
        <http://flask.palletsprojects.com/en/1.1.x/api/#flask.Flask.make_response>.
    """

    # Get the request body
    request_json = request.get_json(silent=True)

    # Define the fields to get from the request body
    fields = ["authorizationToken", "channelID", "lastMessageID"]
    missingFields = []

    # Check if the request body has all the fields
    if not request_json:
        request_json = {}

    for field in fields:
        if request.args and field in request.args:
            request_json[field] = request.args.get(field)
        else:
            missingFields.append(field)

    # In case 'lastMessageID' is not in the request body, set it to None
    # and remove it from the missing fields list
    if "lastMessageID" in missingFields:
        missingFields.remove("lastMessageID")
        request_json["lastMessageID"] = None
    
    # If there are missing fields, return an error
    if len(missingFields) > 0:
        return "Required fields missing: " + ", ".join(missingFields)

    # returnt the downloadMessages function response
    return downloadMessages(request_json['authorizationToken'], 
                            request_json['channelID'], 
                            request_json['lastMessageID'])
    
