import argparse
from datetime import datetime
import os
import logging
import requests
import json
import time
import glob
import shutil

'''
Importing GCP libraries
'''
# from google.cloud import storage
# from google.cloud import pubsub_v1

''' 
Download and Upload functions
'''

###############################################
#####               CONSTANTS             ##### 
###############################################
BASE_URL = 'https://discord.com/api/v9/'
GLOBAL_RATE_LIMIT_PER_SEC = 50
BASE_USER_SERVER_CHANNEL = {}
DATA_FOLDER = 'data/{}/'.format(datetime.now().strftime('%Y-%m-%d'))
DATA_FOLDER_MEDIA = DATA_FOLDER + 'media/'
BUCKET_NAME = 'discordchatexporter'

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
        logging.error("Global Rate Limit for invalid requests exceeded")
        logging.warning('[Pausing for {}s] | Requested URL: {}'.format(
            response.json()['retry_after'], 
            url))
        time.sleep(int(response.json()['retry_after']))
        requests_per_second = 0
        start_time = time.time_ns()

        # Request the URL with the given parameters and headers again
        response = requests.get(url, headers=headers, params=params)
    
    # Check if the request was not successful, if so, log the error, upload the logs and exit
    if response.status_code != 200:
        logging.error('Error while requesting URL: {}'.format(response.json()))
        # uploadLogs()
        exit(1)

    # Return the response
    return response.json()


'''
Download Folder from GCP Storage
@param bucketName: Name of the bucket to download the folder from
@param prefix: Prefix of the folder to download
@param destination: Destination to download the folder to
'''
# def downloadFolder(bucketName, prefix, destination):
#     logging.info('Downloading folder from GCP Storage (Bucket: {}, Prefix: {}, Destination: {})'.format(
#         bucketName, 
#         prefix, 
#         destination))

#     # Create the storage client
#     storage_client = storage.Client()
#     bucket = storage_client.bucket(bucketName)
#     blobs = bucket.list_blobs(prefix=prefix)
    
#     # Create the destination folder if it doesn't exist
#     createFolder(destination)

#     # Download all the files in the folder
#     for blob in blobs:
#         blob.download_to_filename(destination + blob.name.split('/')[-1])


'''
Uploads a Folder recursilvely to GCP Storage
@params bucketName: Name of the bucket to upload the folder to
@params prefix: Prefix of the folder to upload
@params source: Source of the folder to upload
'''
# def uploadFolder(bucketName, prefix, source):
#     logging.info('Uploading folder to GCP Storage (Bucket: {}, Prefix: {}, Source: {})'.format(
#         bucketName, 
#         prefix, 
#         source))

#     uploadFolderRecursively(bucketName, prefix, source)


'''
Uploads a File to GCP Storage with level deeper than 1
@params bucketName: Name of the bucket to upload the file to
@params sourceFile: Source of the file to upload
@params destinationFile: Destination of the file to upload
'''
# def uploadFolderRecursively(bucketName, prefix, source):
#     relative_paths = glob.glob(source + '**', recursive=True)
#     for relative_path in relative_paths:
#         if os.path.isfile(relative_path):
#             uploadFile(bucketName, relative_path, prefix + relative_path.replace(source, ''))


'''
Upload a single file to GCP Storage
@params bucketName: Name of the bucket to upload the file to
@params sourceFile: Source of the file to upload
@params destinationFile: Destination of the file to upload
'''
# def uploadFile(bucketName, sourceFile, destinationFile):
#     logging.info('Uploading file to GCP Storage (Bucket: {}, Source: {}, Destination: {})'.format(
#         bucketName, 
#         sourceFile, 
#         destinationFile))

#     # Create the storage client
#     storage_client = storage.Client()
#     bucket = storage_client.bucket(bucketName)
#     blob = bucket.blob(destinationFile)

#     # Upload the file
#     blob.upload_from_filename(sourceFile)


'''
Create a folder if it doesn't exist
@params folderName: Name of the folder to create
@requires: OS Permissions to create the folder
'''
def createFolder(folderName):
    # Check if the folder exists and create it if it doesn't
    if not os.path.exists(folderName):
        logging.info('Creating folder {}'.format(folderName))
        os.makedirs(folderName)

'''
Delete a folder and all its contents recursively
@params folderName: Name of the folder to delete
@requires: OS Permissions to delete the folder
'''
def deleteFolder(folderName):
    # Check if the folder exists and delete it if it does
    if os.path.exists(folderName):
        logging.info('Deleting folder {}'.format(folderName))
        shutil.rmtree(folderName)


'''
Parse the arguments passed to the script
@returns: Parsed arguments
'''
def createCMDParser():
    # Create the parser 
    parser = argparse.ArgumentParser(description='Dicord Message Extractor')

    # Add the arguments
    parser.add_argument('--mode', type=str, default='help', help='Mode to run the program in')
    return parser


# '''
# Publishes messages to a Pub/Sub topic
# @params topicName: Name of the Pub/Sub topic to publish to
# @params messages: Messages to publish to the topic
# '''
# def publishToPubSub(topicName, message):
#     logging.info('Publishing message to Pub/Sub (Topic: {})'.format(topicName))
#     publisher = pubsub_v1.PublisherClient()
#     topic_path = publisher.topic_path('discord-message-extractor', topicName)
#     future = publisher.publish(topic_path, message.encode('utf-8'))
#     logging.info('({})Message published: {}'.format(future.result(), message))


'''
Reads the config file as a JSON object and returns it
@params path: Path to the config file
@returns: Config file as a JSON object
'''
def readConfigAsJSON(path='configs/user_token.json'):
    logging.info('Reading {} config'.format(path))
    try:
        with open(path, 'r') as f:
            return json.load(f)
    except Exception as e:
        logging.error('Error while reading config: {}'.format(e))
        # uploadLogs()
        exit(1)


'''
Dump json data to a file
@params path: Path to the file to dump the data to
@ param json_data: JSON data to dump to the file
'''
def writeFile(path, json_data):
    logging.info('Writing file: {}'.format(path))
    file_route = path.split('/')
    createFolder('/'.join(file_route[:-1]))
    try:
        with open(path, 'w') as f:
            json.dump(json_data, 
                        f,
                        indent=4, 
                        separators=(',', ': '))
    except Exception as e:
        logging.error('Error while writing file: {}'.format(e))  
        # uploadLogs()
        exit(1)


'''
Update the config file with the latest data

STEPS:
1. Download the config files from GCP Storage
2. Loop though every user in the config file
    - Loop through every guild
        - If guild not in config file, add it
    - Loop through every channel in the guild
    - If a appropiate channel is found (i.e. it has a last_message_id)
        - If channel already in config: Set channel status as 'processing'
        - If channel not in config: Add channel to confif and set status as 'new'
3. For every channel in the config file
    - If channel status is still 'processing', set status as 'inactive'
    - If channel 'latest_message_id' is null, set status as 'inactive'
3. Write the updated JSON to the config file
4. Upload the config file to GCP Storage

TODO: Finalize the config structure
TODO: What exactly is an appropiate channel? (For now a channel which has last_message_id)
TODO: Upload the config file to GCP Storage
TODO: At the edn of the script trigger a pubsub event to start the script again to 
      extract messages from the channels with status 'processing'
'''
def updateConfigs():
    logging.info('Updating configs')

    # Download the config files from GCP Storage
    # downloadFolder(BUCKET_NAME, 'configs/', 'configs/')

    # Read the config files
    user_token = readConfigAsJSON()
    user_server_channel = readConfigAsJSON('configs/user_server_channel_DO_NOT_EDIT.json')

    # Loop through every user in the user config file
    for user in user_token:

        # If user not in user_server_channel, add them
        if user not in user_server_channel:
            user_server_channel[user] = {}

        # Read the user's guilds
        guilds = requestURLResponse(BASE_URL + urls['guilds'], 
                                    user_token[user]['token'], 
                                    url_params['guilds'])

        # Loop through every guild
        for guild in guilds:

            # Filter out guild for testing
            if guild['id'] not in ['1015029498317651979']:#, '894349669499568148']:
                continue

            # If guild not in user_server_channel, add it
            if guild['id'] not in user_server_channel[user]:
                user_server_channel[user][guild['id']] = {}

            # Read the user's channels
            channels = requestURLResponse(BASE_URL + urls['channels'].format(guild['id']), 
                                            user_token[user]['token'], 
                                            url_params['channels'])

            # Loop through every channel
            for channel in channels:
                if "last_message_id" not in channel or channel['last_message_id'] is None:
                    # If channel is not aa appropiate channel, skip it 
                    continue
                elif channel['id'] not in user_server_channel[user][guild['id']]:
                    # If channel not in user_server_channel, add it
                    user_server_channel[user][guild['id']][channel['id']] = {
                        "name": channel['name'],
                        "started": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "last_processed": None,
                        "latest_message_id": channel['last_message_id'], 
                        "status": "new"
                    }
                else:
                    # If channel already in user_server_channel, set status as 'processing'
                    # and update the latest_message_id
                    user_server_channel[user][guild['id']][channel['id']]['status'] = "processing"
                    user_server_channel[user][guild['id']][channel['id']]['latest_message_id'] = channel['last_message_id']
    
    # Set the status of every channel in the config file to 'inactive' if it is still 'processing'
    # or if the latest_message_id is null
    for user in user_server_channel:
        for guild in user_server_channel[user]:
            for channel in user_server_channel[user][guild]:
                if user_server_channel[user][guild][channel]['last_processed'] == None:
                    user_server_channel[user][guild][channel]['status'] = "new"
                if user_server_channel[user][guild][channel]['status'] == "processed":
                    user_server_channel[user][guild][channel]['status'] = "inactive"
                if user_server_channel[user][guild][channel]['latest_message_id'] is None:
                    user_server_channel[user][guild][channel]['status'] = "inactive"

    # Write the updated config file
    writeFile('configs/user_server_channel_DO_NOT_EDIT.json', user_server_channel)

    # Upload the updated config file to GCP Storage
    # uploadFolder(BUCKET_NAME, 'configs/', 'configs/')
    logging.info('Configs updated')


'''
Create base JSON structure for a channel
@param user_id: User ID
@param guild_id: Guild ID
@param channel_id: Channel ID
@param channel_name: Channel name
@returns: Base JSON structure for a channel
'''
def createBaseMessageJSON(user_id, guild_id, channel_id, channel_name):
    return {
        "user_id": user_id,
        "guild_id": guild_id,
        "channel_id": channel_id,
        "channel_name": channel_name,
        "messages": []
    }

'''
Convert a snowflake string to a datetime object
@param snowflake: Snowflake string
@returns: Datetime object
'''
def twitterSnowflakeToDatetime(snowflake):
    return datetime.fromtimestamp(((int(snowflake) >> 22) + 1420070400000) / 1000)

'''
Download media from a url and save it to a file
Download only if file size is less than 8MB
@param url: URL of the media
@param file_path: Path to save the media to

TODO: Handle files larger than 8MB
'''
def downloadContent(url, path):
    try:
        fileRoute = url.split('/')
        path += "/".join(fileRoute[len(fileRoute) - 3:len(fileRoute) - 2])
        fileName = "/" + url.split('/')[-1]
        createFolder(path)
        logging.info('Downloading file: {}'.format(url))
        # Get the file size and download the file only if it is smaller than 8MB
        fileSize = int(requests.head(url).headers['Content-Length'])
        if fileSize < 8388608:
            with open(path + fileName, 'wb') as f:
                f.write(requests.get(url).content)
        else:
            logging.info('File too large ({}): {}'.format(fileSize, url))
    except Exception as e:
        logging.error('Error while downloading file: {}'.format(e))  
        # uploadLogs()
        exit(1)

'''
Download the messages for all the channels in the config file 
which have a status of 'processing'

STEPS:
1. Read the config file
2. Loop through every user in the user_server_channel config file
3. Loop through every guild
4. Loop through every channel and check if the status is 'processing'
    - Get the last message timestamp from the config file
    - Set BEFORE param to the latest message timestamp
    - Request the latest messages from the channel
        - If the latest message timestamp is <= than the last message processed 
        timestamp from the config file, ignore the channel
        - Else, create a new JSON object for the channel and write the messages to it
        - Update the BEFORE param to the latest processed message timestamp
    - Do this until a message is found which has a timestamp <= than the last message 
      processed timestamp
        - Request the messages before the BEFORE param
        - Loop through every message and check if the timestamp is <= than the last
        message processed timestamp
            - If a message is found, break both the loops
            - Else, write the messages to the JSON object and update the BEFORE param
    - Reverse the messages in the JSON object so that the messages are in chronological order
    - Update the config file
        - Update the last_processed timestamp in the config file to the latest message timestamp
        - Update the status to 'processed'
    - Write the JSON object to a file
5. Write the updated config file
6. Upload the update file to GCP Storage
    - Upload the config folder to GCP Storage
    - Upload the Data folder to GCP Storage

TODO: Upload the config file to GCP Storage
TODO: Introduce global time limit for the script
        - If the script has been running for more than 10 minutes (yet to be decided), trigger a new 
        pubsub event to start the script again for the channels which still have a status of 
        'processing'
        - Else, the script will continue to run until all the channels have a status of 'processed'
        - Finally trigger a pubsub event to start the script but now for the channels which have a 
        status of 'new'
'''
def extractMessageFromExploredChannels():
    logging.info('Downloading configs')

    # Download the config file from GCP Storage
    # downloadFolder(BUCKET_NAME, 'configs/', 'configs/')

    # Read the config file
    user_token = readConfigAsJSON()
    user_server_channel = readConfigAsJSON('configs/user_server_channel_DO_NOT_EDIT.json')

    # Loop through every user in the user_server_channel config file
    for user in user_server_channel:
        # Loop through every guild
        for guild in user_server_channel[user]:
            # Loop through every channel and check if the status is 'processing'
            for channel in user_server_channel[user][guild]:
                # Check if the status is 'processing'
                if user_server_channel[user][guild][channel]['status'] == "processing":
                    logging.info('Extracting messages from channel (User: {}, Guild: {}, Channel: {})'.format(
                        user, 
                        guild, 
                        channel))

                    # Get the last message timestamp from the config file
                    last_proccessed_timestamp = twitterSnowflakeToDatetime(user_server_channel[user][guild][channel]['last_processed'])
                    
                    # Create a new JSON object for the channel
                    messages_JSON = createBaseMessageJSON(user, guild, channel, user_server_channel[user][guild][channel]['name'])
                    
                    # Request the latest messages from the channel
                    messages = requestURLResponse(BASE_URL + urls['messages'].format(channel), 
                                                    user_token[user]['token'], 
                                                    {'limit': 1})
                    
                    # If the latest message timestamp is <= than the last message processed, ignore the channel
                    current_timestamp = twitterSnowflakeToDatetime(messages[0]['id'])
                    if current_timestamp <= last_proccessed_timestamp:
                        user_server_channel[user][guild][channel]['status'] = "processed"
                        continue

                    if "attachments" in messages[0]:
                        for attachment in messages[0]["attachments"]:
                            if 'url' in attachment:
                                downloadContent(attachment['url'], DATA_FOLDER_MEDIA)

                    # Update the JSON object with the messages
                    messages_JSON['messages'] = messages

                    # Set the BEFORE param to the timestamp of the latest message processed
                    last_message_processed = messages_JSON['messages'][0]['id']
                    
                    print("Processing channel: {}".format(user_server_channel[user][guild][channel]['name']))

                    # Also, save the latest message ID to store in the config file
                    latest_message_processed = messages_JSON['messages'][-1]['id']
                    
                    check = True
                    while (check):

                        # Get the 'message' endpoint params and set the before param to the last message processed
                        params = url_params['messages'].copy()
                        params['before'] = last_message_processed
                        
                        # Request the messages before the BEFORE param
                        messages = requestURLResponse(BASE_URL + urls['messages'].format(channel), 
                                                        user_token[user]['token'], 
                                                        params)
                        
                        # Loop through every message and check if the timestamp is <= than the last message processed timestamp
                        temporary_messages_holder = []
                        for message in messages:

                            current_timestamp = datetime.fromtimestamp(((int(message['id']) >> 22) + 1420070400000) / 1000)
                            if current_timestamp <= last_proccessed_timestamp:
                                # If a message is found smaller than the last message processed timestamp, break both 
                                # the loops
                                check = False
                                break
                            
                            if "attachments" in message:
                                for attachment in message["attachments"]:
                                    if 'url' in attachment:
                                        downloadContent(attachment['url'], DATA_FOLDER_MEDIA)

                            # Else, write the messages to the JSON object and update the BEFORE param
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

                    # Update the config file
                    user_server_channel[user][guild][channel]['last_processed'] = latest_message_processed
                    user_server_channel[user][guild][channel]['status'] = "processed"

                    # Write the JSON object to a file
                    writeFile(DATA_FOLDER + '{}.json'.format(channel), messages_JSON)
    
    # Write the updated config file
    writeFile('configs/user_server_channel_DO_NOT_EDIT.json', user_server_channel)

    # Upload the updated config files to GCP Storage
    # logging.info('Uploading updated  configs')
    # uploadFolder(BUCKET_NAME, 'configs/', 'configs/')

    # Upload the Data folder to GCP Storage
    # logging.info('Uploading extracted messages')
    # uploadFolder(BUCKET_NAME, DATA_FOLDER, DATA_FOLDER)

'''
Download the messages for all the channels in the config file 
which have a status of 'new'

STEPS:
1. Read the config file
2. Loop through every user in the user_server_channel config file
    - Loop through every guild
    - Loop through every channel and check if the status is 'new'
        - Create a new JSON object for the channel
        - Request the latest messages from the channel
        - Update the JSON object with the messages
        - Set the BEFORE param to the timestamp of the latest message processed
        - Also, save the latest message ID to store in the config file
        - Do the following until no more messages are found
            - Get the 'message' endpoint params and set the before param to the last message processed
            - Request the messages before the BEFORE param
            - Add the messages to the JSON object and update the BEFORE param
        - Reverse the messages in the JSON object so that the messages are in chronological order
        - Update the config file
            - Set the status to 'processed'
            - Set the last_processed to the latest message processed
        - Write the JSON object to a file
3. Write the updated config file
4. Upload the update file to GCP Storage
5. Upload the Data folder to GCP Storage

TODO: Upload the files to GCP Storage
TODO: To avoid global time limit for the script
        - Only process a single channel
        - If more 'new' channels are left, trigger a pubsub event to start the script again 
        - Check if the script times out before even a single channel is processed
            - If it does, then trigger a pubsub event to start the script again to process the 
            same channel partially (TODO: even mentioned in the main driver script)
'''
def extractMessageFromNewChannels():
    logging.info('Downloading configs')

    # Download the config files from GCP Storage
    # downloadFolder(BUCKET_NAME, 'configs/', 'configs/')

    # Read the config file
    user_token = readConfigAsJSON()
    user_server_channel = readConfigAsJSON('configs/user_server_channel_DO_NOT_EDIT.json')

    # Loop through every user in the user_server_channel config file
    for user in user_server_channel:
        # Loop through every guild
        for guild in user_server_channel[user]:
            # Loop through every channel and check if the status is 'new'
            for channel in user_server_channel[user][guild]:
                if user_server_channel[user][guild][channel]['status'] == "new":
                    logging.info('Extracting messages from channel (User: {}, Guild: {}, Channel: {})'.format(
                        user, 
                        guild, 
                        channel))

                    # Create a new JSON object for the channel
                    messages_JSON = createBaseMessageJSON(user, guild, channel, user_server_channel[user][guild][channel]['name'])
                    
                    print("Processing channel: {}".format(user_server_channel[user][guild][channel]['name']))

                    # Request the latest messages from the channel
                    messages = requestURLResponse(BASE_URL + urls['messages'].format(channel), 
                                                    user_token[user]['token'], 
                                                    {'limit': 1})
                    
                    # Update the JSON object with the messages
                    messages_JSON['messages'] = messages

                    if "attachments" in messages[0]:
                        for attachment in messages[0]["attachments"]:
                            if 'url' in attachment:
                                downloadContent(attachment['url'], DATA_FOLDER_MEDIA)

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
                        messages = requestURLResponse(BASE_URL + urls['messages'].format(channel), 
                                                        user_token[user]['token'], 
                                                        params)

                        for message in messages:
                            if "attachments" in message:
                                for attachment in message["attachments"]:
                                    if 'url' in attachment:
                                        downloadContent(attachment['url'], DATA_FOLDER_MEDIA)
                        
                        # Check if the messages list is empty, if it is, break both the loops
                        if len(messages) == 0:
                            check = False
                            break

                        # Update the BEFORE param to the last message processed
                        last_message_processed = messages[-1]['id']

                        # Update the JSON object with the messages
                        messages_JSON['messages'] += messages

                    # Reverse the messages in the JSON object so that the messages are in chronological order
                    messages_JSON['messages'].reverse()

                    # Update the config file
                    user_server_channel[user][guild][channel]['last_processed'] = latest_message_processed
                    user_server_channel[user][guild][channel]['status'] = "processed"

                    # Write the JSON object to a file
                    writeFile(DATA_FOLDER + '{}.json'.format(channel), messages_JSON)

    # Write the updated config file
    writeFile('configs/user_server_channel_DO_NOT_EDIT.json', user_server_channel)
    
    # Upload the updated config files to GCP Storage
    # logging.info('Uploading updated configs')
    # uploadFolder(BUCKET_NAME, 'configs/', 'configs/')

    # Upload the Data folder to GCP Storage
    # logging.info('Uploading extracted messages')
    # uploadFolder(BUCKET_NAME, DATA_FOLDER, DATA_FOLDER)

'''
Upload logs to GCP Storage
'''
# def uploadLogs():
#     logging.info('Uploading logs')
#     uploadFile(BUCKET_NAME, 
#                 'discordMessageExtractor.log', 
#                 '{}/{}/{}'.format(
#                     'logs', 
#                     datetime.now().strftime("%Y-%m-%d"), 
#                     'discordMessageExtractor.log'))


'''
Main driver function

TODO: Print the logs to the console as well
TODO: (Suggestion) have config file in SQL database
TODO: Create partial 'new' processing function - In case the script times out
      To be done in the end
'''
if __name__ == "__main__":

    # Set up logging
    # logging.getLogger().addHandler(logging.StreamHandler())
    logging.basicConfig(filename='discordMessageExtractor.log',
                        filemode='w', 
                        level=logging.DEBUG,
                        format='%(asctime)s [%(levelname)s] %(message)s')
    
    # Read the command line arguments
    print('Starting Discord Message Extractor')
    parser = createCMDParser()
    args = parser.parse_args()

    # Set upload logs to true
    uploadLogFile = True

    # Based on the command line arguments, call the appropriate function
    if args.mode == 'update':
        logging.info('Running in update mode')
        updateConfigs()
    elif args.mode == 'extractOld':
        logging.info('Running in extract mode: OLD')
        extractMessageFromExploredChannels()
    elif args.mode == 'extractNew':
        logging.info('Running in extract mode: NEW')
        extractMessageFromNewChannels()
    elif args.mode == 'extractAll':
        logging.info('Running in do all mode')
        updateConfigs()
        extractMessageFromExploredChannels()
        extractMessageFromNewChannels()
    else:
        print('Invalid mode')
        uploadLogFile = False
    
    # if uploadLogFile:
    #     uploadLogs()

    # Deleting the data folder and the configs folder after the script is done
    # deleteFolder('data/')
    # deleteFolder('configs/')
    