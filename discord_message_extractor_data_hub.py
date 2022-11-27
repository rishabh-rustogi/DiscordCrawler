from datetime import datetime

import json
import logging
import os
import requests
import shutil
import time

class DiscordMessageExtractorDataHub:

    ###############################################
    #####               CONSTANTS             ##### 
    ###############################################
    BASE_URL = 'https://discord.com/api/v9/'
    GLOBAL_RATE_LIMIT_PER_SEC = 50
    BASE_USER_SERVER_CHANNEL = {}
    DATA_FOLDER = 'data/{}/'.format(datetime.now().strftime('%Y-%m-%d'))
    DATA_FOLDER_MEDIA = DATA_FOLDER + 'media/'
    BUCKET_NAME = 'discordchatexporter'

    def __init__(self):

        # Define global variables
        self.requests_per_second = 0
        self.start_time = time.time_ns()

        # URL endpoints for Discord API
        self.urls = {
            'guilds': 'users/@me/guilds',
            'channels': 'guilds/{}/channels',
            'messages': 'channels/{}/messages'
        }

        # Endpoint parameters
        self.url_params = {
            'guilds': {},
            'channels': {},
            'messages': {'limit': 100,
                        'before': None}
        }

        # Set up logging
        logging.basicConfig(file_name = 'discord_message_extractor_data_hub.log',
                            filemode = 'w', 
                            level = logging.DEBUG,
                            format = '%(asctime)s [%(levelname)s] %(message)s')


    def update_configs(self):
        """ Update the config file with the latest data

        Steps:
        1. Download the config files from GCP Storage
        2. Loop though every user in the config file
            Loop through every guild
                If guild not in config file, add it
            Loop through every channel in the guild
            If a appropiate channel is found (i.e. it has a last_message_id)
                If channel already in config: Set channel status as 'processing'
                If channel not in config: Add channel to confif and set status as 'new'
        3. For every channel in the config file
            If channel status is still 'processing', set status as 'inactive'
            f channel 'latest_message_id' is null, set status as 'inactive'
        4. Write the updated JSON to the config file
        5. Upload the config file to GCP Storage

        ----------------------------------

        TODO: Finalize the config structure
        TODO: What exactly is an appropiate channel? (For now a channel which has last_message_id)
        TODO: Upload the config file to GCP Storage
        TODO: At the edn of the script trigger a pubsub event to start the script again to 
        extract messages from the channels with status 'processing'
        """

        logging.info("Updating configs")

        # Download the config files from GCP Storage
        # download_folder(BUCKET_NAME, 'configs/', 'configs/')

        # Read the config files
        user_token = self._read_config_as_json()
        user_server_channel = self._read_config_as_json('configs/user_server_channel_DO_NOT_EDIT.json')

        # Loop through every user in the user config file
        for user in user_token:

            # If user not in user_server_channel, add them
            if user not in user_server_channel:
                user_server_channel[user] = {}

            # Read the user's guilds
            guilds = self._request_url_response(self.BASE_URL + self.urls['guilds'], 
                                        user_token[user]['token'], 
                                        self.url_params['guilds'])

            # Loop through every guild
            for guild in guilds:

                # Filter out guild for testing
                if guild['id'] not in ['1015029498317651979']:#, '894349669499568148']:
                    continue

                # If guild not in user_server_channel, add it
                if guild['id'] not in user_server_channel[user]:
                    user_server_channel[user][guild['id']] = {}

                # Read the user's channels
                channels = self._request_url_response(self.BASE_URL + self.urls['channels'].format(guild['id']), 
                                                user_token[user]['token'], 
                                                self.url_params['channels'])

                # Loop through every channel
                for channel in channels:
                    if 'last_message_id' not in channel or channel['last_message_id'] is None:
                        # If channel is not aa appropiate channel, skip it 
                        continue
                    elif channel['id'] not in user_server_channel[user][guild['id']]:
                        # If channel not in user_server_channel, add it
                        user_server_channel[user][guild['id']][channel['id']] = {
                            'name': channel['name'],
                            'started': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            'last_processed': None,
                            'latest_message_id': channel['last_message_id'], 
                            'status': 'new'
                        }
                    else:
                        # If channel already in user_server_channel, set status as 'processing'
                        # and update the latest_message_id
                        user_server_channel[user][guild['id']][channel['id']]['status'] = 'processing'
                        user_server_channel[user][guild['id']][channel['id']]['latest_message_id'] = channel['last_message_id']
        
        # Set the status of every channel in the config file to 'inactive' if it is still 'processing'
        # or if the latest_message_id is null
        for user in user_server_channel:
            for guild in user_server_channel[user]:
                for channel in user_server_channel[user][guild]:
                    if user_server_channel[user][guild][channel]['last_processed'] == None:
                        user_server_channel[user][guild][channel]['status'] = 'new'
                    if user_server_channel[user][guild][channel]['status'] == 'processed':
                        user_server_channel[user][guild][channel]['status'] = 'inactive'
                    if user_server_channel[user][guild][channel]['latest_message_id'] is None:
                        user_server_channel[user][guild][channel]['status'] = 'inactive'

        # Write the updated config file
        self._write_file('configs/user_server_channel_DO_NOT_EDIT.json', user_server_channel)

        # Upload the updated config file to GCP Storage
        # upload_folder(BUCKET_NAME, 'configs/', 'configs/')
        logging.info('Configs updated')
    

    def extract_message_from_explored_channels(self):
        """ Download the messages for all the channels in the config file which have a status of 'processing'

        Steps:
        1. Read the config file
        2. Loop through every user in the user_server_channel config file
        3. Loop through every guild
        4. Loop through every channel and check if the status is 'processing'
                * Get the last message timestamp from the config file
                * Set BEFORE param to the latest message timestamp
                * Request the latest messages from the channel
                    * If the latest message timestamp is <= than the last message processed 
                    timestamp from the config file, ignore the channel
                    * Else, create a new JSON object for the channel and write the messages to it
                    Update the BEFORE param to the latest processed message timestamp
                * Do this until a message is found which has a timestamp <= than the last message processed timestamp
                    * Request the messages before the BEFORE param
                    * Loop through every message and check if the timestamp is <= than the last message processed timestamp
                        * If a message is found, break both the loops
                        * Else, write the messages to the JSON object and update the BEFORE param
                * Reverse the messages in the JSON object so that the messages are in chronological order
                * Update the config file
                    * Update the last_processed timestamp in the config file to the latest message timestamp
                    * Update the status to 'processed'
                * Write the JSON object to a file
        5. Write the updated config file
        6. Upload the update file to GCP Storage
                * Upload the config folder to GCP Storage
                * Upload the Data folder to GCP Storage
        
        -----------------------------------------------

        TODO: Upload the config file to GCP Storage

        TODO: 
            * Introduce global time limit for the script
                * If the script has been running for more than 10 minutes (yet to be decided), trigger a new 
                pubsub event to start the script again for the channels which still have a status of 
                'processing'
                * Else, the script will continue to run until all the channels have a status of 'processed'
                * Finally trigger a pubsub event to start the script but now for the channels which have a 
                status of 'new'
        """


        logging.info("Downloading configs")

        # Download the config file from GCP Storage
        # download_folder(BUCKET_NAME, 'configs/', 'configs/')

        # Read the config file
        user_token = self._read_config_as_json()
        user_server_channel = self._read_config_as_json('configs/user_server_channel_DO_NOT_EDIT.json')

        # Loop through every user in the user_server_channel config file
        for user in user_server_channel:
            # Loop through every guild
            for guild in user_server_channel[user]:
                # Loop through every channel and check if the status is 'processing'
                for channel in user_server_channel[user][guild]:
                    # Check if the status is 'processing'
                    if user_server_channel[user][guild][channel]['status'] == 'processing':
                        logging.info('Extracting messages from channel (User: {}, Guild: {}, Channel: {})'.format(
                            user, 
                            guild, 
                            channel))

                        # Get the last message timestamp from the config file
                        last_proccessed_timestamp = self._twitter_snowflake_to_datetime(user_server_channel[user][guild][channel]['last_processed'])
                        
                        # Create a new JSON object for the channel
                        messages_json = self._create_base_message_json(user, guild, channel, user_server_channel[user][guild][channel]['name'])
                        
                        # Request the latest messages from the channel
                        messages = self._request_url_response(self.BASE_URL + self.urls['messages'].format(channel), 
                                                        user_token[user]['token'], 
                                                        {'limit': 1})
                        
                        # If the latest message timestamp is <= than the last message processed, ignore the channel
                        current_timestamp = self._twitter_snowflake_to_datetime(messages[0]['id'])
                        if current_timestamp <= last_proccessed_timestamp:
                            user_server_channel[user][guild][channel]['status'] = 'processed'
                            continue

                        if "attachments" in messages[0]:
                            for attachment in messages[0]['attachments']:
                                if 'url' in attachment:
                                    self._download_content(attachment['url'], self.DATA_FOLDER_MEDIA)

                        # Update the JSON object with the messages
                        messages_json['messages'] = messages

                        # Set the BEFORE param to the timestamp of the latest message processed
                        last_message_processed = messages_json['messages'][0]['id']
                        
                        print("Processing channel: {}".format(user_server_channel[user][guild][channel]['name']))

                        # Also, save the latest message ID to store in the config file
                        latest_message_processed = messages_json['messages'][-1]['id']
                        
                        check = True
                        while (check):

                            # Get the 'message' endpoint params and set the before param to the last message processed
                            params = self.url_params['messages'].copy()
                            params['before'] = last_message_processed
                            
                            # Request the messages before the BEFORE param
                            messages = self._request_url_response(self.BASE_URL + self.urls['messages'].format(channel), 
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
                                
                                if 'attachments' in message:
                                    for attachment in message['attachments']:
                                        if 'url' in attachment:
                                            self._download_content(attachment['url'], self.DATA_FOLDER_MEDIA)

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
                            messages_json['messages'] += messages

                        # Reverse the messages in the JSON object so that the messages are in chronological order
                        messages_json['messages'].reverse()

                        # Update the config file
                        user_server_channel[user][guild][channel]['last_processed'] = latest_message_processed
                        user_server_channel[user][guild][channel]['status'] = "processed"

                        # Write the JSON object to a file
                        self._write_file(self.DATA_FOLDER + '{}.json'.format(channel), messages_json)
        
        # Write the updated config file
        self._write_file('configs/user_server_channel_DO_NOT_EDIT.json', user_server_channel)
    

    def extract_message_from_new_channels(self):
        """ Download the messages for all the channels in the config file which have a status of 'new'

        Steps:
        1. Read the config file
        2. Loop through every user in the user_server_channel config file
                * Loop through every guild
                * Loop through every channel and check if the status is 'new'
                    * Create a new JSON object for the channel
                    * Request the latest messages from the channel
                    * Update the JSON object with the messages
                    * Set the BEFORE param to the timestamp of the latest message processed
                    * Also, save the latest message ID to store in the config file
                    * Do the following until no more messages are found
                        * Get the 'message' endpoint params and set the before param to the last message processed
                        * Request the messages before the BEFORE param
                        * Add the messages to the JSON object and update the BEFORE param
                    * Reverse the messages in the JSON object so that the messages are in chronological order
                    * Update the config file
                        * Set the status to 'processed'
                        * Set the last_processed to the latest message processed
                    * Write the JSON object to a file
        3. Write the updated config file
        4. Upload the update file to GCP Storage
        5. Upload the Data folder to GCP Storage

        ------------------------------------

        TODO: Upload the files to GCP Storage

        TODO: 
            * To avoid global time limit for the script
                * Only process a single channel
                * If more 'new' channels are left, trigger a pubsub event to start the script again 
                * Check if the script times out before even a single channel is processed
                    * If it does, then trigger a pubsub event to start the script again to process the 
                    same channel partially (TODO: even mentioned in the main driver script)
        
        """


        logging.info("Downloading configs")

        # Download the config files from GCP Storage
        # download_folder(BUCKET_NAME, 'configs/', 'configs/')

        # Read the config file
        user_token = self._read_config_as_json()
        user_server_channel = self._read_config_as_json('configs/user_server_channel_DO_NOT_EDIT.json')

        # Loop through every user in the user_server_channel config file
        for user in user_server_channel:
            # Loop through every guild
            for guild in user_server_channel[user]:
                # Loop through every channel and check if the status is 'new'
                for channel in user_server_channel[user][guild]:
                    if user_server_channel[user][guild][channel]['status'] == 'new':
                        logging.info("Extracting messages from channel (User: {}, Guild: {}, Channel: {})".format(
                            user, 
                            guild, 
                            channel))

                        # Create a new JSON object for the channel
                        messages_json = self._create_base_message_json(user, guild, channel, user_server_channel[user][guild][channel]['name'])
                        
                        print("Processing channel: {}".format(user_server_channel[user][guild][channel]['name']))

                        # Request the latest messages from the channel
                        messages = self._request_url_response(self.BASE_URL + self.urls['messages'].format(channel), 
                                                        user_token[user]['token'], 
                                                        {'limit': 1})
                        
                        # Update the JSON object with the messages
                        messages_json['messages'] = messages

                        if 'attachments' in messages[0]:
                            for attachment in messages[0]['attachments']:
                                if 'url' in attachment:
                                    self._download_content(attachment['url'], self.DATA_FOLDER_MEDIA)

                        # Set the BEFORE param to the timestamp of the latest message processed
                        last_message_processed = messages_json['messages'][0]['id']
                        
                        # Also, save the latest message ID to store in the config file
                        latest_message_processed = messages_json['messages'][-1]['id']
                        
                        check = True
                        while (check):
                            # Get the 'message' endpoint params and set the before param to the last message processed
                            params = self.url_params['messages'].copy()
                            params['before'] = last_message_processed
                            
                            # Request the messages before the BEFORE param
                            messages = self._request_url_response(self.BASE_URL + self.urls['messages'].format(channel), 
                                                            user_token[user]['token'], 
                                                            params)

                            for message in messages:
                                if 'attachments' in message:
                                    for attachment in message['attachments']:
                                        if 'url' in attachment:
                                            self._download_content(attachment['url'], self.DATA_FOLDER_MEDIA)
                            
                            # Check if the messages list is empty, if it is, break both the loops
                            if len(messages) == 0:
                                check = False
                                break

                            # Update the BEFORE param to the last message processed
                            last_message_processed = messages[-1]['id']

                            # Update the JSON object with the messages
                            messages_json['messages'] += messages

                        # Reverse the messages in the JSON object so that the messages are in chronological order
                        messages_json['messages'].reverse()

                        # Update the config file
                        user_server_channel[user][guild][channel]['last_processed'] = latest_message_processed
                        user_server_channel[user][guild][channel]['status'] = "processed"

                        # Write the JSON object to a file
                        self._write_file(self.DATA_FOLDER + '{}.json'.format(channel), messages_json)

        # Write the updated config file
        self._write_file('configs/user_server_channel_DO_NOT_EDIT.json', user_server_channel)


    def _twitter_snowflake_to_datetime(self, snowflake):
        """ Convert a snowflake string to a datetime object

        Keyword Arguments:
        * snowflake: str -- Snowflake string

        -------------------------------

        Return Values:
        * Datetime object
        """

        return datetime.fromtimestamp(((int(snowflake) >> 22) + 1420070400000) / 1000)


    def _read_config_as_json(self, path = 'configs/user_token.json'):
        """ Read the config file as a JSON object and returns it

        Keyword Arguments:
        * path: str -- Path to the config file
        
        ------------------------------------

        Return Values:
        * Config file as a JSON object
        """

        logging.info("Reading {} config".format(path))
        try:
            with open(path, 'r') as f:
                return json.load(f)
        except Exception as e:
            logging.error("Error while reading config: {}".format(e))
            exit(1)
    

    def _request_url_response(self, url, token, params):
        """ Implements Global Rate Limit with while reuesting URL response.

        Keywrod Arguments:
        * url: str -- URL to request
        * token: str -- Discord token
        * params: dict -- Parameters to send with the request
        
        -------------------------
        
        Return Values:
        * response.json(): json object -- Response from the request
        """

        # Import global variables
        global requests_per_second
        global start_time
        requests_per_second += 1

        # Check if global rate limit is reached
        # If so, wait until the next second
        while requests_per_second > self.GLOBAL_RATE_LIMIT_PER_SEC and time.time_ns() - start_time < 1e9:
            continue

        # If the next second has started, reset the counter
        if time.time_ns() - start_time >= 1e9:
            requests_per_second = 0
            start_time = time.time_ns()
        
        # Calculate the average RPS and print it
        print("RPS: {}".format(requests_per_second), end = f'\r')

        # Request the URL with the given parameters and headers
        headers = {'Authorization': token}
        response = requests.get(url, headers = headers, params = params)

        # Check if somehow the global rate limit was exceeded, if so, wait until the retry-after time
        while response.status_code == 429:
            logging.error("Global Rate Limit for invalid requests exceeded")
            logging.warning("[Pausing for {}s] | Requested URL: {}".format(
                response.json()['retry_after'], 
                url))
            time.sleep(int(response.json()['retry_after']))
            requests_per_second = 0
            start_time = time.time_ns()

            # Request the URL with the given parameters and headers again
            response = requests.get(url, headers = headers, params = params)
        
        # Check if the request was not successful, if so, log the error, upload the logs and exit
        if response.status_code != 200:
            logging.error('Error while requesting URL: {}'.format(response.json()))
            exit(1)

        # Return the response
        return response.json()

    def _write_file(self, path, json_data):
        """ Dump json data to a file

        Keyword Arguments:
        * path: str -- Path to the file to dump the data to
        * json_data: json object -- JSON data to dump to the file
        """

        logging.info("Writing file: {}".format(path))
        file_route = path.split('/')
        self._create_folder('/'.join(file_route[ : -1]))
        try:
            with open(path, 'w') as f:
                json.dump(json_data, 
                            f,
                            indent=4, 
                            separators=(',', ': '))
        except Exception as e:
            logging.error("Error while writing file: {}".format(e))  
            exit(1)
    

    def _create_folder(self, folder_name):
        """ Create a folder if it doesn't exist

        Keyword Arguments:
        * folder_name: str -- Name of the folder to create
        
        --------------------------------

        Requirements:
        * OS Permissions to create the folder
        
        """

        # Check if the folder exists and create it if it doesn't
        if not os.path.exists(folder_name):
            logging.info("Creating folder {}".format(folder_name))
            os.makedirs(folder_name)
    

    def _create_base_message_json(self, user_id, guild_id, channel_id, channel_name):
        """ Create base JSON structure for a channel

        Keyword Arguments:
        * user_id: str -- User ID
        * guild_id: str -- Guild ID
        * channel_id: str -- Channel ID
        * channel_name: str -- Channel Name

        -------------------------------

        Return Values:
        * A dictionary (json format) with:
            user_id,
            guild_id,
            channel_id,
            channel_name
        """

        return {
            "user_id": user_id,
            "guild_id": guild_id,
            "channel_id": channel_id,
            "channel_name": channel_name,
            "messages": []
        }

    def _download_content(self, url, path):
        """ Download media from a url and save it to a file, only if the file size is less than 8MB

        Keyword Arguments:
        * url: str -- URL of the media
        * path: str -- Path to save the media to

        ----------------------------------

        TODO: Handle files larger than 8MB
        """

        try:
            file_route = url.split('/')
            path += '/'.join(file_route[len(file_route) - 3:len(file_route) - 2])
            file_name = '/' + url.split('/')[-1]
            self._create_folder(path)
            logging.info("Downloading file: {}".format(url))
            # Get the file size and download the file only if it is smaller than 8MB
            file_size = int(requests.head(url).headers['Content-Length'])
            if file_size < 8388608:
                with open(path + file_name, 'wb') as f:
                    f.write(requests.get(url).content)
            else:
                logging.info("File too large ({}): {}".format(file_size, url))
        except Exception as e:
            logging.error("Error while downloading file: {}".format(e))  
            exit(1)
    

    def _delete_folder(self, folder_name):
        """ Delete a folder and all its contents recursively

        Keyword Arguments:
        * folder_name: str -- Name of the folder to delete
        
        --------------------------------

        Requirements:
        * OS Permissions to delete the folder
        
        """

        # Check if the folder exists and delete it if it does
        if os.path.exists(folder_name):
            logging.info("Deleting folder {}".format(folder_name))
            shutil.rmtree(folder_name)
