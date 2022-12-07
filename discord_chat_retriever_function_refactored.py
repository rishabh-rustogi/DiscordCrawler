from datetime import datetime
from discord_chat_retriever_data_hub import *
from google.cloud import storage

import argparse
import functions_framework
import glob
import google.cloud.logging
import json
import logging
import os
import requests
import shutil
import time


# Instantiates a logging client
logging_client = google.cloud.logging.Client()

# Retrieves a Cloud Logging handler based on the environment
# you're running in and integrates the handler with the
# Python logging module. By default this captures all logs
# at INFO level and higher
logging_client.setup_logging()


# A central hub to operate on the data
discord_chat_retriever_data_hub = DiscordChatRetrieverDataHub()

# log file name
LOG_FILE_NAME = 'discord_chat_retriever.log'


@functions_framework.http
def http_entry(request):
    """HTTP Cloud Function.
    Keyword Arguments:
        * request (flask.Request): The request object.
        <https://flask.palletsprojects.com/en/1.1.x/api/#incoming-request-data>
    
    --------------------------------------

    Returns Values:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
        <https://flask.palletsprojects.com/en/1.1.x/api/#flask.make_response>.

    --------------------------------------

    TODO: Check for the use of request_json and request_arg
    """
    request_json = request.get_json(silent = True)
    request_args = request.args
    discord_chat_retriever_data_hub.update_configs()
    discord_chat_retriever_data_hub.extract_message_from_explored_channels()
    discord_chat_retriever_data_hub.extract_message_from_new_channels()
    discord_chat_retriever_data_hub.delete_folder('data/')
    discord_chat_retriever_data_hub.delete_folder('configs/')
    return "Request Complete."


def create_cmd_parser():
    """ Parse the arguments passed to the script
    Return Values:
    * parser: python parser object -- python object for parsed command line arguments
    """

    # Create the parser 
    parser = argparse.ArgumentParser(description = 'Dicord Message Extractor')

    # Add the arguments
    parser.add_argument('--mode', type = str, default = 'help', help = 'Mode to run the program in')

    return parser


if __name__ == "__main__":

    # Set up logging
    # logging.getLogger().addHandler(logging.StreamHandler())
    logging.basicConfig(file_name = LOG_FILE_NAME,
                        filemode = 'w', 
                        level = logging.DEBUG,
                        format = '%(asctime)s [%(levelname)s] %(message)s')
    
    # Read the command line arguments
    print("Starting Discord Message Extractor")
    parser = create_cmd_parser()
    args = parser.parse_args()

    # Set upload logs to true
    upload_log_file = True

    # Based on the command line arguments, call the appropriate function
    if args.mode == 'update':
        logging.info("Running in update mode")
        discord_chat_retriever_data_hub.update_configs()
    elif args.mode == 'extractOld':
        logging.info("Running in extract mode: OLD")
        discord_chat_retriever_data_hub.extract_message_from_explored_channels()
    elif args.mode == 'extractNew':
        logging.info("Running in extract mode: NEW")
        discord_chat_retriever_data_hub.extract_message_from_new_channels()
    elif args.mode == 'extractAll':
        logging.info("Running in do all mode")
        discord_chat_retriever_data_hub.update_configs()
        discord_chat_retriever_data_hub.extract_message_from_explored_channels()
        discord_chat_retriever_data_hub.extract_message_from_new_channels()
    else:
        print("Invalid mode")
        upload_log_file = False
    
    if upload_log_file:
        # upload log file for main function
        discord_chat_retriever_data_hub.upload_logs(LOG_FILE_NAME)
        # upload log file for the data hub 
        discord_chat_retriever_data_hub.upload_logs(discord_chat_retriever_data_hub.LOG_FILE_NAME)

    # Deleting the data folder and the configs folder after the script is done
    discord_chat_retriever_data_hub.delete_folder('data/')
    discord_chat_retriever_data_hub.delete_folder('configs/')
