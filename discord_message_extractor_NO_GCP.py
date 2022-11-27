from datetime import datetime
from discord_message_extractor_data_hub import *

import argparse
import logging



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
    """ Main driver function

    TODO: Print the logs to the console as well
    TODO: (Suggestion) have config file in SQL database
    TODO: Create partial 'new' processing function - In case the script times out
        To be done in the end

    """

    # Set up logging
    # logging.getLogger().addHandler(logging.StreamHandler())
    logging.basicConfig(file_name = 'discordMessageExtractor.log',
                        filemode = 'w', 
                        level = logging.DEBUG,
                        format = '%(asctime)s [%(levelname)s] %(message)s')
    
    # Read the command line arguments
    print("Starting Discord Message Extractor")
    parser = create_cmd_parser()
    args = parser.parse_args()

    # A central hub to operate on the data
    discord_message_extractor_data_hub = DiscordMessageExtractorDataHub()

    # Set upload logs to true
    upload_log_file = True

    # Based on the command line arguments, call the appropriate function
    if args.mode == 'update':
        logging.info("Running in update mode")
        discord_message_extractor_data_hub.update_configs()
    elif args.mode == 'extractOld':
        logging.info("Running in extract mode: OLD")
        discord_message_extractor_data_hub.extract_message_from_explored_channels()
    elif args.mode == 'extractNew':
        logging.info("Running in extract mode: NEW")
        discord_message_extractor_data_hub.extract_message_from_new_channels()
    elif args.mode == 'extractAll':
        logging.info("Running in do all mode")
        discord_message_extractor_data_hub.update_configs()
        discord_message_extractor_data_hub.extract_message_from_explored_channels()
        discord_message_extractor_data_hub.extract_message_from_new_channels()
    else:
        print("Invalid mode")
        upload_log_file = False
    
    # if upload_log_file:
    #     uploadLogs()

    # Deleting the data folder and the configs folder after the script is done
    # delete_folder('data/')
    # delete_folder('configs/')
    