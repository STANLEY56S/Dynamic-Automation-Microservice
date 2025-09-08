"""
commonUtility.py
==============

Author: Stanley Parmar
Description: Common Utilities which is used across the project.
See the examples directory to learn about the usage.

"""
# commonUtility.py

# Import the default Libraries
import logging
import os
import json
import sys
import multiprocessing
# Import the custom Libraries
from datetime import datetime, timedelta
import traceback
import pytz


"""
Function Name: extract_content_from_json
Inputs:
- json: json content.

Output: matching one

Description:
Loop through JSON and get the value which is matching with the module
"""


def extract_content_from_json(json_content, module_id):
    try:
        # Loop through each key-value pair in the item
        for columns_mapping_list in json_content:
            # Loop through each key-value pair in the item
            for module_name, column_mapping in columns_mapping_list.items():
                if module_name == module_id:
                    return column_mapping

    except Exception as e:
        debug_print('Error getting extract_content_from_json: \n {}'.format(str(e)))
        # traceback.print_exc()  # This will print the full traceback, including the line number


"""
    Function Name: open_read_file
    Inputs: file_location (file location) , filename (file name before _config.json like flask for general_config.json),
    env[ profile file_location prod|dev|qa]
    Output: config_list_json (List of the configs in the file)
"""


def open_read_file(file_location, local_env, filename):
    try:
        # Get the directory of the current script
        current_dir = os.path.dirname(__file__)

        # Move up to the backend directory
        backend_dir = os.path.abspath(os.path.join(current_dir, '..'))

        # Construct the full path to the config file
        config_path = os.path.join(backend_dir, file_location, filename + '_config.json')

        # print("config_path: {} : {} : {} ".format(backend_dir, file_location, filename + '_config.json'))

        # Load configuration from the file_location and file
        with open(config_path) as config_file:
            config = json.load(config_file)

        # Get the configuration for the current environment
        if local_env:
            config_list_json = config.get(local_env)
        else:
            config_list_json = config

        # Failing if the file is having any issues
        if not config_list_json:
            raise ValueError("No configuration found for environment:%s", local_env)

        # print(config_list_json)
        return config_list_json

    except Exception as e:
        traceback.print_exc()  # This will print the full traceback, including the line number
        logger.error("Error readfile function in commonUtility  : {}".format(str(e)))


"""
    Function Name: open_read_file_box
    Inputs:  filename (file name before _config.json like flask for general_config.json)
    Output: config_list_json (List of the configs in the file)
"""


def open_read_file_box(filename):
    try:
        # Define the path to the config file
        resource_list = open_read_file('resources', '', 'general')

        # Define the file path for Ubuntu
        ubuntu_resource_path = resource_list['ubuntu_resources_path']

        # Define the file path for Windows
        windows_resource_path = resource_list['windows_resources_path']

        # Choose the correct file path based on the operating system
        file_path = ubuntu_resource_path if os.name == 'posix' else windows_resource_path

        filename = file_path + filename + '_config.json'

        # Open the file in read mode and parse the JSON content
        with open(filename, 'r') as file:
            config_list_json = json.load(file)

        # Print the parsed JSON data
        # debug_print(config_list_json)

        # Failing if the file is having any issues
        if not config_list_json:
            raise ValueError("No configuration found for environment")

        # print(config_list_json)
        return config_list_json

    except Exception as e:
        traceback.print_exc()  # This will print the full traceback, including the line number
        logger.error("Error readfile function in commonUtility  : {}".format(str(e)))
        debug_print("Error readfile function in commonUtility  : {}".format(str(e)))


"""
Function Name: get_topic
Inputs: file_location (file location), filename (file name)
Output: topic (Kafka topic)
Description: Get the Kafka topic from the JSON file.
"""


# To get the topic config file.
def get_topic(filename):
    try:
        # Define the path to the config file
        resource_list = open_read_file('resources', '', 'general')

        # Define the file path for Ubuntu
        ubuntu_resource_path = resource_list['ubuntu_resources_path']

        # Define the file path for Windows
        windows_resource_path = resource_list['windows_resources_path']

        # Choose the correct file path based on the operating system
        file_path = ubuntu_resource_path if os.name == 'posix' else windows_resource_path

        topic_path = file_path + filename + '_topic_config.json'

        # Load topic from the file
        with open(topic_path) as topic_file:
            topic_data = json.load(topic_file)

        # Get the topic
        topic = topic_data.get('topic')

        # Failing if the file is having any issues
        if not topic:
            raise ValueError("No topic found in file:", filename)
        return topic

    except Exception as e:
        traceback.print_exc()  # This will print the full traceback, including the line number
        debug_print("Error in get_topic function in commonUtility: {}".format(str(e)))


"""
    Function Name: debug_print
    Inputs: message to print
    Output:  message on console
    Description: only prints statements if the mode is set to debug
"""


def debug_print(message):
    config = open_read_file('resources', '', 'general')
    if config.get("debug", False):
        # print(message)
        try:
            print(message)
        except UnicodeEncodeError:
            print(message.encode(sys.stdout.encoding, errors='replace').decode(sys.stdout.encoding))


"""
    Function Name: find_first_matching_key
    Inputs: none
    Output:  doc[key]
"""


def find_first_matching_key(doc, possible_keys):
    for key in possible_keys:
        if key in doc:
            return doc[key]
    return None


"""
    Function Name: get_key_by_value
    Inputs: none
    Output:  k
"""


def get_key_by_value(d, value):
    for k, v in d.items():
        if value in v:
            return k
    return None


"""
    Function Name: get_first_chars
    Inputs: input_string, input_first_char, input_second_char
    Output: input_string
"""


def get_first_chars(input_string, input_first_char, input_second_char):
    return input_string[input_first_char:input_second_char]


"""
    Function Name: get_last_chars
    Inputs: input_string, input_characters
    Output: input_string
"""


def get_last_chars(input_string, input_characters):
    return input_string[-input_characters:]


"""
Function Name: func_wrapper
Inputs: func, args
Output: func(*args)
Description: Func wrapper for MultiThreads.
"""


def func_wrapper(func, args):
    return func(*args)


"""
Function Name: multi_proc
Inputs: tasks
Output: None
Description: Multi Pooling Threads.
"""


# Example function to simulate a task
def multi_proc(tasks):
    # Execute each function with its parameters using pool.starmap
    # Create a pool of processes
    with multiprocessing.Pool() as pool:
        # Execute each function with its parameters using pool.starmap
        results = pool.starmap(lambda func, args: func(*args), tasks.values())

    # Print results
    debug_print("Results:")
    for result in results:
        debug_print(result)


"""
Function Name: get_sys_args
Inputs: None
Output: Tuple containing environment and config name
Description: Get and validate the command-line arguments.
"""


def get_sys_args():
    if len(sys.argv) != 2:
        debug_print("Usage: python your_script.py <env> <config_name>")
        sys.exit(1)
    config_name = sys.argv[1]
    return config_name


# Retrieve system arguments and configuration
config_list = open_read_file('resources', '', 'general')
path = config_list['log_path']

# Get the current date in yyyy-mm-dd format
current_date = datetime.now().strftime('%Y-%m-%d')

# Define the log file paths for each level, including the date in the file names
log_file_paths = {
    'debug': os.path.join(path, 'debug_%s.log' % current_date),
    'info': os.path.join(path, 'info_%s.log' % current_date),
    'warning': os.path.join(path, 'warning_%s.log' % current_date),
    'error': os.path.join(path, 'error_%s.log' % current_date),
    'critical': os.path.join(path, 'critical_%s.log' % current_date),
}

# Create the logs directory if it doesn't exist
os.makedirs(os.path.dirname(list(log_file_paths.values())[0]), exist_ok=True)

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # Set the lowest threshold

"""
Function Name: create_handler
Inputs: 
- level_name: The log level name (e.g., 'debug', 'info', etc.)
- path: The file path for the log file

Output: 
- handler: Configured logging handler

Description: 
Creates a logging handler for the specified log level, sets the appropriate level and filter, 
and formats the log messages.
"""


def create_handler(level_name, local_path):
    local_handler = logging.FileHandler(local_path)
    local_level = getattr(logging, level_name.upper())
    local_handler.setLevel(local_level)
    local_handler.addFilter(lambda record: record.levelno == local_level)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    local_handler.setFormatter(formatter)
    return local_handler


# Create and add handlers for each log level
for level, path in log_file_paths.items():
    handler = create_handler(level, path)
    logger.addHandler(handler)


"""
Function Name: get_timestamp
Inputs:
Output:
Description:
     get_timestamp
"""


def get_timestamp():
    """Returns a timestamp in YYYYMMDD_HHMMSS format"""
    return datetime.now().strftime("%Y%m%d_%H%M%S")


"""
Function Name: get_local_time_zone
Inputs:None
Output:local time zone now
Description:
     get local time zone based on the config
"""


def get_local_time_zone(config_list):
    # getting the local time zone from the config
    time_zone = config_list['time_zone']
    # converting the local time zone from the config for the current timestamp
    local_tz = pytz.timezone(time_zone)
    # converting the current timestamp to local time zone
    current_time_local = datetime.now(local_tz)
    return current_time_local