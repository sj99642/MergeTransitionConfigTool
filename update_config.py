"""
This is a tool to carry out the configuration work needed in preparation for the rollout of merged firmware (v2_132).

This firmware uses different EEPROM settings than the old firmware for maintaining connections. The old method was to
have:
* An APN, stored inside an AT command
* Main host/port and alt host/port, each stored inside an AT command

The new method is to have:
* The APN on its own
* Main and alt servers on their own
* SSL and non-SSL ports on their own

These settings do not sit on top of the old settings but are in other, currently unused places. The aim of this tool
is to go through a set of old units, read their old versions, parse out what we need for the new versions, and
set the new versions.
"""

from collections import deque, namedtuple
from threading import Thread
import logging
import re
from enum import Enum

from connection import DBConnection
from cmd_ids import *

# Set up the logger
LOG = logging.Logger("update_config")
stderr_handler = logging.StreamHandler()
stderr_handler.setFormatter(logging.Formatter("%(asctime)s - %(threadName)s - %(levelname)s - %(message)s"))
stderr_handler.setLevel(logging.DEBUG)
LOG.addHandler(stderr_handler)
file_handler = logging.FileHandler("log.txt", mode="a")
file_handler.setFormatter(logging.Formatter("%(asctime)s - %(threadName)s - %(levelname)s - %(message)s"))
file_handler.setLevel(logging.DEBUG)
LOG.addHandler(file_handler)


# Create our connection. Our Connection class does all of the SQL work, and handles thread-safe operation.
CONNECTION = DBConnection()

EXPECTED_APN_COMMAND_FORMAT = re.compile(r"""AT\+CSTT=(\".*\",\".*\",\".*\")""")
EXPECTED_SERVER_COMMAND_FORMAT = re.compile(r"AT\+CIPSTART=\"TCP\",\"(.*)\",\".*\"")


class APNFormattingException(Exception):
    pass

class APNWhitelistException(Exception):
    pass

class ServerFormattingException(Exception):
    pass

class ServerWhitelistException(Exception):
    pass

class ServerType(Enum):
    MAIN = 0
    ALT = 1



def get_apn_dict():
    """
    Return a dictionary mapping APN to command library ID, based on the colon-separated apn_whitelist.txt.
    :return:
    """
    with open("apn_whitelist.txt", "rt") as f:
        apns = {l.split(":")[0]: int(l.split(":")[1]) for l in f.readlines() if l.strip()}
    return apns


def get_server_dict():
    """
    Return a dictionary mapping server to a tuple of (command ID to set main, command ID to set alt), based on
    the colon-separated server_whitelist.txt.
    :return:
    """
    servers = {}
    with open("server_whitelist.txt", "rt") as f:
        for line in f.readlines():
            parts = line.split(":")
            key = parts[0]

            try:
                main_server_command = int(parts[1])
            except ValueError:
                main_server_command = None

            try:
                alt_server_command = int(parts[2])
            except ValueError:
                alt_server_command = None

            servers[key] = (main_server_command, alt_server_command)
    return servers


def get_nt_string_from_hex(hex_string):
    """
    Gets a string from a hex readout of an EEPROM field. The field should look like:
     String -> Null Terminating 0 -> Any number of 0xFF bytes
    It is possible for the first one or two to be empty.

    The job of this function is to extract the String element, which may be empty.
    :return:
    """
    hex_string = hex_string.strip()
    b = []
    for i in range(0, len(hex_string), 2):
        b.append(int(hex_string[i:i+2], 16))

    # Find the first zero, and remove everything after it
    try:
        zero_index = b.index(0x00)
        del b[zero_index:]
    except ValueError:
        pass

    # Everything left is what we actually want
    s = ""
    for byte in b:
        s += chr(byte)

    # Return the string
    return s


def thread_zephyr(serialNumber):
    """
    Runs the configuration loop for a given Zephyr.
    :param serialNumber:
    :return:
    """

    # Start off by sending the commands to read the old values
    pending_id_get_old_apn = CONNECTION.send_command_to_zephyr(COMMAND_ID_GET_OLD_APN, serialNumber)
    LOG.debug("Sent GET_OLD_APN command")
    pending_id_get_new_apn = CONNECTION.send_command_to_zephyr(COMMAND_ID_GET_NEW_APN_HEX, serialNumber)
    LOG.debug("Sent GET_NEW_APN_HEX command")
    pending_id_get_old_main_host = CONNECTION.send_command_to_zephyr(COMMAND_ID_GET_OLD_MAIN_HOST, serialNumber)
    LOG.debug("Sent GET_OLD_MAIN_HOST command")
    pending_id_get_new_main_host = CONNECTION.send_command_to_zephyr(COMMAND_ID_GET_NEW_MAIN_HOST_HEX, serialNumber)
    LOG.debug("Sent GET_NEW_MAIN_HOST_HEX command")
    pending_id_get_old_alt_host = CONNECTION.send_command_to_zephyr(COMMAND_ID_GET_OLD_ALT_HOST, serialNumber)
    LOG.debug("Sent GET_OLD_ALT_HOST command")
    pending_id_get_new_alt_host = CONNECTION.send_command_to_zephyr(COMMAND_ID_GET_NEW_ALT_HOST_HEX, serialNumber)
    LOG.debug("Sent GET_NEW_ALT_HOST_HEX command")
    LOG.info("Sent all commands to get information")

    # Get the responses - These functions are blocking but only the first one practically should
    # Responses to QN and QE commands begin with a '0' character, so we have to cut it off
    old_apn_command = CONNECTION.get_command_response(pending_id_get_old_apn)[1:]
    LOG.info(f"Old APN command is {old_apn_command}")
    new_apn = get_nt_string_from_hex(CONNECTION.get_command_response(pending_id_get_new_apn)[1:])
    LOG.info(f"Current new APN command is {new_apn}")
    old_main_command = CONNECTION.get_command_response(pending_id_get_old_main_host)[1:]
    LOG.info(f"Old main server command is {old_main_command}")
    new_main = get_nt_string_from_hex(CONNECTION.get_command_response(pending_id_get_new_main_host)[1:])
    LOG.info(f"Current new main server command is {new_main}")
    old_alt_command = CONNECTION.get_command_response(pending_id_get_old_alt_host)[1:]
    LOG.info(f"Old alt server command is {old_alt_command}")
    new_alt = get_nt_string_from_hex(CONNECTION.get_command_response(pending_id_get_new_alt_host)[1:])
    LOG.info(f"Current new alt server command is {new_alt}")

    # Read in the APN whitelist
    # We read this in from the file every time so that theoretically we could add to the
    #  whitelists during the running of the program.
    apns = get_apn_dict()
    LOG.debug(f"Read APN whitelist: {apns}")

    # First, check the APN.
    if new_apn[:-2] not in apns.keys():
        set_new_apn(serialNumber, old_apn_command, apns)
    else:
        LOG.info(f"New APN {new_apn} already acceptable")

    # Read in the server whitelist
    servers = get_server_dict()
    LOG.debug(f"Read server whitelist: {servers}")

    # Check the main server
    if new_main not in servers.keys():
        set_server(serialNumber, old_main_command, servers, ServerType.MAIN)
    else:
        LOG.info(f"New main server {new_main} is already acceptable")

    # Check the alt server
    if new_alt not in servers.keys():
        set_server(serialNumber, old_alt_command, servers, ServerType.ALT)
    else:
        LOG.info(f"New alt server {new_alt} is already acceptable")

    # Set main port
    CONNECTION.set_ports(serialNumber)


def set_new_apn(serialNumber: str, old_apn_command: str, apns: "dict"):
    """
    Set the new-style APN of this Zephyr, based on its old-style APN command.
    :param serialNumber:
    :param old_apn_command:
    :param apns:
    :return:
    """
    LOG.debug("Starting to set new APN")

    # old_apn_command should have the form AT+CSTT=<apn-and-password>
    # We want to set the new command to <apn-and-password>,
    match = re.match(EXPECTED_APN_COMMAND_FORMAT, old_apn_command)

    if match is None:
        # The format wasn't right
        raise APNFormattingException(f"Old APN for {serialNumber} is {old_apn_command}, not formatted correctly")

    # We did actually find a match
    apn_and_password = match.group(1)
    LOG.info(f"Found APN and password as {apn_and_password}")

    # Check that the old-style APN is in the whitelist
    if apn_and_password not in apns.keys():
        raise APNWhitelistException(f"Old APN for {serialNumber} is {apn_and_password}, not in APN whitelist")

    # The apn/password we have here is in the whitelist, so we should send the command to set it
    set_apn_command_id = apns[apn_and_password]
    LOG.debug(f"Command to set APN is {set_apn_command_id}")
    CONNECTION.send_command_to_zephyr(set_apn_command_id, serialNumber)
    LOG.info(f"Queued up command to set APN to {apn_and_password}")




def set_server(serialNumber: str, old_command: str, servers: dict, main_or_alt: ServerType):
    """
    Set the new-style main server, based on its old-style main server command.
    :param serialNumber:
    :param old_command:
    :param servers:
    :param main_or_alt:
    :return:
    """
    LOG.debug(f"Starting to set new {'main' if main_or_alt == ServerType.MAIN else 'alt'} server")

    # old_main_command should have the form AT+CIPSTART="TCP","<server>","port"
    # We just want to extract <server>
    match = re.match(EXPECTED_SERVER_COMMAND_FORMAT, old_command)

    if match is None:
        # The format wasn't right
        raise ServerFormattingException(f"Old {'main' if main_or_alt == ServerType.MAIN else 'alt'} server for "
                                        f"{serialNumber} is {old_command}, not formatted correctly")

    # We found a match
    server = match.group(1)
    LOG.info(f"Found {'main' if main_or_alt == ServerType.MAIN else 'alt'} server as {server}")

    # Check that the server is in the whitelist
    if server not in servers.keys():
        raise ServerWhitelistException(f"Old {'main' if main_or_alt == ServerType.MAIN else 'alt'} server for "
                                       f"{serialNumber} is {server}, not in whitelist")

    # The server is in the whitelist, so we can set the right server properly
    if main_or_alt == ServerType.MAIN:
        set_server_command_id = servers[server][0]
    else:
        set_server_command_id = servers[server][1]
    LOG.debug(f"Command to set {'main' if main_or_alt == ServerType.MAIN else 'alt'} server is {set_server_command_id}")
    if set_server_command_id is None:
        # This server option is unavailable for this choice of main vs alt
        # (e.g. there is no command to set the alt server to AQ76)
        raise ServerWhitelistException(f"Unable to set {'main' if main_or_alt == ServerType.MAIN else 'alt'} server "
                                       f"for {serialNumber} to {server}, as that option is unavailable")

    CONNECTION.send_command_to_zephyr(set_server_command_id, serialNumber)
    LOG.info(f"Queued up command to set {'main' if main_or_alt == ServerType.MAIN else 'alt'} server to {server}")




def main(zephyrs: "list[str]"):
    """
    Run the program. This starts a new thread for every Zephyr.
    :param zephyrs:
    :return:
    """

    for zephyr in zephyrs:
        Thread(target=thread_zephyr, args=(zephyr,), name="Thread-"+zephyr).start()
        LOG.info(f"Started thread for Zephyr {zephyr}")



# Testing
if __name__ == "__main__":
    main(["TM400059"])