"""
Defines a class to handle our connections in a sensible and thread-safe manner.
"""

from threading import Lock, active_count
import logging
import time

import pymysql

import es_auth

from cmd_ids import *

LOG = logging.getLogger("update_config")


# A decorator to allow us to make functions thread-safe
def synchronised(method):
    def _wrapper(*args, **kwargs):
        self = args[0]
        self._lock.acquire()
        ret = method(*args, **kwargs)
        self._lock.release()
        return ret
    return _wrapper


class DBConnection:
    # Number of times a connection has to sleep before the length of time of the sleep is increased
    # This is multiplied by the number of Zephyrs active, so the connection wait time should increase after
    # every Zephyr has SLEEP_TIME_PATIENCE checks
    SLEEP_TIME_PATIENCE = 10

    # By how many seconds will the sleep time increase
    SLEEP_TIME_INCREMENT = 5

    # The maximum sleep time permitted
    SLEEP_TIME_MAX = 60*10

    def __init__(self):
        self._lock = Lock()
        self._connection = pymysql.connect(user=es_auth._get_creds("dbPOD_write")["user"],
                                          password=es_auth._get_creds("dbPOD_write")["password"],
                                          host=es_auth._get_creds("dbPOD_write")["host"],
                                          database=es_auth._get_creds("dbPOD_write")["db"])
        self._sleep_time = 10
        self._attempts_at_sleep_time = 0

    def _progress_sleep_time(self):
        """
        If _attempts_at_sleep_time is greater than SLEEP_TIME_PATIENCE, increase it by SLEEP_TIME_INCREMENT,
        up to a maximum of SLEEP_TIME_MAX. Increment _attempts_at_sleep_time.

        Used for the process of gradually making requests less and less frequently
        :return:
        """
        self._attempts_at_sleep_time += 1

        if self._sleep_time >= self.SLEEP_TIME_MAX:
            # Already at the maximum sleep time
            return

        if self._attempts_at_sleep_time >= self.SLEEP_TIME_PATIENCE * (active_count()-1):
            self._attempts_at_sleep_time = 0
            self._sleep_time += self.SLEEP_TIME_INCREMENT
            LOG.debug(f"Increasing sleep time to {self._sleep_time}")


    # An unsynchronised version, to be used by synchronised functions
    def _send_command_to_zephyr(self, commandId, zephyrName):
        # The following SQL command is taken from the definition of addActivePendingCommand.
        with self._connection.cursor() as c:
            c.execute(
                """INSERT INTO pendingCommands (status, id_pod, id_libraryCommand, insertionDateTime, repetition)
                   VALUES (0, (SELECT id_pod FROM pod WHERE serialNumber = %s), %s, now(), 0)""",
                (zephyrName, commandId))
            LOG.debug(f"Queued command {commandId} to Zephyr {zephyrName}")
        self._connection.commit()
        return c.lastrowid

    @synchronised
    def send_command_to_zephyr(self, commandId, zephyrName):
        """
        Sends a command of the given ID to the Zephyr.
        :param commandId:
        :param zephyrName:
        :return: The ID in the pendingCommands table of the new command.
        """
        return self._send_command_to_zephyr(commandId, zephyrName)

    def get_command_response(self, pending_command_id):
        """
        A blocking function which waits until the given pending command has been executed, and then returns the response
        from the Zephyr.
        :param pending_command_id:
        :return:
        """
        while True:
            # First, search the executedCommands table for a row with id_pendingCommand = pending_command_id.
            self._lock.acquire()
            with self._connection.cursor() as c:
                c.execute("""SELECT response FROM executedCommands WHERE id_pendingCommand = %s""",
                          (pending_command_id,))
                results = list(c)
                LOG.debug(f"Searching for pending command {pending_command_id}, found results {results}")
            self._connection.commit()
            self._lock.release()

            # Did we get a result?
            if len(results) != 0:
                return results[0][0]

            # We didn't get any results. Sleep for a while and try again.
            self._progress_sleep_time()
            time.sleep(self._sleep_time)

    @synchronised
    def set_ports(self, zephyrName):
        """
        Set the v2.5-style ports of the given Zephyr
        :param zephyrName:
        :return:
        """
        self._send_command_to_zephyr(COMMAND_ID_SET_NEW_PORTS, zephyrName)
        LOG.info("Sent command to set ports")