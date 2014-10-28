import collections
from espa_common import settings
import datetime


class Errors(object):
    '''Implementation for ESPA errors.resolve(error_message) interface'''

    def __init__(self):
        #build list of known error conditions to be checked
        self.conditions = list()
        self.conditions.append(self.night_scene)
        self.conditions.append(self.missing_ledaps_aux_data)
        self.conditions.append(self.ftp_timed_out)
        self.conditions.append(self.ftp_500_oops)
        self.conditions.append(self.ftp_ftplib_error_reply)
        self.conditions.append(self.network_is_unreachable)
        self.conditions.append(self.connection_timed_out)

        #construct the named tuple for the return value of this module
        self.resolution = collections.namedtuple('ErrorResolution',
                                                 ['status', 'reason', 'extra'])

        #set our internal retry dictionary to the settings.RETRY
        #in the future, retrieve it from a database or elsewhere if necessary
        self.retry = settings.RETRY

    def __find_error(self, error_message, key, status, reason, extra=None):
        '''Logic to search the error_message and return the appropriate value

        Keyword args:
        error_message  - The error_message to be searched
        key - The string to search for in the error_message
        status - The resulting status that should be set if the key is found
        reason - The user facing reason the status was returned
        extra - A dictionary with extra parameters, such as retry_after if
                the status was 'retry'

        Returns:
        An Errors.ErrorResolution() named tuple or None

        ErrorResolution.status - The status a product should be set to
        ErrorResolution.reason - The reason the status was set
        '''

        if key.lower() in error_message.lower():
            return self.resolution(status, reason, extra)
        else:
            return None

    def __add_retry(self, timeout_key, extras=dict()):
        ''' Adds retry_after to the extras dictionary based on the supplied
        timeout_key

        Keyword args:
        timeout_key - Name of timeout key defined in espa_common.settings.RETRY
        extras - The dictionary to add the retry_after value to

        Returns:
        A dictionary with retry_after populated with the datetimestamp after
        which an operation should be retried.
        '''
        timeout = self.retry[timeout_key]['timeout']
        ts = datetime.datetime.now()
        extras['retry_after'] = ts + datetime.timedelta(seconds=timeout)
        extras['retry_limit'] = self.retry[timeout_key]['retry_limit']
        return extras

    def night_scene(self, error_message):
        '''Indicates that LEDAPS could not process a scene because the
        sun was beneath the horizon'''

        key = 'solar zenith angle out of range'
        status = 'unavailable'
        reason = 'Night scenes cannot be processed to surface reflectance'
        return self.__find_error(error_message, key, status, reason)

    def missing_ledaps_aux_data(self, error_message):
        '''LEDAPS could not run because there was no aux data available'''

        key = 'Verify the missing auxillary data products'
        status = 'retry'
        reason = 'Auxillary data not yet available for this date'
        extras = self.__add_retry('missing_ledaps_aux_data')
        return self.__find_error(error_message, key, status, reason, extras)

    def ftp_timed_out(self, error_message):
        key = 'timed out|150 Opening BINARY mode data connection'
        status = 'retry'
        reason = 'FTP connection timed out'
        extras = self.__add_retry('ftp_timed_out')
        return self.__find_error(error_message, key, status, reason, extras)

    def ftp_500_oops(self, error_message):
        key = '500 OOPS'
        status = 'retry'
        reason = 'FTP experienced a 500 error'
        extras = self.__add_retry('ftp_500_oops')
        return self.__find_error(error_message, key, status, reason, extras)

    def ftp_ftplib_error_reply(self, error_message):
        key = 'ftplib.error_reply'
        status = 'retry'
        reason = 'FTP experienced an error in the reply'
        extras = self.__add_retry('ftp_ftplib_error_reply')
        return self.__find_error(error_message, key, status, reason, extras)

    def network_is_unreachable(self, error_message):
        key = 'Network is unreachable'
        status = 'retry'
        reason = 'Network error'
        extras = self.__add_retry('network_is_unreachable')
        return self.__find_error(error_message, key, status, reason, extras)

    def connection_timed_out(self, error_message):
        key = 'Connection timed out'
        status = 'retry'
        reason = 'Connection timed out'
        extras = self.__add_retry('connection_timed_out')
        return self.__find_error(error_message, key, status, reason, extras)
        
    def no_such_file_or_directory(self, error_message):
        key = 'No such file or directory'
        status = 'submitted'
        reason = 'Reordered due to online cache purge'
        return self.__find_error(error_message, key, status, reason)


def resolve(error_message):
    '''Attempts to automatically determine the disposition of a scene given
    the error_message that is supplied.

    Keyword args:
    error_message - The full error message received from the backend processing
                    node.

    Returns:
    A named tuple of espa product status code and user facing message that
    should be displayed, or None if it cannot be determined.

    Note that this method will return only the first resolution it can find,
    with the search order being defined in the Errors().conditions list.

    Example 1:
    #Night scene that contains 'solar zenith out of range' in the error_message
    result = resolve(error_message)
    print(result.status)
    'unavailable'

    print(result.reason)
    'Night scenes cannot be processed to surface reflectance'

    Example 2:
    #Cannot be determined based on the supplied message
    result = resolve(error_message)
    print(result is None)
    True

    Example 3:
    #A condition which should be retried after a configured period
    result = resolve(error_message)
    print(result.status)
    'retry'

    print(result.reason)
    'Auxillary data not yet available for this date'

    print(result.extra)
    {
    'retry_after': datetime.datetime(2014, 10, 29, 9, 48, 59, 758093),
    'retry_limit': 5
    }

    '''

    for condition in Errors().conditions:
        result = condition(error_message)
        if result is not None:
            return result
