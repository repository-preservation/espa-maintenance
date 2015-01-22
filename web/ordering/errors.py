import collections
from espa_common import settings
import datetime


class Errors(object):
    '''Implementation for ESPA errors.resolve(error_message) interface'''

    def __init__(self):
        #build list of known error conditions to be checked
        self.conditions = list()

        self.conditions.append(self.connection_aborted)
        self.conditions.append(self.connection_timed_out)
        self.conditions.append(self.db_lock_errors)
        self.conditions.append(self.ftp_timed_out)
        self.conditions.append(self.ftp_500_oops)
        self.conditions.append(self.ftp_ftplib_error_reply)
        self.conditions.append(self.gzip_errors)
        self.conditions.append(self.gzip_errors_eof)
        self.conditions.append(self.http_not_found)
        self.conditions.append(self.incomplete_read)
        self.conditions.append(self.missing_ledaps_aux_data)
        self.conditions.append(self.missing_l8sr_aux_data)
        self.conditions.append(self.network_is_unreachable)
        self.conditions.append(self.night_scene)
        self.conditions.append(self.night_scene2)
        self.conditions.append(self.oli_no_sr)
        self.conditions.append(self.proxy_error_502)
        self.conditions.append(self.ssh_errors)

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

    def ssh_errors(self, error_message):
        ''' errors creating directories or transferring statistics '''
        key = ('Application failed to execute '
               '[ssh -q -o StrictHostKeyChecking=no')
        status = 'retry'
        reason = 'ssh operations interrupted'
        extras = self.__add_retry('ssh_errors')
        return self.__find_error(error_message, key, status, reason, extras)

    def connection_aborted(self, error_message):
        ''' level 1 http download interrupted '''
        key = 'Connection aborted.'
        status = 'retry'
        reason = 'level 1 product download interrupted'
        extras = self.__add_retry('connection_aborted')
        return self.__find_error(error_message, key, status, reason, extras)

    def incomplete_read(self, error_message):
        ''' http read was interrupted '''
        key = 'Connection broken: IncompleteRead'
        status = 'retry'
        reason = 'incomplete read on input data'
        extras = self.__add_retry('incomplete_read')
        return self.__find_error(error_message, key, status, reason, extras)

    def proxy_error_502(self, error_message):
        ''' a service call was interrupted, most likely due to restart '''
        key = '502 Server Error: Proxy Error'
        status = 'retry'
        reason = 'internal service was restarted (502)'
        extras = self.__add_retry('502_proxy_error')
        return self.__find_error(error_message, key, status, reason, extras)

    def db_lock_errors(self, error_message):
        ''' there were problems updating the database '''
        key = 'Lock wait timeout exceeded'
        status = 'retry'
        reason = 'database lock timed out'
        extras = self.__add_retry('db_lock_timeout')
        return self.__find_error(error_message, key, status, reason, extras)

    def gzip_errors(self, error_message):
        ''' there were problems gzipping products '''
        key = 'not in gzip format'
        status = 'retry'
        reason = 'error unpacking gzip'
        extras = self.__add_retry('gzip_format_error')
        return self.__find_error(error_message, key, status, reason, extras)

    def gzip_errors_eof(self, error_message):
        ''' file may be corrupt '''
        key = 'gzip: stdin: unexpected end of file'
        status = 'retry'
        reason = 'gzip unexpected EOF'
        extras = self.__add_retry('gzip_error_eof')
        return self.__find_error(error_message, key, status, reason, extras)

    def oli_no_sr(self, error_message):
        ''' Indicates the user requested sr processing against OLI-only'''

        key = 'oli-only cannot be corrected to surface reflectance'
        status = 'unavailable'
        reason = 'OLI only scenes cannot be processed to surface reflectance'
        return self.__find_error(error_message, key, status, reason)

    def night_scene(self, error_message):
        '''Indicates that LEDAPS/l8sr could not process a scene because the
        sun was beneath the horizon'''

        key = 'solar zenith angle out of range'
        status = 'unavailable'
        reason = ('This scene cannot be processed to surface reflectance '
                  'due to the high solar zenith angle')
        return self.__find_error(error_message, key, status, reason)

    def night_scene2(self, error_message):
        '''Indicates that LEDAPS/l8sr could not process a scene because the
        sun was beneath the horizon'''

        key = 'Solar zenith angle is out of range'
        status = 'unavailable'
        reason = ('This scene cannot be processed to surface reflectance '
                  'due to the high solar zenith angle')
        return self.__find_error(error_message, key, status, reason)

    def http_not_found(self, error_message):
        '''Indicates that we had an issue trying to download the product'''
        key = '404 Client Error: Not Found'
        status = 'retry'
        reason = 'HTTP 404 for input product, retrying download'
        extras = self.__add_retry('http_not_found')
        return self.__find_error(error_message, key, status, reason, extras)

    def missing_ledaps_aux_data(self, error_message):
        '''LEDAPS could not run because there was no aux data available'''

        key = 'Verify the missing auxillary data products'
        status = 'retry'
        reason = 'Auxillary data not yet available for this date'
        extras = self.__add_retry('missing_ledaps_aux_data')
        return self.__find_error(error_message, key, status, reason, extras)

    def missing_l8sr_aux_data(self, error_message):
        '''L8SR could not run because there was no aux data available'''

        key = 'Could not find auxnm data file:'
        status = 'retry'
        reason = 'Auxillary data not yet available for this date'
        extras = self.__add_retry('missing_l8sr_aux_data')
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

    conditions = None
    result = None
    try:
        conditions = Errors().conditions
        for condition in conditions:
            result = condition(error_message)
            if result is not None:
                return result
        else:
            return None
    finally:
        conditions = None
        result = None
