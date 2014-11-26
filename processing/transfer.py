
'''
License:
  "NASA Open Source Agreement 1.3"

Description:
  Provides routines for transfering files.

History:
  Created Jan/2014 by Ron Dilley, USGS/EROS
'''

import ftplib
import urllib
import requests
from contextlib import closing

# imports from espa_common through processing.__init__.py
from processing import EspaLogging
from processing import settings
from processing import utilities


# ============================================================================
def copy_file_to_file(source_file, destination_file):
    '''
    Description:
      Use unix 'cp' to copy a file from one place to another on the localhost.
    '''

    logger = EspaLogging.get_logger(settings.PROCESSING_LOGGER)

    cmd = ' '.join(['cp', source_file, destination_file])

    # Transfer the data and raise any errors
    output = ''
    try:
        output = utilities.execute_cmd(cmd)
    except Exception, e:
        logger.error("Failed to copy file")
        raise e
    finally:
        if len(output) > 0:
            logger.info(output)

    logger.info("Transfer complete - CP")
# END - copy_file_to_directory


# ============================================================================
def remote_copy_file_to_file(source_host, source_file, destination_file):
    '''
    Description:
      Use unix 'cp' to copy a file from one place to another on a remote
      machine using ssh.
    '''

    logger = EspaLogging.get_logger(settings.PROCESSING_LOGGER)

    cmd = ' '.join(['ssh', '-q', '-o', 'StrictHostKeyChecking=no',
                    source_host, 'cp', source_file, destination_file])

    # Transfer the data and raise any errors
    output = ''
    try:
        output = utilities.execute_cmd(cmd)
    except Exception, e:
        logger.error("Failed to copy file")
        raise e
    finally:
        if len(output) > 0:
            logger.info(output)

    logger.info("Transfer complete - SSH-CP")
# END - remote_copy_file_to_directory


# ============================================================================
def ftp_from_remote_location(username, pw, host, remotefile, localfile):
    '''
    Author: David Hill

    Date: 12/5/13

    Description:
      Transfers files from a remote location to the local machine using ftplib.

    Parameters:
      username = Username for ftp account
      pw = Password for ftp account
      host = The ftp server host
      remotefile = The file to transfer
      localfile = Full path to where the local file should be created.
                  (Parent directories must exist)

    Returns: None

    Errors: Raises Exception() in the event of error
    '''

    logger = EspaLogging.get_logger(settings.PROCESSING_LOGGER)

    # Make sure the src_file is absolute, otherwise ftp will choke
    if not remotefile.startswith('/'):
        remotefile = ''.join(['/', remotefile])

    pw = urllib.unquote(pw)

    url = 'ftp://%s/%s' % (host, remotefile)

    logger.info("Transferring file from %s to %s" % (url, localfile))
    ftp = None
    try:
        with open(localfile, 'wb') as loc_file:
            def callback(data):
                loc_file.write(data)

            ftp = ftplib.FTP(host, timeout=60)
            ftp.login(user=username, passwd=pw)
            ftp.set_debuglevel(0)
            ftp.retrbinary(' '.join(['RETR', remotefile]), callback)

    finally:
        if ftp:
            ftp.quit()

    logger.info("Transfer complete - FTP")
# END - ftp_from_remote_location


# ============================================================================
def ftp_to_remote_location(username, pw, localfile, host, remotefile):
    '''
    Author: David Hill

    Date: 12/5/13

    Description:
      Transfers files from the local machine to a remote location using ftplib.

    Parameters:
      username = Username for ftp account
      pw = Password for ftp account
      host = The ftp server host
      remotefile = Full path of where to store the file
                   (Directories must exist)
      localfile = Full path of file to transfer out

    Returns: None

    Errors: Raises Exception() in the event of error
    '''

    logger = EspaLogging.get_logger(settings.PROCESSING_LOGGER)

    # Make sure the src_file is absolute, otherwise ftp will choke
    if not remotefile.startswith('/'):
        remotefile = ''.join(['/', remotefile])

    pw = urllib.unquote(pw)

    logger.info("Transferring file from %s to %s"
                % (localfile, 'ftp://%s/%s' % (host, remotefile)))

    ftp = None

    try:
        logger.info("Logging in to %s with %s:%s" % (host, username, pw))
        ftp = ftplib.FTP(host, user=username, passwd=pw, timeout=60)
        with open(localfile, 'rb') as tmp_fd:
            ftp.storbinary(' '.join(['STOR', remotefile]), tmp_fd, 1024)
    finally:
        if ftp:
            ftp.quit()

    logger.info("Transfer complete - FTP")
# END - ftp_to_remote_location


# ============================================================================
def scp_transfer_file(source_host, source_file,
                      destination_host, destination_file):
    '''
    Description:
      Using SCP transfer a file from a source location to a destination
      location.

    Note:
      - It is assumed ssh has been setup for access between the localhost
        and destination system
      - If wild cards are to be used with the source, then the destination
        file must be a directory.  ***No checking is performed in this code***
    '''

    logger = EspaLogging.get_logger(settings.PROCESSING_LOGGER)

    if source_host == destination_host:
        msg = "source and destination host match unable to scp"
        logger.error(msg)
        raise Exception(msg)

    cmd = ['scp', '-q', '-o', 'StrictHostKeyChecking=no', '-c', 'arcfour',
           '-C']

    # Build the source portion of the command
    # Single quote the source to allow for wild cards
    if source_host == 'localhost':
        cmd.append(source_file)
    else:
        cmd.append("'%s:%s'" % (source_host, source_file))

    # Build the destination portion of the command
    if destination_host == 'localhost':
        cmd.append(destination_file)
    else:
        cmd.append('%s:%s' % (destination_host, destination_file))

    cmd = ' '.join(cmd)

    # Transfer the data and raise any errors
    output = ''
    try:
        output = utilities.execute_cmd(cmd)
    except Exception, e:
        if len(output) > 0:
            logger.info(output)
        logger.error("Failed to transfer data")
        raise e

    logger.info("Transfer complete - SCP")
# END - scp_transfer_file


# ============================================================================
def http_transfer_file(download_url, destination_file):
    '''
    Description:
      Using http transfer a file from a source location to a destination
      file on the localhost.
    '''

    logger = EspaLogging.get_logger(settings.PROCESSING_LOGGER)

    logger.info(download_url)

    file_size = 0
    retrieved_bytes = 0
    with closing(requests.get(download_url, stream=True)) as req:
        if not req.ok:
            raise Exception("Transfer Failed - HTTP - Reason(%s)"
                            % req.reason)

        file_size = int(req.headers['content-length'])

        with open(destination_file, 'wb') as local_fd:
            for data_chunk in req.iter_content(settings.TRANSFER_BLOCK_SIZE):
                local_fd.write(data_chunk)
                retrieved_bytes += len(data_chunk)

    if retrieved_bytes != file_size:
        raise Exception("Transfer Failed - HTTP - Retrieved %d out of %d bytes"
                        % (retrieved_bytes, file_size))
    else:
        logger.info("Transfer complete - HTTP")
# END - http_transfer_file


# ============================================================================
def download_file_url(download_url, destination_file):
    '''
    Description:
        Using a URL download the specified file to the destination.
    '''

    if download_url.startswith('http://'):
        http_transfer_file(download_url, destination_file)
    elif download_url.startswith('file://'):
        source_file = download_url.replace('file://', '')
        transfer_file('localhost', source_file, 'localhost', destination_file)
    else:
        raise Exception("Transfer Failed -"
                        " Unknown URL transport protocol [%s]"
                        % download_url)
# END - download_file_url


# ============================================================================
def transfer_file(source_host, source_file,
                  destination_host, destination_file,
                  source_username=None, source_pw=None,
                  destination_username=None, destination_pw=None):
    '''
    Description:
      Using cp/FTP/SCP transfer a file from a source location to a destination
      location.

    Notes:
      We are not doing anything significant here other then some logic and
      fallback to SCP if FTP fails.
    '''

    logger = EspaLogging.get_logger(settings.PROCESSING_LOGGER)

    logger.info("Transfering [%s:%s] to [%s:%s]"
                % (source_host, source_file,
                   destination_host, destination_file))

    # If both source and destination are localhost we can just copy the data
    if source_host == 'localhost' and destination_host == 'localhost':
        copy_file_to_file(source_file, destination_file)
        return

    # If both source and destination hosts are the same, we can use ssh to copy
    # the files locally on the remote host
    if source_host == destination_host:
        remote_copy_file_to_file(source_host, source_file, destination_file)
        return

    # Try FTP first before SCP if usernames and passwords are provided
    if source_username is not None and source_pw is not None:
        try:
            ftp_from_remote_location(source_username, source_pw, source_host,
                                     source_file, destination_file)
            return
        except Exception, e:
            logger.warning("FTP failures will attempt transfer using SCP")
            logger.warning("FTP Errors: %s" % str(e))

    elif destination_username is not None and destination_pw is not None:
        try:
            ftp_to_remote_location(destination_username, destination_pw,
                                   source_file, destination_host,
                                   destination_file)
            return
        except Exception, e:
            logger.warning("FTP failures will attempt transfer using SCP")
            logger.warning("FTP Errors: %s" % str(e))

    # As a last resort try SCP
    scp_transfer_file(source_host, source_file,
                      destination_host, destination_file)
# END - transfer_file
