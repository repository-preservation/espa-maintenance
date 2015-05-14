

import os

# imports from espa_common
import utilities

# local objects and methods
from environment import Environment


# ============================================================================
def create_local_stage_directory(base_path):
    '''
    Description:
        Creates a local stage directory.

    Note: "local" in this case means a standard directory.

    Returns:
        string: The fullpath to the "stage" directory.

    Parameters:
        base_path - The location where to create the "stage" directory under.
    '''

    full_path = os.path.join(base_path, 'stage')

    utilities.create_directory(full_path)

    return full_path


# ============================================================================
def create_local_work_directory(base_path):
    '''
    Description:
        Creates a local work directory.

    Note: "local" in this case means a standard directory.

    Returns:
        string: The fullpath to the "work" directory.

    Parameters:
        base_path - The location where to create the "work" directory under.
    '''

    full_path = os.path.join(base_path, 'work')

    utilities.create_directory(full_path)

    return full_path


# ============================================================================
def create_local_output_directory(base_path):
    '''
    Description:
        Creates a local output directory.

    Note: "local" in this case means a standard directory.

    Returns:
        string: The fullpath to the "output" directory.

    Parameters:
        base_path - The location where to create the "output" directory under.
    '''

    full_path = os.path.join(base_path, 'output')

    utilities.create_directory(full_path)

    return full_path


# ============================================================================
def create_linked_output_directory(base_path, distribution_directory):
    '''
    Description:
        Creates a local output directory linked to the distribution
        directory.  Or in other words, the online cache.

    Note: "linked" in this case means a symbollic link to the online cache.

    Returns:
        string: The fullpath to the "output" link.

    Parameters:
        base_path - The location where to create the "output" link under.
    '''

    full_path = os.path.join(base_path, 'output')

    utilities.create_link(distribution_directory, full_path)

    return full_path


# ============================================================================
# API Implementation

def create_stage_directory(base_path):
    return create_local_stage_directory(base_path)


def create_work_directory(base_path):
    return create_local_work_directory(base_path)


def create_output_directory(base_path):
    '''
    Description:
        Creates either a symbolic link to the online cache or a local
        directory.

    Note: With the local method, a symbolic link is created so that we can
          just tar.gz the product and place the checksum directly on the
          product cache.
          With the remote method, we just create a directory to hold the
          tar.gz and checksum before using ftp/scp to transfer the product
          over the network.

    Returns:
        string: The fullpath to the "output" link or directory.

    Parameters:
        base_path - The location where to create the "output" link or
                    directory under.
    '''

    e = Environment()

    distribution_method = e.get_distribution_method()

    if distribution_method == 'local':
        distribution_directory = e.get_distribution_directory()
        return create_linked_output_directory(base_path,
                                              distribution_directory)
    else:
        return create_local_output_directory(base_path)
