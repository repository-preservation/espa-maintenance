
import os
import glob


def change_directory(first_dir_name=None):
    '''
    Description:
        This implements a decorator which takes a directory name as input.

        It is used to wrap all the required directory changing conventions
        and resetting code around the decorated routine.

    Input:
        first_dir_name - This is the directory to change to when it is
                         specified at decoration time and the second_dir_name
                         is not specified.
    '''

    def cd_decorator(routine_to_be_called):
        '''
        Description:
            This is the decorator which grabs the routine to be called by the
            worker.

        Input:
            routine_to_be_called - This is the routine that was decorated.  It
                                   will be executed by the worker method.
        '''

        def cd_worker(second_dir_name=None):
            '''
            Description:
                This is the worker for the decorator.  It does the setup and
                calling of the decorated routine.

            Input:
                second_dir_name - If this is specified use this as the
                                  directory to change to.  This overrides the
                                  first_dir_name.
            '''

            directory_name = None

            # Always use second first and first second, lets us override
            if second_dir_name is not None:
                directory_name = second_dir_name
            elif first_dir_name is not None:
                directory_name = first_dir_name

            # If no directory name was specified raise an exception
            if directory_name is None:
                raise Exception("Directory name not specified!")

            # Save the current directory so we can return to it
            current_directory = os.getcwd()
            print "Changing directory from [{0}] to [{1}]".format(
                  current_directory, directory_name)

            # Wrap the routine with the directory changing
            os.chdir(directory_name)
            try:
                routine_to_be_called()
            finally:
                os.chdir(current_directory)
        # END - cd_worker

        return cd_worker
    # END - cd_decorator

    return cd_decorator
# END - change_directory


# Setup with a defined directory
@change_directory("flask_espa")
def foo1():
    for item in glob.glob('*'):
        print '  ', item


# Setup for specifying a directory when called
@change_directory()
def foo2():
    for item in glob.glob('*'):
	print '  ', item


# Call with the defined directory
foo1()
# Call with a directoy specified at runtime overriding the defined directory
foo1("modis_crawler")
# Call with the defined directory
foo1()
# Call with a directoy specified at runtime
foo2("modis_crawler")
