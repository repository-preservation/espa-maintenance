

import os


class Environment(object):
    '''
    Description:
        Provides consistent access to the environment variables, so that
        they do not need to be retrieved everywhere they are used.
    '''

    _keys = None

    # --------------------------------------
    def __init__(self):
        '''
        Description:
            Provides the initialization of the object and the internally
            stored environment variable values.

            Validation is called to verify the variables and store the values
            internally.
        '''

        self._keys = \
            {'distribution_method': {'env_var': 'ESPA_DISTRIBUTION_METHOD',
                                     'required': True,
                                     'valid_values': ['local', 'remote'],
                                     'value': None},
             'distribution_directory': {'env_var': 'ESPA_DISTRIBUTION_DIR',
                                        'required': False,
                                        'valid_values': None,
                                        'value': None},
             'base_work_directory': {'env_var': 'ESPA_WORK_DIR',
                                     'required': False,
                                     'valid_values': None,
                                     'value': None}}

        self.validate_environment()

    # --------------------------------------
    def validate_environment(self):
        '''
        Description:
            Processes through the Specified environment dictionary and
            validates as-well-as stores the values for each variable.
        '''

        for key in self._keys:
            obj = self._keys[key]
            env_var = obj['env_var']
            valid_values = obj['valid_values']

            # Check that required variables are present in the environment
            if (obj['required'] and (env_var not in os.environ)):
                raise Exception("Environment variable ${0} is not defined".
                                format(env_var))

            env_value = os.environ.get(env_var, '')

            # If it has a strict set of values, verify that it is set to one
            # of them
            if valid_values is not None:
                if env_value not in valid_values:
                    raise Exception("Invalid ${0} value was '{1}',"
                                    " but should be one of {2}".
                                    format(env_var, env_value, valid_values))

            obj['value'] = env_value

    # --------------------------------------
    def get_distribution_method(self):
        return self._keys['distribution_method']['value']

    # --------------------------------------
    def get_distribution_directory(self):
        return self._keys['distribution_directory']['value']

    # --------------------------------------
    def get_base_work_directory(self):
        return self._keys['base_work_directory']['value']
