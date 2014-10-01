import copy


class Validator(object):
    '''Superclass to create the logic of the validation framework.  The
    validation framework defines a method to include validation code for
    specific cases in a consistent manner.  In the simplest case,
    users of this framework subclass Validator and override the
    errors() method.  Inside the errors method the code should look for
    target dictionary values and perform the necessary validation.  If errors
    are found they should be added by calling self.add_error().  Each subclass
    must return super(sub_class, sub_class_instance).errors().  Thus a real
    example of this is as follows:

    class PolarStereographicValidator(Validator):

        def errors(self):

            if not "longitudinal_pole" in self.parameters:
                msg = "longitudinal pole is required for polar stereographic"
                self.add_error('longitudinal_pole', [msg])

            elif self.parameters['longitudinal_pole'] == 'what':
                self.add_error('longitudinal_pole',
                               ['longitudinal_pole was something crazy'])

            return super(PolarStereographicValidator, self).errors()

    The Validator also allows child Validator instances to be defined and
    attached to a parent Validator, thus enabling rich (and conditional)
    structures to be created.  The Validator superclass handles calling each
    child attached to the tree when the errors() method is invoked on its
    parent.

    All child validators are intended to be constructed
    by overriding the __init__() method in its parent Validator.

    An example of a Validator that creates a set of child Validators follows:

    class ProjectionValidator(Validator):

        valid_projections = ['aea', 'ps', 'sinu', 'longlat']

        def __init__(self, parameters, child_validators=None, name=None):
            # delegate the call to superclass since we are overriding the
            # __init__ method
            super(ProjectionValidator, self).__init__(parameters,
                                                      child_validators,
                                                      name)

            # check for projection value and add appropriate child validators
            proj = None

            if not 'projection' in parameters:
                self.add_error("projection", ['projection must be specified'])
            else:
                proj = parameters['projection']

            if proj and proj not in self.valid_projections:

                self.add_error("projection",
                               ['projection must be one of %s'
                                   % self.valid_projections])
            else:
                if proj is 'aea':
                    self.add_child(AlbersValidator(parameters))
                elif proj is 'ps':
                    self.add_child(PolarStereographicValidator(parameters))
                elif proj is 'sinu':
                    self.add_child(SinusoidalValidator(parameters))
                elif proj is 'longlat':
                    self.add_child(GeographicValidator(parameters))

         def errors(self):
            return super(ProjectionValidator, self).errors()

    This Validator performs no validation of its own, it simply constructs
    the correct set of child validators depending on the supplied parameters.

    From a usage perspective, there is only one applicable method that should
    be called to find validators errors, which is the errors() method.

    Once the Validation tree is instantiated, users call errors() which will
    return a dictionary of {parameter: ['error 1', 'error2', ... ]} allowing
    the calling code to obtain all validation errors for each originally
    supplied parameter.

    This code construct was created because Django's form validation was
    too simplistic and did not easily allow the developer to create
    a conditional, cascading tree.
    '''

    name = ""

    parameters = {}

    validation_errors = None

    child_validators = None

    def __init__(self, parameters, child_validators=None, name=None):

        if type(parameters) is dict:
            self.parameters = parameters
        else:
            raise Exception("parameters was of type %s, dict required"
                            % type(parameters))

        if child_validators:
            self.child_validators = child_validators

        if name:
            self.name = name

    def __repr__(self):

        if self.name and len(self.name) > 0:
            return self.name
        else:
            return str(self.__class__)

    def add_child(self, child_validator):

        if not self.child_validators:
            self.child_validators = list()

        self.child_validators.append(child_validator)

    def has_children(self):

        return hasattr(self, 'child_validators') \
            and type(self.child_validators) is list \
            and len(self.child_validators) > 0

    def add_error(self, key, errmsg):
        if not self.validation_errors:
            self.validation_errors = {key: errmsg, }
        elif key in self.validation_errors:
            self.validation_errors[key].extend(errmsg)
            # do this to prevent the list from continuing to grow
            errs = set(self.validation_errors[key])
            self.validation_errors[key] = list(errs)
        else:
            self.validation_errors[key] = errmsg

    def errors(self):
        if self.has_children():
            for v in self.child_validators:
                errs = v.errors()
                if errs:
                    for key, value in errs.iteritems():
                        self.add_error(key, value)

        # make a copy of the dictionary and return the copy
        # then wipe out the original object
        # otherwise we just keep adding more and more
        # values to the dict every time errors() is run
        #errs = copy.deepcopy(self.validation_errors)
        #self.validation_errors = None
        #return errs
        return self.validation_errors


class ExampleFilesValidator(Validator):
    '''Example validator to check uploaded files'''

    def errors(self):
        '''This example does nothing'''
        return super(ExampleFilesValidator, self).errors()


class ExampleProjectionValidator(Validator):
    '''Example validator to construct a set of child validators'''

    valid_projections = ['aea', 'ps', 'sinu', 'longlat']

    def __init__(self, parameters, child_validators=None, name=None):
        '''Conditionally build and attach child validators'''
        # delegate the call to superclass since we are overriding the
        # __init__ method
        super(ExampleProjectionValidator, self).__init__(parameters,
                                                         child_validators,
                                                         name)

        # check for projection value and add appropriate child validators
        proj = None

        if not 'projection' in parameters:
            self.add_error("projection", ['projection must be specified'])
        else:
            proj = parameters['projection']

        if proj and proj not in self.valid_projections:

            self.add_error("projection",
                           ['projection must be one of %s'
                               % self.valid_projections])
        else:
            if proj is 'aea':
                self.add_child(ExampleAlbersValidator(parameters))
            elif proj is 'ps':
                self.add_child(ExamplePolarStereographicValidator(parameters))
            elif proj is 'sinu':
                self.add_child(ExampleSinusoidalValidator(parameters))
            elif proj is 'longlat':
                self.add_child(ExampleGeographicValidator(parameters))

    def errors(self):
        '''No actual validation happening in this validator'''
        return super(ExampleProjectionValidator, self).errors()


class ExampleAlbersValidator(Validator):
    '''Example conditional child validator'''

    def errors(self):
        '''This validator does nothing'''
        return super(ExampleAlbersValidator, self).errors()


class ExampleSinusoidalValidator(Validator):
    '''Example conditional child validator'''

    def errors(self):
        '''This validator does nothing'''
        return super(ExampleAlbersValidator, self).errors()


class ExampleGeographicValidator(Validator):
    '''Example conditional child validator'''

    def errors(self):
        '''This validator does nothing'''
        return super(ExampleAlbersValidator, self).errors()


class ExamplePolarStereographicValidator(Validator):
    '''Example conditional child validator'''

    def errors(self):
        '''This validator looks for a needed key and examines its value,
        reporting errors where appropriate'''

        if not "longitudinal_pole" in self.parameters:
            msg = "longitudinal pole is required for polar stereographic"
            self.add_error('longitudinal_pole', [msg])

        elif self.parameters['longitudinal_pole'] == 'what':
            self.add_error('longitudinal_pole',
                           ['longitudinal_pole was something crazy'])

        return super(ExamplePolarStereographicValidator, self).errors()


class ExampleSceneListValidator(Validator):
    '''Example validator to check scene lists'''

    def errors(self):
        '''This validator looks for a needed key and examines its value,
        reporting errors where appropriate'''

        if not "scenes" in self.parameters:
            self.add_error('scenes', ['scene list is required'])
        elif self.parameters['scenes'] is not 'what':
            self.add_error('scenes', ['scenes was not equal to "what"'])

        return super(ExampleSceneListValidator, self).errors()


class ExampleFormValidator(Validator):
    '''Example top-level validator that initiates validation tree construction.
    This Validator would be called as the single top level validator to be
    used from within calling code modules, such as a Django view.'''

    def __init__(self, parameters, child_validators=None, name=None):
        '''This validator builds a adds several child validators that would
        be part of an HTML form submission.'''

        super(ExampleFormValidator, self).__init__(parameters,
                                                   child_validators,
                                                   name)

        self.add_child(ExampleFilesValidator(parameters))

        self.add_child(ExampleSceneListValidator(parameters))

        self.add_child(ExampleProjectionValidator(parameters))

    def errors(self):
        '''Trigger the child validators by overriding the error() method
        and calling the error() method defined in Validator superclass'''

        return super(ExampleFormValidator, self).errors()


if __name__ == '__main__':
    # This will complain due to the value of the 'scenes' parameter...
    # This is intentional to demonstrate the return value of the call to
    # errors()
    form = ExampleFormValidator({'scenes': ['a', 'b'],
                                 'longitudinal_pole': 'abc123',
                                 'projection': 'ps'})

    print("--------------------------------------------")
    print("Example call to validator tree with an error")
    print("--------------------------------------------")
    print(form.errors())

    # This will demonstrate a call to validator in which no validation
    # errors occurred
    form = ExampleFormValidator({'scenes': 'what',
                                 'longitudinal_pole': 'abc123',
                                 'projection': 'ps'})

    print("")
    print("---------------------------------------------")
    print("Example call to validator tree with no errors")
    print("---------------------------------------------")
    # This completes with no validation errors
    print(form.errors())

    #END example call
