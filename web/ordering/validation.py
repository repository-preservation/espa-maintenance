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

            if not "longitudinal_origin" in self.parameters:
                msg = "longitudinal origin is required for polar stereographic"
                self.add_error('longitudinal_origin', [msg])

            elif self.parameters['longitudinal_origin'] == 'what':
                self.add_error('longitudinal_origin',
                               ['longitudinal_origin was something crazy'])

            return super(PolarStereographicValidator, self).errors()

    The Validator also allows child Validator instances to be defined and
    attached to a parent Validator, thus allowing rich (and conditional)
    structures to be created.  All child validators are intended to be created
    by overriding the __init__() method.  An example of a Validator that
    creates a set of child Validators is as follows:

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
    return a dictionary of {parameter: ['error 1', 'error2, ... ]} allowing
    the calling code to obtain all validation errors for each originally
    supplied parameter.

    This code construct was created because Django's form validation was
    too simplistic and did not easily allow the developer to create a rich,
    conditional tree in a simple manner.
    '''

    name = ""

    parameters = {}

    validation_errors = None

    child_validators = None

    def __init__(self, parameters, child_validators=None, name=None):

        if type(parameters) is dict:
            self.parameters = parameters

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
            self.validation_errors = {key: errmsg,}
        elif key in self.validation_errors:
            self.validation_errors[key].extend(errmsg)
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
        errs = copy.deepcopy(self.validation_errors)
        self.validation_errors = None
        return errs

class FilesValidator(Validator):
    '''Example validator to check uploaded files'''

    def errors(self):
        return super(FilesValidator, self).errors()


class ProjectionValidator(Validator):
    '''Example validator to construct a set of child validators'''

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


class AlbersValidator(Validator):
    '''Example conditional child validator'''

    def errors(self):
        return super(AlbersValidator, self).errors()


class SinusoidalValidator(Validator):
    '''Example conditional child validator'''

    def errors(self):
        return super(AlbersValidator, self).errors()


class GeographicValidator(Validator):
    '''Example conditional child validator'''

    def errors(self):
        return super(AlbersValidator, self).errors()


class PolarStereographicValidator(Validator):
    '''Example conditional child validator'''

    def errors(self):

        if not "longitudinal_origin" in self.parameters:
            msg = "longitudinal origin is required for polar stereographic"
            self.add_error('longitudinal_origin', [msg])

        elif self.parameters['longitudinal_origin'] == 'what':
            self.add_error('longitudinal_origin',
                           ['longitudinal_origin was something crazy'])

        return super(PolarStereographicValidator, self).errors()


class SceneListValidator(Validator):
    '''Example validator to check scene lists'''
    def errors(self):

        if not "scenes" in self.parameters:
            self.add_error('scenes', ['scene list is required'])
        elif self.parameters['scenes'] is not 'what':
            self.add_error('scenes', ['scenes was not equal to "what"'])

        return super(SceneListValidator, self).errors()


class FormValidator(Validator):
    '''Example top-level validator that initiates validation tree construction.
    This Validator would be called as the single top level validator to be
    used from within calling code modules, such as a Django view.'''

    def __init__(self, parameters, child_validators=None, name=None):
        super(FormValidator, self).__init__(parameters, child_validators, name)

        projection = ProjectionValidator(parameters)

        scene_list = SceneListValidator(parameters)

        files = FilesValidator(parameters)

        self.add_child(files)

        self.add_child(scene_list)

        self.add_child(projection)

    def errors(self):
        return super(FormValidator, self).errors()


if __name__ == '__main__':
    form = FormValidator({'scenes':['a', 'b'],
                          'longitudinal_origin': 'abc123',
                          'projection':'ps'}, name='InputFormValidator')

    print(form.errors())

    #END example call






