import copy

class Validator(object):

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
    
    def errors(self):          
        return super(FilesValidator, self).errors()
        
                    
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
                           ['projection must be one of %s' % self.valid_projections])
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
        
    def errors(self):           
        return super(AlbersValidator, self).errors()

        
class SinusoidalValidator(Validator):
        
    def errors(self):           
        return super(AlbersValidator, self).errors()
        
        
class GeographicValidator(Validator):
        
    def errors(self):           
        return super(AlbersValidator, self).errors()
        
    
class PolarStereographicValidator(Validator):
    
    def errors(self):
      
        if not "longitudinal_origin" in self.parameters:
            msg = "longitudinal origin is required for polar stereographic"
            self.add_error('longitudinal_origin', [msg])
        
        elif self.parameters['longitudinal_origin'] == 'what':
            self.add_error('longitudinal_origin', 
                           ['longitudinal_origin was something crazy'])
        
        return super(PolarStereographicValidator, self).errors()
        

class SceneListValidator(Validator):
    
    def errors(self):
              
        if not "scenes" in self.parameters:
            self.add_error('scenes', ['scene list is required'])
        elif self.parameters['scenes'] is not 'what':
            self.add_error('scenes', ['scenes was not equal to "what"'])
        
        return super(SceneListValidator, self).errors()
        
        
class FormValidator(Validator):
        
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
        
        
        
        
        
                    
        