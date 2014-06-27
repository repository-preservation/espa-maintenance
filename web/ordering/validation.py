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
            return self.__class__
            
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
                      
        if not "x" in self.parameters:
            self.add_error('x', ['x was not found'])
        elif self.parameters['x'] == 'x':
            self.add_error('x', ['x was equal to itself'])
                        
        return super(FilesValidator, self).errors()
        
                    
class Child(Validator):
                
    def errors(self):
                
        if not "y" in self.parameters:
            self.add_error('y', ['y was not found'])
        elif not self.parameters['y'] == 'KABOOM':
            self.add_error('y', ['y was NOT KaBOOM'])
        
        
        return super(Child, self).errors()               
        

class Child2(Validator):
        
    def errors(self):
               
        if not "z" in self.parameters:
            self.add_error('z', ['z was not found'])
        elif not "z" == "your mother":
            self.add_error('z', ['z was not equal to your mother'])
            
        return super(Child2, self).errors()
        
    
class Child2_1(Validator):
    
    def errors(self):
              
        if not "z" in self.parameters or not self.parameters['z'] == 'what':
            self.add_error('z', ['z was not equal to what'])
        
        return super(Child2_1, self).errors()
        
        
class FormValidator(Validator):
        
    def __init__(self, parameters, child_validators=None, name=None):
        super(FormValidator, self).__init__(parameters, child_validators, name)
        
        c2 = Child2(parameters, name="Child2 Validator")
        
        c2_1 = Child2_1(parameters, name="Child2_1 Validator")
        
        c1 = Child(parameters, name="Child Validator")
        
        p = FilesValidator(parameters, name="Files Validator")
        
        c1.add_child(c2)
        c1.add_child(c2_1)
        
        p.add_child(c1)
        
        self.add_child(p)
        
    def errors(self):
        errors = super(FormValidator, self).errors()
        return errors
        
        
        
        
        
                    
        