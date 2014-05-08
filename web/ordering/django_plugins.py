from ordering.models import UserProfile
from lta import RegistrationServiceClient
from django.contrib.auth.models import User


class EEAuthBackend(object):
    ''' 
    Django authentication system plugin to authenticate against the
    Earth Explorer Registration Service.
    
    Once authenticated, if the user does not exist in Django it will be created.  This is 
    necessary for Django to enforce authentication & authorization as well as capturing
    user related info such as the EE contact id.
    '''
    
    def authenticate(self, username=None, password=None):

        try:
            contactid = RegistrationServiceClient().login_user(username, password)
    
            try:
                user = User.objects.get(username=username)
            except User.DoesNotExist:
                # Create a new user. Note that we can set password
                # to anything, because it won't be checked; the password
                # from RegistrationServiceClient will.
                user = User(username=username, password='this value isnt used')
                user.is_staff = False
                user.is_superuser = False
                user.save()
                
                UserProfile(contactid = contactid, user = user).save()                            
            return user
        except Exception, e:            
            return None
    
    

    def get_user(self, user_id):
        try:
            return User.objects.get(pk=user_id)
        except User.DoesNotExist:
            return None
