from django.conf import settings
from django.contrib.auth.models import User, check_password
from lta import RegistrationServiceClient

class EEAuthBackend(object):
    ''' 
    Authenticate against the Earth Explorer Registration Service .

    Use the login name, and a hash of the password. For example:
    '''
    
    def authenticate(self, username=None, password=None):

        #login_valid = (settings.ADMIN_LOGIN == username)

        #pwd_valid = check_password(password, settings.ADMIN_PASSWORD)

        contactid = RegistrationServiceClient().login(username, password)
    
        if contactid:
            try:
                user = User.objects.get(username=username)
            except User.DoesNotExist:
                # Create a new user. Note that we can set password
                # to anything, because it won't be checked; the password
                # from RegistrationServiceClient will.
                user = User(username=username, password='this value isnt used')
                user.is_staff = False
                user.is_superuser = False
                user.profile.contactid = contactid
                user.save()
            return user
        return None

    def get_user(self, user_id):
        try:
            return User.objects.get(pk=user_id)
        except User.DoesNotExist:
            return None
