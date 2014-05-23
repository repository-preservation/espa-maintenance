from django.conf import settings


def include_external_urls(request):
    '''Django context processor to include needed urls in the
    request contexts.  This method must be included in the
    list of TEMPLATE_CONTEXT_PROCESSORS in settings.py to be active.

    Keyword args:
    request -- HTTP request object

    Return:
    A dictionary of values to be included in the request context
    '''
    context = {}
    context['register_user'] = settings.URL_FOR('register_user')
    context['forgot_login'] = settings.URL_FOR('forgot_login')
    return context
