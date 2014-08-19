from django.contrib.auth.models import User
from django.contrib.messages.views import SuccessMessageMixin
from django.core.urlresolvers import reverse
from django.http import HttpResponseRedirect
from django.shortcuts import render
from django.views.generic import View
from django.views.generic.edit import FormView

from forms import StatusMessageForm
from ordering.models import Configuration

class Index(View):
    template = 'console/index.html'

    def get(self, request, *args, **kwargs):
        user = User.objects.get(username=request.user.username)
        if not user.is_staff:
            return HttpResponseRedirect(reverse('login'))

        
        return render(request, self.template)

class StatusMessage(SuccessMessageMixin, FormView):
    template_name = 'console/statusmsg.html'
    form_class = StatusMessageForm
    success_url = 'statusmsg'
    success_message = 'Status message updated'

    def get(self, request, *args, **kwargs):
        user = User.objects.get(username=request.user.username)
        if not user.is_staff:
            return HttpResponseRedirect(reverse('login'))

        return super(StatusMessage, self).get(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        user = User.objects.get(username=request.user.username)
        if not user.is_staff:
            return HttpResponseRedirect(reverse('login'))

        return super(StatusMessage, self).post(request, *args, **kwargs)

    def get_initial(self):
        return_data = {}
        try:
            title = Configuration.objects.get(key="system_message_title")
            return_data['title'] = title.value
        except Configuration.DoesNotExist:
            return_data['title'] = ''

        try:
            message = Configuration.objects.get(key="system_message_body")
            return_data['message'] = message.value
        except Configuration.DoesNotExist:
            return_data['message'] = ''

        try:
            display = Configuration.objects.get(key="display_system_message")
            if display.value.lower() == 'true':
                return_data['display'] = True
            else:
                return_data['display'] = False
        except Configuration.DoesNotExist:
            return_data['display'] = False

        return return_data

    def form_valid(self, form):
        title, created = Configuration.objects.get_or_create(key="system_message_title")
        title.value = form.cleaned_data['title']
        title.save()

        message, created = Configuration.objects.get_or_create(key="system_message_body")
        message.value = form.cleaned_data['message']
        message.save()

        display, created = Configuration.objects.get_or_create(key="display_system_message")
        display.value = form.cleaned_data['display']
        display.save()
        
        return super(StatusMessage, self).form_valid(form)
