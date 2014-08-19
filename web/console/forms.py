
from django import forms

class StatusMessageForm(forms.Form):
    title = forms.CharField(label='Title', required=False, max_length=255)
    
    message = forms.CharField(label='Message',
                              widget=forms.Textarea,
                              required=False)
                              
    display = forms.BooleanField(label='Publish Message', required=False)
