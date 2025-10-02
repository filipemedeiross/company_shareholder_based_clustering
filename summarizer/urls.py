from django.urls import path

from .views import dashboard


app_name = 'summarizer'


urlpatterns = [
    path('', dashboard, name='dashboard'),
]
