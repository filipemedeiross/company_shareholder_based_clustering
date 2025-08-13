from django.urls import path

from .views import CompaniesListView


app_name = 'companies'


urlpatterns = [
    path('', CompaniesListView.as_view(), name='list'),
]
