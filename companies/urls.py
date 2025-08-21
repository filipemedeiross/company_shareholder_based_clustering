from django.urls import path

from .views import CompaniesListView,  \
                   CompaniesSearchView


app_name = 'companies'


urlpatterns = [
    path(''               , CompaniesListView  .as_view(), name='list'  ),
    path('search/<str:q>/', CompaniesSearchView.as_view(), name='search'),
]
