from django.views.generic import ListView

from .models import Companies


class CompaniesListView(ListView):
    model = Companies

    template_name       = 'companies/list.html'
    context_object_name = 'companies'

    paginate_by = 20
    ordering    = [
        'cnpj'
    ]
