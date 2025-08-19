from django.views.generic import ListView

from .models    import Companies
from .constants import COMPANIES_LIST_PAGINATE, \
                       COMPANIES_LIST_CONTEXT


class CompaniesListView(ListView):
    model = Companies

    template_name       = 'companies/list.html'
    context_object_name = COMPANIES_LIST_CONTEXT

    paginate_by = COMPANIES_LIST_PAGINATE
    ordering    = [
        'cnpj'
    ]
