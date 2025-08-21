from django.db.models     import Q
from django.http.response import Http404
from django.views.generic import ListView

from .models    import Companies
from .constants import COMPANIES_LIST_PAGINATE, \
                       COMPANIES_LIST_CONTEXT


class CompaniesListView(ListView):
    model = Companies

    template_name       = 'companies/list.html'
    context_object_name = COMPANIES_LIST_CONTEXT

    paginate_by = COMPANIES_LIST_PAGINATE
    ordering    = ['cnpj']


class CompaniesSearchView(CompaniesListView):
    template_name = 'companies/search.html'

    def get_queryset(self):
        q = self.request.GET.get('q', '')

        if not q:
            raise Http404

        queryset = super().get_queryset()

        return queryset.filter(
            Q(
                Q(cnpj__startswith         =q) |
                Q(corporate_name__icontains=q)
            )
        )
