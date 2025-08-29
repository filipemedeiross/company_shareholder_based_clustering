from django.db.models     import Q
from django.http.response import Http404
from django.views.generic import ListView

from .models import Companies, \
                    CompaniesFts
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

        if q.isdigit() and len(q) == 8:
            return queryset.filter(cnpj=q)

        fts_matches = CompaniesFts.objects.extra(
            where =['corporate_name MATCH %s'],
            params=[f'{q}*']                  ,
        ).values_list('rowid', flat=True)

        return queryset.filter(rowid__in=fts_matches)
