from django.urls import reverse
from django.http.response import Http404
from django.views.generic import ListView

from .models import Companies, \
                    CompaniesFts
from .constants import COMPANIES_LIST_PAGINATE, \
                       COMPANIES_LIST_CONTEXT


class CompaniesBaseView(ListView):
    model = Companies

    context_object_name = COMPANIES_LIST_CONTEXT
    paginate_by         = COMPANIES_LIST_PAGINATE

    ordering = ['cnpj']

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)

        context['form_action' ] = reverse('companies:search')
        context['search_query'] = self.request.GET.get('q', '')

        return context


class CompaniesListView(CompaniesBaseView):
    template_name = 'companies/list.html'


class CompaniesSearchView(CompaniesBaseView):
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
