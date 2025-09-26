from django.urls          import reverse
from django.contrib       import messages
from django.views.generic import ListView
from django.shortcuts     import get_object_or_404

from .models import Business , \
                    Partners , \
                    Companies, \
                    CompaniesFts
from .paginator import CursorPaginator
from .constants import COMPANIES_LIST_PAGINATE, \
                       COMPANIES_LIST_CONTEXT


class CompaniesBaseView(ListView):
    model = Companies

    paginate_by = None
    ordering    = 'cnpj'

    context_object_name = COMPANIES_LIST_CONTEXT
    per_page            = COMPANIES_LIST_PAGINATE

    def get_context_data(self, **kwargs):
        context = {**kwargs}

        paginator = CursorPaginator(
            self.get_queryset()   ,
            self.per_page         ,
            ordering=self.ordering,
        )
        page = paginator.page(
            after =self.request.GET.get("after" ),
            before=self.request.GET.get("before"),
        )

        context.update({
            "page_obj"     : page,
            "is_paginated" : page.has_next or page.has_previous,
            self.context_object_name : page.object_list,
        })

        context.update({
            "form_action"  : reverse("companies:search"),
            "search_query" : self.search_query          ,
        })

        return context

    @property
    def search_query(self):
        return self.request.GET.get('q', '').strip()


class CompaniesListView(CompaniesBaseView):
    template_name = 'companies/list.html'


class CompaniesSearchView(CompaniesBaseView):
    template_name = 'companies/search.html'

    def get(self, request, *args, **kwargs):
        if not self.search_query:
            messages.error(
                request,
                'Please fill in the search field.'
            )

        return super().get(request, *args, **kwargs)

    def get_queryset(self):
        queryset = super().get_queryset()

        q = self.search_query
        if not q:
            return queryset.none()

        if q.isdigit() and len(q) == 8:
            return queryset.filter(cnpj=q)

        fts_matches = CompaniesFts.objects.extra(
            where =['corporate_name MATCH %s'],
            params=[f'{q}*']                  ,
        ).values_list('rowid', flat=True)

        return queryset.filter(rowid__in=fts_matches)


class CompaniesDetailView(ListView):
    template_name = 'companies/detail.html'
    context_object_name = 'establishments'

    def get_queryset(self):
        cnpj = self.kwargs.get('cnpj')
        cnpj_base = cnpj[:8]
        return Companies.objects.filter(cnpj=cnpj_base)

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        cnpj = self.kwargs.get('cnpj')
        company = get_object_or_404(Companies, cnpj=cnpj)
        cnpj_base = cnpj[:8]
        partners = Partners.objects.filter(cnpj_base=cnpj_base)
        context['company'] = company
        context['partners'] = partners
        context['cnpj_base'] = cnpj_base
        return context

'''
# ...existing code...
from .views import CompaniesDetailView
# ...existing code...

urlpatterns = [
    # ...existing code...
    path('detail/<str:cnpj>/', CompaniesDetailView.as_view(), name='detail'),
]

# ...existing code...
<a href="{% url 'companies:detail' cnpj=company.cnpj %}">{{ company.cnpj }}</a>

get_absolute_url nos models (seria interessante)?
'''
