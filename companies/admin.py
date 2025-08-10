from django.contrib import admin

from .models import Companies  , \
                    Partners   , \
                    Business   , \
                    PartnersFts, \
                    BusinessFts


@admin.register(Companies)
class CompaniesAdmin(admin.ModelAdmin):
    list_display  = ('cnpj', 'corporate_name', 'capital')
    search_fields = ('cnpj', 'corporate_name',)

    list_per_page = 20


@admin.register(Partners)
class PartnersAdmin(admin.ModelAdmin):
    list_display  = ('cnpj', 'name_partner', 'start_date')
    search_fields = ('cnpj', 'name_partner',)

    list_per_page = 20


@admin.register(Business)
class BusinessAdmin(admin.ModelAdmin):
    list_display  = (
        'cnpj'        ,
        'cnpj_order'  ,
        'cnpj_dv'     ,
        'trade_name'  ,
        'branch'      ,
        'opening_date',
        'closing_date',
        'cep'         ,
    )
    search_fields = ('cnpj'  , 'trade_name',)
    list_filter   = ('branch',)

    list_per_page = 20


@admin.register(PartnersFts)
class PartnersFtsAdmin(admin.ModelAdmin):
    list_display  = ('name_partner',)
    search_fields = ('name_partner',)

    list_per_page = 20


@admin.register(BusinessFts)
class BusinessFtsAdmin(admin.ModelAdmin):
    list_display  = ('trade_name',)
    search_fields = ('trade_name',)

    list_per_page = 20
