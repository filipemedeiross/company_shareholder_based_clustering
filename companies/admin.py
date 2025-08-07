from datetime import datetime
from django.contrib import admin

from .models import Companies  , \
                    Partners   , \
                    Business   , \
                    PartnersFts, \
                    BusinessFts


@admin.register(Companies)
class CompaniesAdmin(admin.ModelAdmin):
    list_display  = ('cnpj_str', 'corporate_name', 'capital')
    search_fields = ('cnpj'    , 'corporate_name',)

    list_per_page = 20


@admin.register(Partners)
class PartnersAdmin(admin.ModelAdmin):
    list_display  = ('cnpj_str', 'name_partner', 'start_date_obj')
    search_fields = ('cnpj'    , 'name_partner',)

    list_per_page = 20


@admin.register(Business)
class BusinessAdmin(admin.ModelAdmin):
    list_display  = (
        'cnpj_str'        ,
        'cnpj_order_str'  ,
        'cnpj_dv_str'     ,
        'trade_name'      ,
        'branch'          ,
        'opening_date_obj',
        'closing_date_obj',
        'cep_str'         ,
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
