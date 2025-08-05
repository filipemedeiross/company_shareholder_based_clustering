from datetime import datetime
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
    list_display  = ('cnpj', 'name_partner', 'formatted_start_date')
    search_fields = ('cnpj', 'name_partner',)

    list_per_page = 20

    def formatted_start_date(self, obj):
        if obj.start_date:
            try:
                return datetime.strptime(str(obj.start_date), '%Y%m%d').date()
            except ValueError:
                return obj.start_date

        return "-"

    formatted_start_date.admin_order_field = 'start_date'
    formatted_start_date.short_description = 'Start Date'


@admin.register(Business)
class BusinessAdmin(admin.ModelAdmin):
    list_display  = (
        'cnpj'                  ,
        'cnpj_order'            ,
        'cnpj_dv'               ,
        'trade_name'            ,
        'branch'                ,
        'formatted_opening_date',
        'formatted_closing_date',
        'cep'                   ,
    )
    search_fields = ('cnpj', 'trade_name',)
    list_filter   = ('branch',)

    list_per_page = 20

    def formatted_opening_date(self, obj):
        if obj.opening_date:
            try:
                return datetime.strptime(str(obj.opening_date), '%Y%m%d').date()
            except ValueError:
                return obj.opening_date

        return "-"

    def formatted_closing_date(self, obj):
        if obj.closing_date:
            try:
                return datetime.strptime(str(obj.closing_date), '%Y%m%d').date()
            except ValueError:
                return obj.closing_date

        return "-"

    formatted_opening_date.admin_order_field = 'opening_date'
    formatted_opening_date.short_description = 'Opening Date'
    formatted_closing_date.admin_order_field = 'closing_date'
    formatted_closing_date.short_description = 'Closing Date'


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
