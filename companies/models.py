from datetime  import datetime

from django.db import models
from django.contrib import admin


class Companies(models.Model):
    cnpj           = models.IntegerField(primary_key=True)
    corporate_name = models.TextField(blank=True, null=True)
    capital        = models.IntegerField(blank=True, null=True)

    class Meta:
        managed  = False
        db_table = 'companies'
        verbose_name        = 'Company'
        verbose_name_plural = 'Companies'

    def __str__(self):
        return f"{self.cnpj}"

    @admin.display(description='CNPJ')
    def cnpj_str(self):
        return f"{self.cnpj:08d}"


class Partners(models.Model):
    rowid        = models.IntegerField(primary_key=True)
    cnpj         = models.ForeignKey(Companies, models.DO_NOTHING, db_column='cnpj')
    name_partner = models.TextField(blank=True, null=True)
    start_date   = models.IntegerField(blank=True, null=True)

    class Meta:
        managed  = False
        db_table = 'partners'
        verbose_name        = 'Partner'
        verbose_name_plural = 'Partners'

    @admin.display(description='CNPJ')
    def cnpj_str(self):
        return f"{self.cnpj.cnpj:08d}"

    @admin.display(description='Start Date')
    def start_date_obj(self):
        if self.start_date:
            return datetime.strptime(str(self.start_date), "%Y%m%d").date()

        return None


class Business(models.Model):
    rowid        = models.IntegerField(primary_key=True)
    cnpj         = models.ForeignKey('Companies', models.DO_NOTHING, db_column='cnpj')
    cnpj_order   = models.IntegerField()
    cnpj_dv      = models.IntegerField()
    branch       = models.BooleanField(blank=True, null=True)
    trade_name   = models.TextField(blank=True, null=True)
    closing_date = models.IntegerField(blank=True, null=True)
    opening_date = models.IntegerField(blank=True, null=True)
    cep          = models.IntegerField(blank=True, null=True)

    class Meta:
        managed  = False
        db_table = 'business'
        unique_together = (('cnpj', 'cnpj_order', 'cnpj_dv'),)
        verbose_name        = 'Business'
        verbose_name_plural = 'Businesses'

    @admin.display(description='CNPJ')
    def cnpj_str(self):
        return f"{self.cnpj.cnpj:08d}"

    @admin.display(description='CNPJ Order')
    def cnpj_order_str(self):
        return f"{self.cnpj_order:04d}"

    @admin.display(description='CNPJ Dv')
    def cnpj_dv_str(self):
        return f"{self.cnpj_dv:02d}"

    @admin.display(description='Opening Date')
    def opening_date_obj(self):
        if self.opening_date:
            return datetime.strptime(str(self.opening_date), "%Y%m%d").date()

        return None

    @admin.display(description='Closing Date')
    def closing_date_obj(self):
        if self.closing_date:
            return datetime.strptime(str(self.closing_date), "%Y%m%d").date()

        return None

    @admin.display(description='CEP')
    def cep_str(self):
        if self.cep:
            return f"{self.cep:08d}"

        return None


class PartnersFts(models.Model):
    rowid        = models.IntegerField(primary_key=True)
    name_partner = models.TextField(blank=True, null=True)

    class Meta:
        managed  = False
        db_table = 'partners_fts'
        verbose_name        = 'Partners FTS'
        verbose_name_plural = 'Partners FTSs'


class BusinessFts(models.Model):
    rowid      = models.IntegerField(primary_key=True)
    trade_name = models.TextField(blank=True, null=True)

    class Meta:
        managed  = False
        db_table = 'business_fts'
        verbose_name        = 'Businesses FTS'
        verbose_name_plural = 'Businesses FTSs'
