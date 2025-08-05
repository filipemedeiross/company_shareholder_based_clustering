from django.db import models


class Companies(models.Model):
    cnpj = models.IntegerField(primary_key=True)
    corporate_name = models.TextField(blank=True, null=True)
    capital = models.IntegerField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'companies'


class Partners(models.Model):
    cnpj = models.ForeignKey(Companies, models.DO_NOTHING, db_column='cnpj')
    name_partner = models.TextField(blank=True, null=True)
    start_date = models.IntegerField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'partners'


class Business(models.Model):
    cnpj = models.ForeignKey('Companies', models.DO_NOTHING, db_column='cnpj')
    cnpj_order = models.IntegerField()
    cnpj_dv = models.IntegerField()
    branch = models.BooleanField(blank=True, null=True)
    trade_name = models.TextField(blank=True, null=True)
    closing_date = models.IntegerField(blank=True, null=True)
    opening_date = models.IntegerField(blank=True, null=True)
    cep = models.IntegerField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'business'
        unique_together = (('cnpj', 'cnpj_order', 'cnpj_dv'))


class PartnersFts(models.Model):
    name_partner = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'partners_fts'


class BusinessFts(models.Model):
    trade_name = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'business_fts'
