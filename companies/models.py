from django.db import models


class Companies(models.Model):
    cnpj           = models.IntegerField(primary_key=True)
    corporate_name = models.TextField(blank=True, null=True)
    capital        = models.IntegerField(blank=True, null=True)

    class Meta:
        managed  = False
        db_table = 'companies'
        verbose_name        = 'Companie'
        verbose_name_plural = 'Companies'

    def __str__(self):
        return f"{self.cnpj}"


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
        managed = False
        db_table = 'business'
        unique_together = (('cnpj', 'cnpj_order', 'cnpj_dv'),)
        verbose_name        = 'Business'
        verbose_name_plural = 'Businesses'


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
