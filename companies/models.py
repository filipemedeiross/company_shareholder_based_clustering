from django.db import models


class Companies(models.Model):
    cnpj           = models.TextField(primary_key=True)
    corporate_name = models.TextField(blank=True, null=True)
    capital        = models.IntegerField(blank=True, null=True)

    class Meta:
        managed  = False
        db_table = 'companies'
        verbose_name        = 'Company'
        verbose_name_plural = 'Companies'

    def __str__(self):
        return f"{self.cnpj}"

    rfb = True


class Partners(models.Model):
    rowid        = models.IntegerField(primary_key=True)
    cnpj         = models.ForeignKey(Companies, models.DO_NOTHING, db_column='cnpj')
    name_partner = models.TextField(blank=True, null=True)
    start_date   = models.DateField(blank=True, null=True)

    class Meta:
        managed  = False
        db_table = 'partners'
        verbose_name        = 'Partner'
        verbose_name_plural = 'Partners'

    def __str__(self):
        return f"{self.cnpj} - {self.name_partner}"

    rfb = True


class Business(models.Model):
    rowid        = models.IntegerField(primary_key=True)
    cnpj         = models.ForeignKey('Companies', models.DO_NOTHING, db_column='cnpj')
    cnpj_order   = models.TextField()
    cnpj_dv      = models.TextField()
    branch       = models.BooleanField(blank=True, null=True)
    trade_name   = models.TextField(blank=True, null=True)
    closing_date = models.DateField(blank=True, null=True)
    opening_date = models.DateField(blank=True, null=True)
    cep          = models.TextField(blank=True, null=True)

    class Meta:
        managed  = False
        db_table = 'business'
        unique_together = (('cnpj', 'cnpj_order', 'cnpj_dv'),)
        verbose_name        = 'Business'
        verbose_name_plural = 'Businesses'

    def __str__(self):
        return f"{self.cnpj}.{self.cnpj_order}-{self.cnpj_dv}"

    rfb = True


class PartnersFts(models.Model):
    rowid        = models.IntegerField(primary_key=True)
    name_partner = models.TextField(blank=True, null=True)

    class Meta:
        managed  = False
        db_table = 'partners_fts'
        verbose_name        = 'Partners FTS'
        verbose_name_plural = 'Partners FTSs'

    def __str__(self):
        return f"{self.name_partner}"

    rfb = True


class CompaniesFts(models.Model):
    rowid          = models.IntegerField(primary_key=True)
    corporate_name = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'companies_fts'
        verbose_name        = 'Companies FTS'
        verbose_name_plural = 'Companies FTSs'

    rfb = True


class BusinessFts(models.Model):
    rowid      = models.IntegerField(primary_key=True)
    trade_name = models.TextField(blank=True, null=True)

    class Meta:
        managed  = False
        db_table = 'business_fts'
        verbose_name        = 'Businesses FTS'
        verbose_name_plural = 'Businesses FTSs'

    def __str__(self):
        return f"{self.trade_name}"

    rfb = True
