from django.contrib import admin
from .models import (
    Organization, Building, Account, Meter,
    VirtualAllocation, Formula, Reading
)

admin.site.site_header = "Uelogic Admin"

@admin.register(Organization)
class OrganizationAdmin(admin.ModelAdmin):
    search_fields = ['name']
    list_display = ['name']

@admin.register(Building)
class BuildingAdmin(admin.ModelAdmin):
    list_display = ['name', 'org']
    list_filter = ['org']
    search_fields = ['name', 'org__name']

@admin.register(Account)
class AccountAdmin(admin.ModelAdmin):
    list_display = ['name', 'org']
    list_filter = ['org']
    search_fields = ['name', 'org__name']

@admin.register(Meter)
class MeterAdmin(admin.ModelAdmin):
    list_display = ['identifier', 'meter_type', 'building', 'account', 'org', 'is_active', 'unit']
    list_filter = ['org', 'meter_type']
    search_fields = ['identifier', 'external_id', 'building__name', 'account__name', 'org__name']
    autocomplete_fields = ['building', 'account', 'parent', 'org']

@admin.register(VirtualAllocation)
class VirtualAllocationAdmin(admin.ModelAdmin):
    list_display = ['parent', 'child', 'percent']
    list_filter = ['parent__org']
    search_fields = ['parent__identifier', 'child__identifier']

@admin.register(Formula)
class FormulaAdmin(admin.ModelAdmin):
    list_display = ['target_meter', 'start', 'end']
    search_fields = ['target_meter__identifier', 'expression']
    list_filter = ['target_meter__org']

@admin.register(Reading)
class ReadingAdmin(admin.ModelAdmin):
    list_display = ['meter', 'ts', 'value', 'source', 'classification', 'unit', 'kind']
    list_filter = ['meter__org', 'source', 'classification', 'kind', 'unit']
    search_fields = ['meter__identifier']
    date_hierarchy = 'ts'
