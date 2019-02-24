from daterange_filter.filter import DateRangeFilter
from django import forms
from django.contrib import admin

from .models import HitRecord, Notice, Station, Switch

# Register your models here.
admin.site.register(Station)

@admin.register(HitRecord)
class HitRecordAdmin(admin.ModelAdmin):
    list_filter = (('date', DateRangeFilter),)

@admin.register(Notice)
class NoticeAdmin(admin.ModelAdmin):

    def formfield_for_dbfield(self, db_field, request, **kwargs):
        field = super().formfield_for_dbfield(db_field, request, **kwargs)
        if db_field.name == 'content':
            attrs = field.widget.attrs
            attrs.pop('class')
            attrs['cols'] = 80
            attrs['rows'] = 4
            field.widget = forms.Textarea(attrs=attrs)
        return field

@admin.register(Switch)
class SwitchAdmin(admin.ModelAdmin):

    def formfield_for_dbfield(self, db_field, request, **kwargs):
        field = super().formfield_for_dbfield(db_field, request, **kwargs)
        if db_field.name == 'description':
            attrs = field.widget.attrs
            attrs.pop('class')
            attrs['cols'] = 80
            attrs['rows'] = 4
            field.widget = forms.Textarea(attrs=attrs)
        return field

