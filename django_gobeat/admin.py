from django.contrib import admin

from .models import Dispatch, GobeatSettings


@admin.register(GobeatSettings)
class GobeatSettingsAdmin(admin.ModelAdmin):
    list_display = ('key', 'version', 'updated_at')
    readonly_fields = ('key', 'version', 'data', 'updated_at')

    def has_add_permission(self, request):
        return False


@admin.register(Dispatch)
class DispatchAdmin(admin.ModelAdmin):
    list_display = ('name', 'task', 'scheduled_for', 'claimed_at',
                    'published_at', 'abandoned_at', 'attempts')
    list_filter = ('published_at',)
    search_fields = ('name', 'task', 'task_id')
    readonly_fields = [f.name for f in Dispatch._meta.fields]

    def has_add_permission(self, request):
        return False
