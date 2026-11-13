from django.contrib import admin

from .models import Dispatch, GobeatSettings, Job


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


@admin.register(Job)
class JobAdmin(admin.ModelAdmin):
    list_display = ('name', 'task', 'kind', 'source', 'enabled', 'paused',
                    'last_run_at', 'total_run_count')
    list_filter = ('source', 'kind', 'enabled', 'paused')
    search_fields = ('name', 'task')

    def get_readonly_fields(self, request, obj=None):
        fixed = ['source', 'last_run_at', 'total_run_count', 'date_changed']
        if obj is not None and obj.source == Job.SOURCE_CODE:
            # code-defined: gobeat_sync owns everything except `paused`
            return [f.name for f in Job._meta.fields if f.name != 'paused']
        return fixed
