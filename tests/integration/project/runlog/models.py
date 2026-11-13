from django.db import models


class Run(models.Model):
    """One task execution, written by the worker."""
    task = models.CharField(max_length=200)
    task_id = models.CharField(max_length=64, db_index=True)
    periodic_task_name = models.CharField(max_length=200, null=True)
    args = models.TextField()
    kwargs = models.TextField()
    queue = models.CharField(max_length=200, null=True)
    exchange = models.CharField(max_length=200, null=True)
    priority = models.IntegerField(null=True)
    expires = models.CharField(max_length=64, null=True)
    headers = models.TextField(default='')
    executed_at = models.DateTimeField(auto_now_add=True)
