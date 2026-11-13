from django.db import migrations, models


class Migration(migrations.Migration):
    initial = True
    dependencies = []
    operations = [
        migrations.CreateModel(
            name='Run',
            fields=[
                ('id', models.BigAutoField(primary_key=True, serialize=False)),
                ('task', models.CharField(max_length=200)),
                ('task_id', models.CharField(db_index=True, max_length=64)),
                ('periodic_task_name', models.CharField(max_length=200, null=True)),
                ('args', models.TextField()),
                ('kwargs', models.TextField()),
                ('queue', models.CharField(max_length=200, null=True)),
                ('exchange', models.CharField(max_length=200, null=True)),
                ('priority', models.IntegerField(null=True)),
                ('expires', models.CharField(max_length=64, null=True)),
                ('headers', models.TextField(default='')),
                ('executed_at', models.DateTimeField(auto_now_add=True)),
            ],
        ),
    ]
