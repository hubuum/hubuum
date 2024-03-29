# Generated by Django 3.2.22 on 2023-11-22 00:19

import django.contrib.auth.models
import django.contrib.auth.validators
from django.db import migrations, models
import django.db.models.deletion
import django.utils.timezone


class Migration(migrations.Migration):

    initial = True

    dependencies = [
        ('auth', '0012_alter_user_first_name_max_length'),
    ]

    operations = [
        migrations.CreateModel(
            name='HubuumClass',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
                ('name', models.CharField(max_length=200, unique=True)),
                ('json_schema', models.JSONField(blank=True, null=True)),
                ('validate_schema', models.BooleanField(default=False)),
            ],
            options={
                'abstract': False,
            },
        ),
        migrations.CreateModel(
            name='Namespace',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
                ('name', models.CharField(max_length=255, unique=True)),
                ('description', models.TextField(blank=True)),
            ],
            options={
                'ordering': ['id'],
            },
        ),
        migrations.CreateModel(
            name='HubuumObject',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
                ('name', models.CharField(max_length=200)),
                ('json_data', models.JSONField()),
                ('hubuum_class', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='hubuum.hubuumclass')),
                ('namespace', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='hubuum.namespace')),
            ],
            options={
                'unique_together': {('name', 'hubuum_class')},
            },
        ),
        migrations.AddField(
            model_name='hubuumclass',
            name='namespace',
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='hubuum.namespace'),
        ),
        migrations.CreateModel(
            name='ClassLink',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
                ('max_links', models.IntegerField()),
                ('namespace', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='hubuum.namespace')),
                ('source_class', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='source_links', to='hubuum.hubuumclass')),
                ('target_class', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='target_links', to='hubuum.hubuumclass')),
            ],
            options={
                'unique_together': {('source_class', 'target_class')},
            },
        ),
        migrations.CreateModel(
            name='AttachmentManager',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
                ('enabled', models.BooleanField(default=False)),
                ('per_object_count_limit', models.PositiveIntegerField(default=0)),
                ('per_object_individual_size_limit', models.PositiveIntegerField(default=0)),
                ('per_object_total_size_limit', models.PositiveIntegerField(default=0)),
                ('hubuum_class', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='hubuum.hubuumclass')),
            ],
            options={
                'ordering': ['id'],
            },
        ),
        migrations.CreateModel(
            name='Attachment',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
                ('attachment', models.FileField(unique=True, upload_to='')),
                ('sha256', models.CharField(editable=False, max_length=64, unique=True)),
                ('size', models.PositiveIntegerField(editable=False)),
                ('original_filename', models.CharField(editable=False, max_length=255)),
                ('hubuum_class', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='hubuum.hubuumclass')),
                ('hubuum_object', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='hubuum.hubuumobject')),
                ('namespace', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='hubuum.namespace')),
            ],
            options={
                'ordering': ['id'],
            },
        ),
        migrations.CreateModel(
            name='User',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('password', models.CharField(max_length=128, verbose_name='password')),
                ('last_login', models.DateTimeField(blank=True, null=True, verbose_name='last login')),
                ('is_superuser', models.BooleanField(default=False, help_text='Designates that this user has all permissions without explicitly assigning them.', verbose_name='superuser status')),
                ('username', models.CharField(error_messages={'unique': 'A user with that username already exists.'}, help_text='Required. 150 characters or fewer. Letters, digits and @/./+/-/_ only.', max_length=150, unique=True, validators=[django.contrib.auth.validators.UnicodeUsernameValidator()], verbose_name='username')),
                ('first_name', models.CharField(blank=True, max_length=150, verbose_name='first name')),
                ('last_name', models.CharField(blank=True, max_length=150, verbose_name='last name')),
                ('email', models.EmailField(blank=True, max_length=254, verbose_name='email address')),
                ('is_staff', models.BooleanField(default=False, help_text='Designates whether the user can log into this admin site.', verbose_name='staff status')),
                ('is_active', models.BooleanField(default=True, help_text='Designates whether this user should be treated as active. Unselect this instead of deleting accounts.', verbose_name='active')),
                ('date_joined', models.DateTimeField(default=django.utils.timezone.now, verbose_name='date joined')),
                ('groups', models.ManyToManyField(blank=True, help_text='The groups this user belongs to. A user will get all permissions granted to each of their groups.', related_name='user_set', related_query_name='user', to='auth.Group', verbose_name='groups')),
                ('user_permissions', models.ManyToManyField(blank=True, help_text='Specific permissions for this user.', related_name='user_set', related_query_name='user', to='auth.Permission', verbose_name='user permissions')),
            ],
            options={
                'ordering': ['id'],
            },
            managers=[
                ('objects', django.contrib.auth.models.UserManager()),
            ],
        ),
        migrations.CreateModel(
            name='Permission',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
                ('has_create', models.BooleanField(default=False)),
                ('has_read', models.BooleanField(default=False)),
                ('has_update', models.BooleanField(default=False)),
                ('has_delete', models.BooleanField(default=False)),
                ('has_namespace', models.BooleanField(default=False)),
                ('group', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='p_group', to='auth.group')),
                ('namespace', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='p_namespace', to='hubuum.namespace')),
            ],
            options={
                'ordering': ['id'],
                'unique_together': {('namespace', 'group')},
            },
        ),
        migrations.CreateModel(
            name='ObjectLink',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
                ('link_type', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='links', to='hubuum.classlink')),
                ('namespace', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='hubuum.namespace')),
                ('source', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='outbound_links', to='hubuum.hubuumobject')),
                ('target', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='inbound_links', to='hubuum.hubuumobject')),
            ],
            options={
                'unique_together': {('source', 'target')},
            },
        ),
    ]
