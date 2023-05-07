"""Core models for hubuum."""

import hashlib
import re

from django.contrib.contenttypes.fields import GenericForeignKey, GenericRelation
from django.contrib.contenttypes.models import ContentType
from django.db import models

from hubuum.tools import get_model
from hubuum.validators import (
    url_interpolation_regexp,
    validate_model_can_have_attachments,
    validate_model_can_have_extensions,
    validate_url,
)


def model_is_open(model):
    """Check if the model is an open model."""
    return model in models_that_are_open()


def models_that_are_open():
    """Return a list of models open to all authenticated users."""
    return ("user", "group")


def model_supports_extensions(model):
    """Check if a model supports extensions."""
    if isinstance(model, str):
        model = get_model(model)

    return issubclass(model, ExtensionsModel)


def model_supports_attachments(model):
    """Check if a model supports attachments."""
    if isinstance(model, str):
        model = get_model(model)

    return issubclass(model, AttachmentModel)


class HubuumModel(models.Model):
    """Base model for Hubuum Objects."""

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    @classmethod
    def supports_extensions(cls):
        """Check if a class supports extensions."""
        return issubclass(cls, ExtensionsModel)

    @classmethod
    def supports_attachments(cls):
        """Check if a class supports attachments."""
        return issubclass(cls, AttachmentModel)

    readonly_fields = (
        "created_at",
        "updated_at",
    )

    class Meta:
        """Meta data for the class."""

        abstract = True


class NamespacedHubuumModel(HubuumModel):
    """Base model for a namespaced Hubuum Objects."""

    # When we delete a namespace, do we want *all* the objects to disappear?
    # That'd be harsh. But, well... What is the realistic option?
    namespace = models.ForeignKey(
        "Namespace",
        on_delete=models.CASCADE,
        blank=False,
        null=False,
    )

    class Meta:
        """Meta data for the class."""

        abstract = True


class AttachmentManager(HubuumModel):
    """A model for attachments to objects."""

    model = models.CharField(
        max_length=255,
        null=False,
        validators=[validate_model_can_have_attachments],
        unique=True,
    )
    enabled = models.BooleanField(default=False, null=False)
    per_object_count_limit = models.PositiveIntegerField(default=0, null=False)
    per_object_individual_size_limit = models.PositiveIntegerField(
        default=0, null=False
    )
    per_object_total_size_limit = models.PositiveIntegerField(default=0, null=False)

    class Meta:
        """Meta for the model."""

        ordering = ["id"]

    def __str__(self):
        """Stringify the object, used to represent the object towards users."""
        return str(self.id)


class Attachment(NamespacedHubuumModel):
    """A model for the attachments data for objects."""

    attachment = models.FileField(unique=True)
    content_type = models.ForeignKey(ContentType, on_delete=models.CASCADE)
    object_id = models.PositiveIntegerField()
    content_object = GenericForeignKey("content_type", "object_id")
    sha256 = models.CharField(max_length=64, unique=True, editable=False)
    size = models.PositiveIntegerField(editable=False)
    original_filename = models.CharField(max_length=255, editable=False)

    def generate_sha256_filename(self, sha256_hash):
        """Generate a custom filename for the uploaded file using its sha256 hash."""
        return f"attachments/file/{sha256_hash}"

    def save(self, *args, **kwargs):
        """Override the save method to compute the sha256 hash and size of the uploaded file."""
        file_contents = self.attachment.read()
        self.sha256 = hashlib.sha256(file_contents).hexdigest()
        self.size = self.attachment.size
        self.original_filename = self.attachment.name
        self.attachment.name = self.generate_sha256_filename(self.sha256)
        super().save(*args, **kwargs)

    class Meta:
        """Meta for the model."""

        ordering = ["id"]

    def __str__(self):
        """Stringify the object, used to represent the object towards users."""
        return str(self.id)


class AttachmentModel(models.Model):
    """A model that supports attachments."""

    attachment_data_objects = GenericRelation(
        Attachment, related_query_name="att_objects"
    )

    def attachments_are_enabled(self):
        """Check if the model is ready to receive attachments."""
        return AttachmentManager.objects.filter(
            model=self.__class__.__name__.lower(), enabled=True
        ).exists()

    def attachments(self):
        """List all attachments registered to the object."""
        return self.attachment_data_objects.all()

    def attachment_count(self):
        """Return the number of attachments registered to the object."""
        return self.attachment_data_objects.count()

    def attachment_size(self):
        """Return the total size of all attachments registered to the object."""
        return sum(attachment.size for attachment in self.attachments())

    def attachment_individual_size_limit(self):
        """Return the max size of an attachment for the object."""
        return AttachmentManager.objects.get(
            model=self.__class__.__name__.lower(), enabled=True
        ).per_object_individual_size_limit

    def attachment_total_size_limit(self):
        """Return the size limit of attachments for the object."""
        return AttachmentManager.objects.get(
            model=self.__class__.__name__.lower(), enabled=True
        ).per_object_total_size_limit

    def attachment_count_limit(self):
        """Return the count limit of attachments for the object."""
        return AttachmentManager.objects.get(
            model=self.__class__.__name__.lower(), enabled=True
        ).per_object_count_limit

    class Meta:
        """Meta for the model."""

        abstract = True


class Extension(NamespacedHubuumModel):
    """An extension to a specific model.

    For now, it is implied that the extension uses REST.
    """

    name = models.CharField(max_length=255, null=False, unique=True)
    model = models.CharField(
        max_length=255, null=False, validators=[validate_model_can_have_extensions]
    )
    url = models.CharField(max_length=255, null=False, validators=[validate_url])
    require_interpolation = models.BooleanField(default=True, null=False)
    header = models.CharField(max_length=512)
    cache_time = models.PositiveSmallIntegerField(default=60)

    class Meta:
        """Meta for the model."""

        ordering = ["id"]

    def __str__(self):
        """Stringify the object, used to represent the object towards users."""
        return self.name


class ExtensionData(NamespacedHubuumModel):
    """A model for the extensions data for objects.

    Note that the object_id refers to an object of the appropriate model.
    https://docs.djangoproject.com/en/4.1/ref/contrib/contenttypes/#generic-relations
    """

    extension = models.ForeignKey("Extension", on_delete=models.CASCADE, null=False)

    content_type = models.ForeignKey(ContentType, on_delete=models.CASCADE)
    object_id = models.PositiveIntegerField()
    content_object = GenericForeignKey("content_type", "object_id")

    json_data = models.JSONField(null=True)

    class Meta:
        """Meta for the model."""

        unique_together = ("extension", "content_type", "object_id")
        ordering = ["id"]

    def __str__(self):
        """Stringify the object, used to represent the object towards users."""
        return str(self.id)


class ExtensionsModel(models.Model):
    """A model that supports extensions."""

    extension_data_objects = GenericRelation(
        ExtensionData, related_query_name="ext_objects"
    )

    def extensions(self):
        """List all extensions registered for the object."""
        model = self.__class__.__name__.lower()
        return Extension.objects.filter(model=model).order_by("name")

    def extension_data(self):
        """Return the data for each extension the object has."""
        extension_data = {}

        for extension in self.extensions():
            extension_data[extension.name] = None

        for extension_data_obj in self.extension_data_objects.all():
            extension_data[
                extension_data_obj.extension.name
            ] = extension_data_obj.json_data

        return extension_data

    def extension_urls(self):
        """Return the URLs for each extension the object has."""
        url_map = {}
        for extension in self.extensions():
            url_map[extension.name] = self.interpolate(extension.url)

        return url_map

    def interpolate(self, string):
        """Interpolate fields within {} to the values of those fields."""

        def _get_value_from_match(matchobj):
            """Interpolate the match object."""
            return getattr(self, matchobj.group(1))

        return re.sub(url_interpolation_regexp, _get_value_from_match, string)

    class Meta:
        """Meta data for the class."""

        abstract = True