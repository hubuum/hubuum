"""Filters for hubuum permissions."""
from django.contrib.auth.models import Group
from django.db.models import Q
from django_filters import rest_framework as filters
from rest_framework.exceptions import ValidationError

from hubuum.models.auth import User
from hubuum.models.base import (
    Extension,
    ExtensionData,
    Host,
    HostType,
    Jack,
    Namespace,
    Permission,
    Person,
    PurchaseDocuments,
    PurchaseOrder,
    Room,
    Vendor,
    model_is_open,
)

_key_lookups = ["exact"]  # in?
_many_to_many_lookups = _key_lookups
_many_to_one_lookups = _key_lookups
_textual_lookups = [
    "contains",
    "icontains",
    "endswith",
    "iendswith",
    "startswith",
    "istartswith",
    "exact",
    "iexact",
]
_numeric_lookups = ["exact", "gt", "gte", "lt", "lte", "range"]
_date_lookups = [
    "day",
    "month",
    "quarter",
    "year",
    "exact",
    "week_day",
    "iso_week_day",
    "iso_year",
]
_date_lookups.extend(_numeric_lookups)

_hubuum_fields = {
    "id": _key_lookups,
    "created_at": _date_lookups,
    "updated_at": _date_lookups,
}
_namespace_fields = {"namespace": _key_lookups}
_namespace_fields.update(_hubuum_fields)


class JSONFieldLookupFilter(filters.CharFilter):
    """Class to allow filtering on JSON fields.

    Args:
        field_name (str): The field name to filter on. Must be a JSON field.
    """

    def filter(self, qs, value):
        """Filter the queryset based on a JSON key, value, and optional lookup type.

        Args:
            qs (QuerySet): The queryset to filter.
            value (str): The input value containing the key, value, and optional lookup type.

        Returns:
            QuerySet: The filtered queryset.

        Raises:
            ValidationError: If an invalid lookup type for the value is provided.
        """
        if not value:
            return qs

        try:
            key, val = value.split("=")
        except ValueError as ex:
            raise ValidationError(
                "Filtering requires both a key and a value, separated by '='"
            ) from ex

        try:
            val = float(val)
        except ValueError:
            pass

        if isinstance(val, (float, int)):
            val_type = "numeric"
            allowed_lookups = _numeric_lookups
        else:  # Assume string
            val_type = "text"
            allowed_lookups = _textual_lookups

        parts = key.split("__")
        if len(parts) > 1 and parts[-1] in _numeric_lookups + _textual_lookups:
            lookup_type = parts[-1]
        else:
            lookup_type = "exact"
            key = f"{key}__exact"

        if lookup_type not in allowed_lookups:
            valid_lookups = ", ".join(allowed_lookups)
            allowed_string = f"Allowed types for {val_type} are {valid_lookups}."
            raise ValidationError(
                f"Invalid lookup type '{lookup_type}'. {allowed_string}"
            )

        json_lookup = Q(**{f"{self.field_name}__{key}": val})
        return qs.filter(json_lookup)


class NamespacePermissionFilter(filters.FilterSet):
    """Return viewable objects for a user.

    This filter returns (request.)user-visible objects of a model in question.
    """

    def filter_queryset(self, queryset):
        """Perform the filtering."""
        queryset = super().filter_queryset(queryset)
        user = self.request.user
        # model = queryset.model._meta.model_name
        #        permission = self.perm_format % {
        #            "app_label": queryset.model._meta.app_label,
        #            "model_name": queryset.model._meta.model_name,
        #        }

        #    Find all namespaces we can perform the given operation in.

        #        print("List of {}".format(model))
        model_name = queryset.model._meta.model_name  # pylint: disable=protected-access
        if user.is_admin() or model_is_open(model_name):
            return queryset

        res = Permission.objects.filter(
            has_read=True, group__in=user.groups.all()
        ).values_list("namespace", flat=True)
        # print(res)
        # print(queryset)
        if model_name == "namespace":
            filtered = queryset.filter(pk__in=res)
        else:
            filtered = queryset.filter(namespace__in=res)
        return filtered


class NamespaceFilterSet(NamespacePermissionFilter):
    """FilterSet class for Namespace."""

    class Meta:
        """Metadata for the class."""

        model = Namespace
        fields = {
            "name": _textual_lookups,
            "description": _textual_lookups,
        }
        fields.update(_hubuum_fields)


class UserFilterSet(filters.FilterSet):
    """FilterSet class for User."""

    class Meta:
        """Metadata for the class."""

        model = User
        fields = {
            "id": _numeric_lookups,
            "username": _textual_lookups,
            "email": _textual_lookups,
            "is_active": ["exact"],
            "is_staff": ["exact"],
            "is_superuser": ["exact"],
            "last_login": _date_lookups,
            "groups": _many_to_many_lookups,
        }


class GroupFilterSet(filters.FilterSet):
    """FilterSet class for Group."""

    class Meta:
        """Metadata for the class."""

        model = Group
        fields = {
            "id": _numeric_lookups,
            "name": _textual_lookups,
            "user": _many_to_many_lookups,
            "permissions": _many_to_many_lookups,
        }


class PermissionFilterSet(filters.FilterSet):
    """FilterSet class for Permission."""

    class Meta:
        """Metadata for the class."""

        model = Permission
        fields = {
            "namespace": _key_lookups,
            "group": _key_lookups,
            "has_create": ["exact"],
            "has_read": ["exact"],
            "has_update": ["exact"],
            "has_delete": ["exact"],
            "has_namespace": ["exact"],
        }
        fields.update(_hubuum_fields)


class ExtensionFilterSet(NamespacePermissionFilter):
    """FilterSet class for Extension."""

    class Meta:
        """Metadata for the class."""

        model = Extension
        fields = {
            "name": _textual_lookups,
            "model": _textual_lookups,
            "url": _textual_lookups,
            "require_interpolation": ["exact"],
            "header": _textual_lookups,
            "cache_time": _numeric_lookups,
        }
        fields.update(_namespace_fields)


class ExtensionDataFilterSet(NamespacePermissionFilter):
    """FilterSet for the ExtensionData model with a custom json_data_lookup filter."""

    json_data_lookup = JSONFieldLookupFilter(field_name="json_data")

    class Meta:
        """Meta class for ExtensionDataFilterSet."""

        model = ExtensionData
        fields = ["extension", "content_type", "object_id"]


class HostFilterSet(NamespacePermissionFilter):
    """FilterSet class for Host."""

    class Meta:
        """Metadata for the class."""

        model = Host
        fields = {
            "name": _textual_lookups,
            "fqdn": _textual_lookups,
            "serial": _textual_lookups,
            "registration_date": _date_lookups,
            "room": _key_lookups,
            "jack": _key_lookups,
            "purchase_order": _key_lookups,
            "person": _key_lookups,
        }


class HostTypeFilterSet(NamespacePermissionFilter):
    """FilterSet class for HostType."""

    class Meta:
        """Metadata for the class."""

        model = HostType
        fields = {"name": _textual_lookups, "description": _textual_lookups}
        fields.update(_namespace_fields)


class JackFilterSet(NamespacePermissionFilter):
    """FilterSet class for Jack."""

    class Meta:
        """Metadata for the class."""

        model = Jack
        fields = {
            "name": _textual_lookups,
            "building": _textual_lookups,
            "room": _key_lookups,
        }
        fields.update(_namespace_fields)


class PersonFilterSet(NamespacePermissionFilter):
    """FilterSet class for Person."""

    class Meta:
        """Metadata for the class."""

        model = Person
        fields = {
            "username": _textual_lookups,
            "section": _textual_lookups,
            "department": _textual_lookups,
            "email": _textual_lookups,
            "office_phone": _textual_lookups,
            "mobile_phone": _textual_lookups,
            "room": _key_lookups,
        }
        fields.update(_namespace_fields)


class PurchaseDocumentsFilterSet(NamespacePermissionFilter):
    """FilterSet class for PurchaseDocuments."""

    class Meta:
        """Metadata for the class."""

        # It would be neat to have the binary field "document"
        # be matchable to a hash...
        model = PurchaseDocuments
        fields = {
            "document_id": _numeric_lookups,
            "purchase_order": _key_lookups,
        }
        fields.update(_namespace_fields)


class PurchaseOrderFilterSet(NamespacePermissionFilter):
    """FilterSet class for PurchaseOrder."""

    class Meta:
        """Metadata for the class."""

        model = PurchaseOrder
        fields = {
            "vendor": _key_lookups,
            "order_date": _date_lookups,
            "po_number": _textual_lookups,
        }
        fields.update(_namespace_fields)


class RoomFilterSet(NamespacePermissionFilter):
    """FilterSet class for Room."""

    class Meta:
        """Metadata for the class."""

        model = Room
        fields = {
            "room_id": _textual_lookups,
            "building": _textual_lookups,
            "floor": _textual_lookups,
        }
        fields.update(_namespace_fields)


class VendorFilterSet(NamespacePermissionFilter):
    """FilterSet class for Vendor."""

    class Meta:
        """Metadata for the class."""

        model = Vendor
        fields = {
            "vendor_name": _textual_lookups,
            "vendor_url": _textual_lookups,
            "vendor_credentials": _textual_lookups,
            "contact_name": _textual_lookups,
            "contact_email": _textual_lookups,
            "contact_phone": _textual_lookups,
        }
        fields.update(_namespace_fields)
