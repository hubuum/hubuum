"""Provide a base class for testing model behaviour."""
from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.test import TestCase

from hubuum.exceptions import MissingParam
from hubuum.models.base import (  # Permissions,
    Extension,
    Namespace,
    Person,
    PurchaseDocuments,
    PurchaseOrder,
    Room,
    Vendor,
)


class HubuumModelTestCase(TestCase):
    """This class defines the test suite for a generic model."""

    def setUp(self):
        """Set up defaults for the test object."""
        self.username = "test"
        self.password = "test"  # nosec
        self.groupname = "test"
        self.namespacename = "test"

        self.user, _ = get_user_model().objects.get_or_create(
            username=self.username, password=self.password
        )
        self.assertIsNotNone(self.user)

        self.group, _ = Group.objects.get_or_create(name=self.groupname)
        self.assertIsNotNone(self.group)

        self.namespace, _ = Namespace.objects.get_or_create(
            name=self.namespacename, description="Test namespace."
        )
        self.assertIsNotNone(self.namespace)

        self.attributes = {}
        self.obj = None

    def attribute(self, key):
        """Fetch attributes from the attribute dictionary."""
        return self.attributes[key]

    def _test_can_create_object(self, model=None, **kwargs):
        """Create a generic object of any model."""
        if "namespace" not in kwargs and model is not Namespace:
            kwargs["namespace"] = self.namespace

        return self._create_object(model=model, **kwargs)

    def _test_has_identical_values(self, obj=None, dictionary=None):
        """Compare the dictionary with the same attributes from the self."""
        if not (obj and dictionary):
            raise MissingParam

        for key in dictionary.keys():
            self.assertEqual(getattr(obj, key), dictionary[key])

    def _create_object(self, model=None, **kwargs):
        """Create an object with overridable default group ownership."""
        if not model:
            raise MissingParam

        # Ick.
        if model == Extension:
            kwargs["model"] = "host"

        obj, _ = model.objects.get_or_create(**kwargs)
        self.assertIsNotNone(obj)
        self.assertIsInstance(obj, model)
        return obj

    def _test_str(self):
        """Test that stringifying objects works as expected."""
        obj = self.obj
        if isinstance(obj, Person):
            self.assertEqual(str(obj), self.attribute("username"))
        elif isinstance(obj, PurchaseDocuments):
            self.assertEqual(str(obj), self.attribute("document_id"))
        elif isinstance(obj, PurchaseOrder):
            self.assertEqual(str(obj), self.attribute("po_number"))
        elif isinstance(obj, Room):
            floor = self.attribute("floor").rjust(2, "0")
            building = self.attribute("building")
            room_id = self.attribute("room_id")
            string = building + "-" + floor + "-" + room_id
            self.assertEqual(str(obj), string)
        elif isinstance(obj, Vendor):
            self.assertEqual(str(obj), self.attribute("vendor_name"))
        else:
            self.assertEqual(str(obj), self.attribute("name"))
