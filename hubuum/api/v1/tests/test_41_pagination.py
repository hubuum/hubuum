"""Test the pagination in hubuum."""

from typing import Dict, Union
from urllib.parse import parse_qs, urlparse

from rest_framework.response import Response

from hubuum.models.iam import Namespace

from .base import HubuumAPITestCase
from .helpers.populators import BasePopulator


class HubuumPaginationTestCase(HubuumAPITestCase, BasePopulator):
    """Test case for pagination in the Hubuum project."""

    def setUp(self):
        """Set up the test environment."""
        super().setUp()
        self.namespace, _ = Namespace.objects.get_or_create(name="namespace1")
        self.hosts_url = "/dynamic/Host/"
        self.host_class = self.create_class_direct("Host", namespace=self.namespace)
        self._create_hosts(250)

    def tearDown(self) -> None:
        """Tear down the test environment."""
        self.namespace.delete()
        return super().tearDown()

    def _create_hosts(self, num_hosts: int) -> None:
        """Create the specified number of Host objects.

        :param num_hosts: The number of Host objects to create.
        :type num_hosts: int

        :return: None
        """
        for i in range(num_hosts):
            self.create_object_direct(
                self.host_class, namespace=self.namespace, name=f"hostname-{i}"
            )

    def test_pagination(self):
        """Test the pagination sizes and limits."""
        # Test default pagination size
        self.assert_get_elements(self.hosts_url, 100)

        # Test custom pagination size
        custom_page_size_url = f"{self.hosts_url}?page_size=50"
        self.assert_get_elements(custom_page_size_url, 50)

        # Test maximum pagination size limit
        max_page_size_url = f"{self.hosts_url}?page_size=200"
        self.assert_get_elements(max_page_size_url, 200)

        # Test maximum pagination size limit, but we're capped at 200.
        max_page_size_url = f"{self.hosts_url}?page_size=300"
        self.assert_get_elements(max_page_size_url, 200)

    def test_next_and_prev_links(self):
        """Test the next and prev links in the Link header."""
        # Test next and prev links when using the default page size
        response = self.assert_get(self.hosts_url)
        self.check_next_and_prev_links(response, expected_prev=None, expected_next=2)

        # Test next and prev links when using a custom page size
        custom_page_size_url = f"{self.hosts_url}?page_size=50"
        response = self.assert_get(custom_page_size_url)
        self.check_next_and_prev_links(response, expected_prev=None, expected_next=2)

        # Test next and prev links when using a custom page size and a specific page.
        custom_page_size_url = f"{self.hosts_url}?page_size=20&page=2"
        response = self.assert_get(custom_page_size_url)
        self.check_next_and_prev_links(response, expected_prev=1, expected_next=3)

        # Test next and prev links when going past the maximum page size limit
        max_page_size_url = f"{self.hosts_url}?page_size=300"
        response = self.assert_get(max_page_size_url)
        self.check_next_and_prev_links(response, expected_prev=None, expected_next=2)

        # Test next and prev links when going to the last hosts the maximum page size limit
        max_page_size_url = f"{self.hosts_url}?page_size=200&page=2"
        response = self.assert_get(max_page_size_url)
        self.check_next_and_prev_links(response, expected_prev=1, expected_next=None)

    def check_next_and_prev_links(
        self,
        response: Response,
        expected_prev: Union[int, None],
        expected_next: Union[int, None],
    ) -> None:
        """Check the next and prev links in the Link header of the given response.

        :param response: The response to check the Link header.
        :param expected_prev: The expected page number of the prev link or None if it should not
            exist.
        :param expected_next: The expected page number of the next link or None if it should not
            exist.
        """
        link_header = response["Link"]
        links = self.parse_link_header(link_header)

        if expected_prev:
            self.assertIn("prev", links)
            prev_link = links["prev"]
            prev_page = self.get_page_number_from_link(prev_link)
            self.assertEqual(prev_page, expected_prev)
        else:
            self.assertNotIn("prev", links)

        if expected_next:
            self.assertIn("next", links)
            next_link = links["next"]
            next_page = self.get_page_number_from_link(next_link)
            self.assertEqual(next_page, expected_next)
        else:
            self.assertNotIn("next", links)

    def parse_link_header(self, link_header: str) -> Dict[str, str]:
        """Parse the Link header into a dictionary.

        :param link_header: The Link header value.

        :return: A dictionary containing the parsed links with their relationship as keys.
        """
        links = {}
        for link in link_header.split(","):
            url, rel = link.strip().split(";")
            url = url.strip("<>")
            rel = rel.strip().split("=")[1].strip('"')
            links[rel] = url
        return links

    def get_page_number_from_link(self, link: str) -> int:
        """Get the page number from a paginated link.

        :param link: The paginated link.

        :return: The page number extracted from the link.
        """
        parsed_url = urlparse(link)
        query_params = parse_qs(parsed_url.query)
        return int(query_params.get("page", [1])[0])
