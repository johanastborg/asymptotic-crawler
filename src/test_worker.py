import unittest
from unittest.mock import AsyncMock, MagicMock
from bs4 import BeautifulSoup
import worker

class TestWorker(unittest.IsolatedAsyncioTestCase):
    async def test_fetch_and_parse_with_webmcp(self):
        html_content = """
        <html>
            <head>
                <script src="https://example.com/webmcp.js"></script>
            </head>
            <body>
                <a href="/link1">Link 1</a>
            </body>
        </html>
        """

        # Mock session and response
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.text.return_value = html_content
        mock_response.__aenter__.return_value = mock_response
        mock_response.__aexit__.return_value = None

        mock_session = MagicMock()
        mock_session.get.return_value = mock_response

        # Execute
        result = await worker.fetch_and_parse(mock_session, "http://example.com")

        links, has_webmcp = result
        self.assertIn("http://example.com/link1", links)
        self.assertTrue(has_webmcp)

    async def test_fetch_and_parse_without_webmcp(self):
        html_content = """
        <html>
            <head>
                <script src="https://example.com/jquery.js"></script>
            </head>
            <body>
                <a href="/link1">Link 1</a>
            </body>
        </html>
        """

        # Mock session and response
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.text.return_value = html_content
        mock_response.__aenter__.return_value = mock_response
        mock_response.__aexit__.return_value = None

        mock_session = MagicMock()
        mock_session.get.return_value = mock_response

        # Execute
        result = await worker.fetch_and_parse(mock_session, "http://example.com")

        links, has_webmcp = result
        self.assertIn("http://example.com/link1", links)
        self.assertFalse(has_webmcp)

if __name__ == '__main__':
    unittest.main()
