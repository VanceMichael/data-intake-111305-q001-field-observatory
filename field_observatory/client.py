from dataclasses import dataclass
from urllib.error import HTTPError
from urllib.parse import urlencode, urljoin, urlsplit
from urllib.request import Request, urlopen


@dataclass(frozen=True)
class Response:
    url: str
    status: int
    headers: dict
    body: bytes


def _check_path(path):
    if not path.startswith("/") or path.startswith("//") or urlsplit(path).netloc:
        raise ValueError("source path must be origin-relative")


class Client:
    def __init__(self, base_url, timeout=3):
        parsed = urlsplit(base_url)
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            raise ValueError("HTTP base URL required")
        self.base_url = base_url.rstrip("/") + "/"
        self.timeout = timeout

    def _send(self, request):
        try:
            response = urlopen(request, timeout=self.timeout)
        except HTTPError as error:
            response = error
        with response:
            return Response(
                response.geturl(), response.status,
                {name.lower(): value for name, value in response.headers.items()},
                response.read(),
            )

    def fetch(self, path, headers=None):
        _check_path(path)
        return self._send(Request(urljoin(self.base_url, path), headers=dict(headers or {})))

    def post_form(self, path, fields):
        _check_path(path)
        data = urlencode({name: str(value) for name, value in fields.items()}).encode()
        return self._send(Request(urljoin(self.base_url, path), data=data))


def collect(client, archive, source, path="/feed"):
    response = client.fetch(path)
    capture_id = archive.append(source, response.url, response.status, response.headers, response.body)
    return capture_id, response
