"""Tier-1 (``_fetch_plain``) httpx client pooling — offline tests.

Covers: client reuse per (loop, proxy), isolation across proxies and loops,
no cookie leakage between fetches, per-hop ``check_url`` on redirects, the
LRU bound, and ``aclose_plain_clients``.
"""

import asyncio

import httpx
import pytest

from apps.api.services.leadgen import http as H
from apps.api.services.leadgen.http import StealthClient


@pytest.fixture
def mock_httpx(monkeypatch):
    """Route every new AsyncClient through a MockTransport; record clients/requests."""
    state = {"clients": [], "proxies": [], "requests": [], "handler": None}

    def default_handler(request):
        return httpx.Response(200, text="<html>ok</html>")

    real_init = httpx.AsyncClient.__init__

    def init(self, *args, **kwargs):
        state["proxies"].append(kwargs.pop("proxy", None))

        def handler(request):
            state["requests"].append(request)
            return (state["handler"] or default_handler)(request)

        kwargs["transport"] = httpx.MockTransport(handler)
        real_init(self, *args, **kwargs)
        state["clients"].append(self)

    monkeypatch.setattr(httpx.AsyncClient, "__init__", init)
    # Public-looking literal IPs don't need DNS; keep check_url real but offline.
    return state


URL = "http://93.184.216.34/page"


def test_client_reused_across_fetches_same_proxy(mock_httpx):
    async def run():
        c = StealthClient()
        r1 = await c._fetch_plain(URL, None, 5)
        r2 = await c._fetch_plain(URL, None, 5)
        await H.aclose_plain_clients()
        return r1, r2

    r1, r2 = asyncio.run(run())
    assert r1.status_code == r2.status_code == 200 and not r1.error
    assert len(mock_httpx["clients"]) == 1
    assert len(mock_httpx["requests"]) == 2


def test_different_proxies_get_different_clients(mock_httpx):
    async def run():
        c = StealthClient()
        await c._fetch_plain(URL, "http://proxy-a:8080", 5)
        await c._fetch_plain(URL, "http://proxy-b:8080", 5)
        await c._fetch_plain(URL, "http://proxy-a:8080", 5)
        await H.aclose_plain_clients()

    asyncio.run(run())
    assert mock_httpx["proxies"] == ["http://proxy-a:8080", "http://proxy-b:8080"]


def test_clients_not_shared_across_event_loops(mock_httpx):
    async def run():
        await StealthClient()._fetch_plain(URL, None, 5)

    asyncio.run(run())
    asyncio.run(run())
    assert len(mock_httpx["clients"]) == 2


def test_timeout_applied_per_request(mock_httpx):
    async def run():
        c = StealthClient()
        await c._fetch_plain(URL, None, 3)
        await c._fetch_plain(URL, None, 11)
        await H.aclose_plain_clients()

    asyncio.run(run())
    t1, t2 = (r.extensions["timeout"] for r in mock_httpx["requests"])
    assert t1["read"] == 3 and t2["read"] == 11


def test_cookies_do_not_leak_between_fetches(mock_httpx):
    def handler(request):
        if request.url.path == "/set":
            return httpx.Response(
                200, text="x" * 600, headers={"set-cookie": "session=tenantA; Path=/"}
            )
        return httpx.Response(200, text="ok")

    mock_httpx["handler"] = handler

    async def run():
        c = StealthClient()
        await c._fetch_plain("http://93.184.216.34/set", None, 5)
        await c._fetch_plain("http://93.184.216.34/other", None, 5)
        client = mock_httpx["clients"][0]
        jar_size = len(client.cookies.jar)
        await H.aclose_plain_clients()
        return jar_size

    jar_size = asyncio.run(run())
    assert len(mock_httpx["clients"]) == 1
    assert jar_size == 0
    assert "cookie" not in mock_httpx["requests"][1].headers


def test_cookies_carried_within_one_redirect_chain_only(mock_httpx):
    def handler(request):
        if request.url.path == "/login":
            return httpx.Response(
                302, headers={"location": "/home", "set-cookie": "sid=abc; Path=/"}
            )
        return httpx.Response(200, text="home")

    mock_httpx["handler"] = handler

    async def run():
        c = StealthClient()
        await c._fetch_plain("http://93.184.216.34/login", None, 5)
        await c._fetch_plain("http://93.184.216.34/home", None, 5)
        await H.aclose_plain_clients()

    asyncio.run(run())
    reqs = mock_httpx["requests"]
    assert [r.url.path for r in reqs] == ["/login", "/home", "/home"]
    assert reqs[1].headers.get("cookie") == "sid=abc"  # same fetch: replayed
    assert "cookie" not in reqs[2].headers  # later fetch: not leaked


def test_check_url_runs_on_every_redirect_hop(mock_httpx, monkeypatch):
    def handler(request):
        n = int(request.url.path.strip("/") or 0)
        if n < 3:
            return httpx.Response(302, headers={"location": f"/{n + 1}"})
        return httpx.Response(200, text="done")

    mock_httpx["handler"] = handler
    checked = []
    real_check = H.check_url

    def spy(url, **kw):
        checked.append((url, kw))
        return real_check(url, **kw)

    monkeypatch.setattr(H, "check_url", spy)

    async def run():
        r = await StealthClient()._fetch_plain("http://93.184.216.34/0", None, 5)
        await H.aclose_plain_clients()
        return r

    r = asyncio.run(run())
    assert r.status_code == 200
    assert [u for u, _ in checked] == [f"http://93.184.216.34/{i}" for i in range(4)]
    assert all(kw == {"allow_http": True, "resolve": True} for _, kw in checked)


def test_redirect_to_private_target_blocked_with_pooled_client(mock_httpx):
    mock_httpx["handler"] = lambda req: httpx.Response(
        302, headers={"location": "http://127.0.0.1/admin"}
    )

    async def run():
        c = StealthClient()
        r = await c._fetch_plain(URL, None, 5)
        await H.aclose_plain_clients()
        return r

    r = asyncio.run(run())
    assert r.status_code == 0 and "private/blocked" in r.error
    assert [str(q.url) for q in mock_httpx["requests"]] == [URL]


def test_too_many_redirects_still_enforced(mock_httpx):
    mock_httpx["handler"] = lambda req: httpx.Response(302, headers={"location": URL})

    async def run():
        r = await StealthClient()._fetch_plain(URL, None, 5)
        await H.aclose_plain_clients()
        return r

    r = asyncio.run(run())
    assert r.status_code == 0 and "too many redirects" in r.error
    assert len(mock_httpx["requests"]) == H._MAX_REDIRECTS + 1


def test_pool_is_bounded_and_evicted_clients_closed(mock_httpx, monkeypatch):
    monkeypatch.setattr(H, "_MAX_PLAIN_CLIENTS", 2)

    async def run():
        c = StealthClient()
        for p in ("http://p1:1", "http://p2:1", "http://p3:1"):
            await c._fetch_plain(URL, p, 5)
        pool = H._plain_clients[asyncio.get_running_loop()]
        keys = list(pool)
        await H.aclose_plain_clients()
        return keys

    keys = asyncio.run(run())
    assert keys == ["http://p2:1", "http://p3:1"]
    assert mock_httpx["clients"][0].is_closed


def test_aclose_plain_clients_closes_and_clears(mock_httpx):
    async def run():
        await StealthClient()._fetch_plain(URL, None, 5)
        loop = asyncio.get_running_loop()
        assert loop in H._plain_clients
        await H.aclose_plain_clients()
        return loop in H._plain_clients

    assert asyncio.run(run()) is False
    assert mock_httpx["clients"][0].is_closed
