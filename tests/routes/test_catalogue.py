"""`GET /catalogue` and `GET /catalogue/{id}` — the DCAT-AP 3 surface.

The single-entry path is served by two routers: the HTML view in `routes/views.py`
is registered first and would swallow every request, so these tests pin the
content negotiation that hands JSON clients back to the JSON-LD handler.
"""
from __future__ import annotations

import json

from celine.dataset.db.models.dataset_entry import DatasetEntry

BROWSER_ACCEPT = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"


async def _entry(session, **kw) -> DatasetEntry:
    entry = DatasetEntry(
        dataset_id=kw.pop("dataset_id", "ds.gold.pv"),
        title="PV",
        backend_type=kw.pop("backend_type", "postgres"),
        backend_config=kw.pop("backend_config", {"table": "pv"}),
        expose=kw.pop("expose", True),
        access_level=kw.pop("access_level", "open"),
        **kw,
    )
    session.add(entry)
    await session.commit()
    return entry


async def test_listing_is_json_ld(client, test_session):
    await _entry(test_session)
    res = await client.get("/catalogue")
    assert res.status_code == 200
    assert res.headers["content-type"].startswith("application/ld+json")


async def test_entry_asked_for_ld_json_is_not_shadowed_by_the_html_view(
    client, test_session
):
    """The reported 500: the HTML view owns this path, so a JSON-LD client used
    to render (and crash on) a template instead of getting its document."""
    await _entry(test_session)
    res = await client.get(
        "/catalogue/ds.gold.pv", headers={"accept": "application/ld+json"}
    )
    assert res.status_code == 200
    assert res.headers["content-type"].startswith("application/ld+json")
    assert json.loads(res.text)["@id"].endswith("ds.gold.pv")


async def test_entry_without_accept_header_is_json_ld(client, test_session):
    """`curl` sends `*/*`. An API path defaults to the API representation."""
    await _entry(test_session)
    res = await client.get("/catalogue/ds.gold.pv", headers={"accept": "*/*"})
    assert res.status_code == 200
    assert res.headers["content-type"].startswith("application/ld+json")


async def test_entry_matches_the_listing(client, test_session):
    """Same dataset, same document — a client may fetch either and must not have
    to reconcile two shapes."""
    await _entry(test_session)
    listed = json.loads((await client.get("/catalogue")).text)
    single = json.loads(
        (
            await client.get(
                "/catalogue/ds.gold.pv", headers={"accept": "application/ld+json"}
            )
        ).text
    )
    from_listing = [d for d in listed["dcat:dataset"] if d["@id"] == single["@id"]]
    assert len(from_listing) == 1
    assert from_listing[0]["dct:title"] == single["dct:title"]


async def test_browser_still_gets_the_html_page(client, test_session):
    """The negotiation must not cost the human-facing catalogue."""
    await _entry(test_session, backend_type="fs", backend_config={})
    res = await client.get("/catalogue/ds.gold.pv", headers={"accept": BROWSER_ACCEPT})
    assert res.status_code == 200
    assert res.headers["content-type"].startswith("text/html")
    assert "ds.gold.pv" in res.text


async def test_index_page_renders(client, test_session):
    """`/` uses the same template call that broke the entry page."""
    await _entry(test_session)
    res = await client.get("/", headers={"accept": BROWSER_ACCEPT})
    assert res.status_code == 200
    assert res.headers["content-type"].startswith("text/html")


async def test_unexposed_entry_is_404_for_json(client, test_session):
    await _entry(test_session, expose=False)
    res = await client.get(
        "/catalogue/ds.gold.pv", headers={"accept": "application/ld+json"}
    )
    assert res.status_code == 404


async def test_secret_entry_is_404_for_json(client, test_session):
    await _entry(test_session, access_level="secret")
    res = await client.get(
        "/catalogue/ds.gold.pv", headers={"accept": "application/ld+json"}
    )
    assert res.status_code == 404


# ------------------------------------------------------------------------------
# Visibility — the HTML surface answers the same anonymous caller as the JSON
# one, so it must hide exactly as much. It used to hide nothing.
# ------------------------------------------------------------------------------


async def test_unexposed_entry_is_404_as_html(client, test_session):
    await _entry(test_session, expose=False)
    res = await client.get("/catalogue/ds.gold.pv", headers={"accept": BROWSER_ACCEPT})
    assert res.status_code == 404


async def test_secret_entry_is_404_as_html(client, test_session):
    """The strongest case: `secret` withholds the metadata itself, and the page
    is metadata."""
    await _entry(test_session, access_level="secret")
    res = await client.get("/catalogue/ds.gold.pv", headers={"accept": BROWSER_ACCEPT})
    assert res.status_code == 404


async def test_dataspace_offer_alone_does_not_publish_a_page(client, test_session):
    """`dataspace_expose` is consent to offer the dataset to a contracted
    consumer over EDR — not to show it to whoever opens the browser. The export
    rejects this pairing; an entry written through the admin API can still carry
    it."""
    await _entry(test_session, expose=False, dataspace_expose=True)
    assert (
        await client.get("/catalogue/ds.gold.pv", headers={"accept": BROWSER_ACCEPT})
    ).status_code == 404
    assert (
        await client.get(
            "/catalogue/ds.gold.pv", headers={"accept": "application/ld+json"}
        )
    ).status_code == 404


async def test_index_page_lists_only_what_the_catalogue_may_show(client, test_session):
    await _entry(test_session, dataset_id="ds.gold.public")
    await _entry(test_session, dataset_id="ds.gold.hidden", expose=False)
    await _entry(test_session, dataset_id="ds.gold.classified", access_level="secret")

    res = await client.get("/", headers={"accept": BROWSER_ACCEPT})
    assert res.status_code == 200
    assert "ds.gold.public" in res.text
    assert "ds.gold.hidden" not in res.text
    assert "ds.gold.classified" not in res.text


async def test_null_access_level_is_not_secret(client, test_session):
    """`access_level` is nullable, and SQL drops NULL rows on a bare `!=`. An
    entry that states no level must stay listed."""
    await _entry(test_session, access_level=None)
    res = await client.get("/catalogue/ds.gold.pv", headers={"accept": "*/*"})
    assert res.status_code == 200


async def test_secret_entry_leaks_no_schema_or_vocabulary(client, test_session):
    """The sub-resources used to gate on `expose` alone, so a secret-but-exposed
    entry handed out its column names."""
    await _entry(test_session, access_level="secret")
    assert (await client.get("/catalogue/ds.gold.pv/schema")).status_code == 404
    assert (await client.get("/catalogue/ds.gold.pv/vocabulary")).status_code == 404
