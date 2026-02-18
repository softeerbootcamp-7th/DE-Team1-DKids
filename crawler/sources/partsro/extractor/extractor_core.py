#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
from typing import Iterable, Optional
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse

import requests


DEFAULT_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)


def fetch(url: str, timeout: int = 15) -> str:
    """Fetch a URL and return the response body as text.

    Args:
        url: Target URL.
        timeout: Request timeout in seconds.

    Returns:
        Response text decoded as UTF-8 (with replacement on errors).
    """
    try:
        # Send HTTP GET request
        # Returns a requests.models.Response object
        resp = requests.get(
            url,
            headers={"User-Agent": DEFAULT_UA},
            timeout=timeout,
        )

        # Raise HTTPError if status code is 4xx / 5xx
        resp.raise_for_status()

        # Guess encoding from response content (useful when server sends wrong encoding)
        resp.encoding = resp.apparent_encoding

        # Return decoded text
        return resp.text

    except requests.exceptions.RequestException as e:
        # Network error, timeout, DNS error, HTTP error etc.
        raise RuntimeError(f"Failed to fetch {url}: {e}") from e


def build_api_url(
    list_url: str,
    page: int,
    count: int,
    supplier_code: str,
    b_init_more: str = "F",
) -> str:
    """Build the product list API URL for a given category page.

    Args:
        list_url: List page URL containing `cate_no`.
        page: Page number.
        count: Items per page.
        supplier_code: Supplier code required by the API.
        b_init_more: List paging flag used by the site.

    Returns:
        API URL to fetch product list JSON.

    Raises:
        ValueError: If `cate_no` is missing in the list URL.
    """

    # parse list URL
    parts = urlparse(list_url)
    q = parse_qs(parts.query)

    # extract cate_no from query
    cate_no = q.get("cate_no", [None])[0]
    if not cate_no:
        raise ValueError("list-url must include cate_no query param when using API")
    
    # API endpoint path
    api_path = "/exec/front/Product/ApiProductNormal"
    
    # API query parameters
    api_q = {
        "cate_no": cate_no,
        "supplier_code": supplier_code,
        "page": str(page),
        "bInitMore": b_init_more,
        "count": str(count),
    }

    # rebuild URL using same scheme + host
    return urlunparse((parts.scheme, parts.netloc, api_path, "", urlencode(api_q), ""))


def replace_cate_no(list_url: str, cate_no: int) -> str:
    """Return a list URL with cate_no replaced safely.

    Args:
        list_url: Source list URL.
        cate_no: Target category number.

    Returns:
        URL with updated cate_no query parameter.
    """
    parts = urlparse(list_url)
    query = parse_qs(parts.query, keep_blank_values=True)
    query["cate_no"] = [str(cate_no)]
    query_text = urlencode(query, doseq=True)
    return urlunparse(
        (parts.scheme, parts.netloc, parts.path, parts.params, query_text, parts.fragment)
    )


def parse_api_payload(payload: str, base_url: str) -> tuple[list[str], bool]:
    """Parse API JSON payload into product detail URLs.

    The product list endpoint provided by the site returns JSON like:
      {"rtn_code":"1000","rtn_data":{"data":[{...}], "end":false}}

    Args:
        payload: API response body.
        base_url: Base URL to resolve relative detail links.

    Returns:
        A tuple of (detail_urls, is_end).
    """

    # parse JSON payload
    try:
        data = json.loads(payload)
    except json.JSONDecodeError:
        return [], True

    # validate top-level structure
    if not isinstance(data, dict):
        return [], True

    rtn = data.get("rtn_data")
    if not isinstance(rtn, dict):
        return [], True

    raw_list = rtn.get("data")
    if not isinstance(raw_list, list):
        return [], True

    # extract detail URLs
    items: list[str] = []
    for p in raw_list:
        if not isinstance(p, dict):
            continue

        href = p.get("link_product_detail")
        if not href:
            continue

        # resolve relative URL → absolute URL
        items.append(urljoin(base_url, href))

    # API end flag
    is_end = bool(rtn.get("end"))

    return items, is_end


def iter_detail_urls(
    list_url: str,
    max_pages: Optional[int],
    count: int,
    supplier_code: str,
) -> Iterable[str]:
    """Yield product detail URLs from a list page.

    Args:
        list_url: List page URL containing `cate_no`.
        max_pages: Optional max pages to fetch.
        count: Items per page.
        supplier_code: Supplier code required by the API.

    Yields:
        Product detail URLs.
    """
    page = 1
    while True:
        # stop if max_pages limit is reached
        if max_pages is not None and page > max_pages:
            break

        # build paginated API URL
        api_url = build_api_url(
            list_url=list_url,
            page=page,
            count=count,
            supplier_code=supplier_code,
            b_init_more="F",
        )

        # request API
        payload = fetch(api_url)

        # parse items and end flag
        items, is_end = parse_api_payload(payload, list_url)
        
        # stop if no items returned
        if not items:
            break

        # yield each detail URL
        for url in items:
            yield url

        # stop if API indicates last page
        if is_end:
            break
        
        # next page
        page += 1


def extract_all_detail_urls(
    *,
    base_list_url: str,
    max_pages: Optional[int],
    count: int,
    supplier_code: str,
    category_map: dict[int, str],
) -> list[str]:
    """Extract detail URLs across multiple categories.

    Args:
        base_list_url: Base list URL to replace cate_no.
        max_pages: Optional max pages to fetch.
        count: Items per page.
        supplier_code: Supplier code required by the API.
        category_map: Category ID to name mapping.

    Returns:
        List of detail URLs.
    """

    # Final list to store all collected detail URLs
    detailed_urls: list[str] = []

    # Iterate over each category ID
    for cate_no in category_map.keys():
        # Update cate_no without relying on literal string replacement.
        list_url = replace_cate_no(base_list_url, cate_no)

        # iter_detail_urls() is expected to iterate detail URLs from paginated list
        for url in iter_detail_urls(list_url, max_pages, count, supplier_code):
            detailed_urls.append(url)

    # Return the aggregated list
    return detailed_urls
