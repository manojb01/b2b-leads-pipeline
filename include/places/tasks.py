import json

import requests
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk.bases.hook import BaseHook
from airflow.sdk.bases.sensor import PokeReturnValue

# --- S3 Config ---
S3_BUCKET = "swacchh-leads-pipeline"
S3_PREFIX = "raw"  # s3://swacchh-leads-pipeline/raw/{h3_index}/{business_type}.json

# --- Config ---
# Business types to search for
BUSINESS_TYPES = ["restaurant", "catering_service", "hostel", "grocery_store"]

# Search radius in meters — pairs with H3 resolution 8 (~461m edge length)
SEARCH_RADIUS_METERS = 500

# Max results per API call — Places API (New) hard limit is 20
MAX_RESULTS = 20


def _get_unprocessed_coordinates(coordinates: list[dict]) -> list[dict]:
    """
    Return only coordinates whose hexagons have not been fully processed yet.
    A hexagon is fully processed when all 4 business type files exist in S3.

    S3 is the source of truth — if raw/{h3_index}/{business_type}.json exists,
    that hexagon+type was fetched. No separate tracking table needed.

    Called before dynamic task mapping to filter out already-done hexagons.
    """
    s3_hook = S3Hook(aws_conn_id="aws_s3")

    # List all objects under raw/ once — one S3 API call for all hexagons
    existing_keys = set(
        s3_hook.list_keys(bucket_name=S3_BUCKET, prefix=f"{S3_PREFIX}/") or []
    )

    unprocessed = []
    for coord in coordinates:
        h3_index = coord["h3_index"]
        # A hexagon is fully processed when every business type file exists
        all_done = all(
            f"{S3_PREFIX}/{h3_index}/{bt}.json" in existing_keys
            for bt in BUSINESS_TYPES
        )
        if not all_done:
            unprocessed.append(coord)

    processed_count = len(coordinates) - len(unprocessed)
    print(f"Total hexagons: {len(coordinates)}, already processed: {processed_count}, to fetch: {len(unprocessed)}")
    return unprocessed


def _is_api_available() -> PokeReturnValue:
    conn = BaseHook.get_connection("google_places_api")
    api_key = conn.password

    response = requests.post(
        f"{conn.host}/v1/places:searchNearby",
        headers={
            "Content-Type": "application/json",
            "X-Goog-Api-Key": api_key,
            "X-Goog-FieldMask": "places.displayName",
        },
        json={
            "includedTypes": ["restaurant"],
            "maxResultCount": 1,
            "locationRestriction": {
                "circle": {
                    "center": {"latitude": 17.4344, "longitude": 78.4487},
                    "radius": SEARCH_RADIUS_METERS,
                }
            },
        },
    )

    condition = response.status_code == 200 and "places" in response.json()
    return PokeReturnValue(is_done=condition)


def _fetch_places(lat: float, lng: float, business_type: str) -> list[dict]:
    """
    Fetch places from Google Places API (New) for a given coordinate and business type.
    """
    conn = BaseHook.get_connection("google_places_api")
    api_key = conn.password

    response = requests.post(
        f"{conn.host}/v1/places:searchNearby",
        headers={
            "Content-Type": "application/json",
            "X-Goog-Api-Key": api_key,
            "X-Goog-FieldMask": (
                "places.id,"
                "places.displayName,"
                "places.formattedAddress,"
                "places.internationalPhoneNumber,"
                "places.rating,"
                "places.userRatingCount,"
                "places.websiteUri,"
                "places.googleMapsUri,"
                "places.businessStatus,"
                "places.location"
            ),
        },
        json={
            "includedTypes": [business_type],
            "maxResultCount": MAX_RESULTS,
            "locationRestriction": {
                "circle": {
                    "center": {"latitude": lat, "longitude": lng},
                    "radius": SEARCH_RADIUS_METERS,
                }
            },
        },
    )

    if response.status_code != 200:
        raise Exception(
            f"Places API error {response.status_code} for "
            f"({lat}, {lng}) type={business_type}: {response.text}"
        )

    places = response.json().get("places", [])

    for place in places:
        place["_business_type"] = business_type
        place["_search_lat"] = lat
        place["_search_lng"] = lng

    return places


def _fetch_and_save_hexagon(coord: dict) -> list[str]:
    """
    Fetch all business types for a single hexagon and save each to S3.
    This function is called as a dynamically mapped Airflow task —
    one task instance per hexagon, all running in parallel.

    Args:
        coord: dict with lat, lng, h3_index

    Returns:
        List of S3 keys saved for this hexagon
    """
    h3_index = coord["h3_index"]
    lat = coord["lat"]
    lng = coord["lng"]
    s3_hook = S3Hook(aws_conn_id="aws_s3")
    s3_keys = []

    for business_type in BUSINESS_TYPES:
        print(f"Fetching {business_type} at {h3_index} ({lat:.4f}, {lng:.4f})")
        places = _fetch_places(lat, lng, business_type)

        for place in places:
            place["_h3_index"] = h3_index

        print(f"  → {len(places)} results")

        # Save to S3 — immutable, partitioned by h3_index/business_type
        s3_key = f"{S3_PREFIX}/{h3_index}/{business_type}.json"

        if s3_hook.check_for_key(s3_key, bucket_name=S3_BUCKET):
            print(f"  → Already exists in S3, skipping upload: {s3_key}")
        else:
            s3_hook.load_string(
                string_data=json.dumps(places, indent=2, ensure_ascii=False),
                key=s3_key,
                bucket_name=S3_BUCKET,
                replace=False,  # never overwrite — immutable archive
            )
            print(f"  → Saved to s3://{S3_BUCKET}/{s3_key}")

        # S3 file existence is the source of truth — no separate tracking needed
        s3_keys.append(s3_key)

    print(f"✅ Hexagon {h3_index} done — {len(s3_keys)} files saved")
    return s3_keys
