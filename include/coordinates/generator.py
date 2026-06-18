import h3

# Telangana + Andhra Pradesh bounding polygon (approximate)
TELANGANA_AP_POLYGON = [
    [79.5, 18.5],
    [84.5, 18.5],
    [84.5, 13.5],
    [79.5, 13.5],
    [79.5, 18.5],
]

# Ameerpet, Hyderabad — starting point of the business
AMEERPET = (17.4344, 78.4487)

# Secunderabad — distinct city in the Hyderabad metro, same state
SECUNDERABAD = (17.4399, 78.4983)

# H3 resolution 8 — ~461m edge length, pairs with 500m search radius
H3_RESOLUTION = 8

# Search radius in meters passed to Places API
SEARCH_RADIUS_METERS = 500


def get_hexagons_for_region(polygon: list[list[float]], resolution: int) -> set[str]:
    """
    Fill a polygon with H3 hexagons at the given resolution.
    Returns a set of H3 cell indexes.

    Args:
        polygon: List of [lng, lat] coordinates forming the boundary
        resolution: H3 resolution level (8 recommended)
    """
    # H3 expects [lat, lng] format — convert from [lng, lat]
    h3_polygon = h3.LatLngPoly([(lat, lng) for lng, lat in polygon])
    hexagons = h3.h3shape_to_cells(h3_polygon, resolution)
    return hexagons


def get_coordinates_for_region() -> list[dict]:
    """
    Generate a list of coordinate dicts covering Telangana + AP.
    Each dict contains lat, lng, and h3_index.

    Returns:
        List of dicts: [{"lat": ..., "lng": ..., "h3_index": ...}, ...]
    """
    hexagons = get_hexagons_for_region(TELANGANA_AP_POLYGON, H3_RESOLUTION)

    coordinates = []
    for h3_index in hexagons:
        lat, lng = h3.cell_to_latlng(h3_index)
        coordinates.append({
            "lat": lat,
            "lng": lng,
            "h3_index": h3_index,
        })

    return coordinates


def get_ameerpet_and_neighbors(rings: int = 1) -> list[dict]:
    """
    Get coordinates for Ameerpet (Hyderabad) and Secunderabad, each with
    `rings` rings of hexagons. Two cities in the same state gives meaningful
    geographic coverage for dev/testing without blowing the API quota.

    Args:
        rings: Number of rings around each center (1 = 7 hexagons per city)

    Returns:
        List of dicts: [{"lat": ..., "lng": ..., "h3_index": ...}, ...]
    """
    seen = set()
    coordinates = []
    for center in (AMEERPET, SECUNDERABAD):
        center_hex = h3.latlng_to_cell(center[0], center[1], H3_RESOLUTION)
        for h3_index in h3.grid_disk(center_hex, rings):
            if h3_index not in seen:
                seen.add(h3_index)
                lat, lng = h3.cell_to_latlng(h3_index)
                coordinates.append({"lat": lat, "lng": lng, "h3_index": h3_index})
    return coordinates
