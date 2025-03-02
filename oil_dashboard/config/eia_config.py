EIA_CRUDE_INVENTORY_REQUEST_PARAMS = {
    "frequency": "weekly",
    "data[0]": ["value"],  # This is key — asks for actual values
    "facets[product][]": ["EPC0"],  # Crude Oil
    "facets[duoarea][]": ["NUS"],  # Total U.S.
    "facets[process][]": ["SAE"],  # Ending Stocks
    "sort[0][column]": "period",
    "sort[0][direction]": "desc",
    "offset": 0,
    "length": 100,
}
