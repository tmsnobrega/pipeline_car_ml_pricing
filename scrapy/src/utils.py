# utils.py

def load_project_variables():
    """
    Returns project configuration variables as a dictionary.
    """
    return {
        "MAX_PAGES": 20,
        "AD_AGE": 2,
        "PRICE_FROM": 5000,
        "PRICE_TO": 75000,
        "YEAR_FROM": 2016,

        # Define a dictionary of manufacturers & models
        "MANUFACTURERS_MODELS": {
            "audi": ["a3", "a4"],
            "cupra": ["formentor"],
            "honda": ["civic", "hr-v"],
            "hyundai": ["tucson"],
            "kia": ["ev6", "niro"],
            "lexus": ["ux-series-(all)"],
            "lynk-&-co": ["01"],
            "mazda": ["3", "cx-30"],
            "tesla": ["model-3", "model-y"],
            "toyota": ["c-hr", "corolla", "yaris-cross"],
            "volvo": ["s60", "xc40"]
        }
    }
