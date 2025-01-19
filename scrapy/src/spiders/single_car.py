import scrapy
from datetime import datetime

class SingleCarSpider(scrapy.Spider):
    name = "single_car"

    start_urls = [
        "https://www.autoscout24.com/offers/tesla-model-3-75-kwh-long-range-dual-motor-awd-b2b-only-electric-black-014864ad-f4ae-4e40-8f82-da55d12bed62?sort=standard&desc=0&lastSeenGuidPresent=false&cldtidx=1&position=1&search_id=138xpk8a146&source_otp=nfm&ap_tier=t50&source=listpage_search-results&order_bucket=0&new_taxonomy_available=false&mia_tier=t50&relevance_adjustment=sponsored&applied_mia_tier=t50&boosting_product=mia&topspot-algorithm=nfm-default&topspot-dealer-id=44151164"
    ]

    def parse(self, response):
        # Extract overview data
        overview_containers = response.css("div.VehicleOverview_itemContainer__XSLWi")
        vehicle_overview_data = {}

        for overview in overview_containers:
            label_overview = overview.css("div.VehicleOverview_itemTitle__S2_lb::text").get()
            value_overview = overview.css("div.VehicleOverview_itemText__AI4dA::text").get()
            if label_overview:
                vehicle_overview_data[label_overview.strip()] = value_overview.strip() if value_overview else None

        # Extract detailed data
        detailed_containers = response.css("div.DetailsSection_childrenSection__aElbi")
        vehicle_detailed_data = {}

        for detailed in detailed_containers:
            label_detailed = [label.strip() for label in detailed.css("dt.DataGrid_defaultDtStyle__soJ6R::text").getall()]
            value_detailed = [value.strip() for value in detailed.css("dd.DataGrid_defaultDdStyle__3IYpG.DataGrid_fontBold__RqU01::text").getall()]
            
            if label_detailed:
                for label, value in zip(label_detailed, value_detailed):
                    vehicle_detailed_data[label] = value if value else None

        yield {
            "manufacturer": response.css("span.StageTitle_boldClassifiedInfo__sQb0l::text").get(default=None),
            "car": response.css("span.StageTitle_model__EbfjC.StageTitle_boldClassifiedInfo__sQb0l::text").get(default=None),
            "description": response.css("div.StageTitle_modelVersion__Yof2Z::text").get(default=None),
            "price": response.css("span.PriceInfo_price__XU0aF::text").get(default=None),
            "lease_price_per_month": response.css("div.FinancialLeaseStage_rate__h8aCR span:nth-child(2)::text").get(default=None),
            "km": vehicle_overview_data.get("Mileage"),
            "gear_type": vehicle_overview_data.get("Gearbox"),
            "built_in": vehicle_overview_data.get("First registration"),
            "fuel": vehicle_overview_data.get("Fuel type"),
            "engine_power": vehicle_overview_data.get("Power"),
            "seller_type": vehicle_overview_data.get("Seller"),
            "body_type": vehicle_detailed_data.get("Body type"),
            "used_or_new": vehicle_detailed_data.get("Type"),
            "drive_train": vehicle_detailed_data.get("Drivetrain"),
            "seats": vehicle_detailed_data.get("Seats"),
            "doors": vehicle_detailed_data.get("Doors"),
            "engine_size": vehicle_detailed_data.get("Engine size"),
            "gears": vehicle_detailed_data.get("Gears"),
            "cylinders": vehicle_detailed_data.get("Cylinders"),
            "empty_weight": vehicle_detailed_data.get("Empty weight"),
            "emission_class": vehicle_detailed_data.get("Emission class"),
            "co2_emission": vehicle_detailed_data.get("CO₂-emissions"),
            "eletric_range": vehicle_detailed_data.get("Electric Range"),
            "car_color": vehicle_detailed_data.get("Colour"),
            "manufacturer_color": vehicle_detailed_data.get("Manufacturer colour"),
            "paint": vehicle_detailed_data.get("Paint"),
            "upholstery_color": vehicle_detailed_data.get("Upholstery colour"),
            "upholstery": vehicle_detailed_data.get("Upholstery"),
            "previous_owners": response.css("dt:contains('Previous owner') + dd::text").get(default=None),
            "full_service_history": response.css("dt:contains('Full service history') + dd::text").get(default=None),
            "non-smoker": response.css("dt:contains('Non-smoker vehicle') + dd::text").get(default=None),
            "equipment": response.css("dd.DataGrid_defaultDdStyle__3IYpG ul li::text").getall() or None,
            "seller_name": response.css("div.RatingsAndCompanyName_dealer__EaECM div::text").get(default=None),
            "active_since": response.css("span.RatingsAndCompanyName_customerSince__Zf7h4::text").get(default=None),
            "seller_address_1": response.css("a.scr-link.Department_link__xMUEe::text").getall()[0] if response.css("a.scr-link.Department_link__xMUEe::text").getall() else None,
            "seller_address_2": response.css("a.scr-link.Department_link__xMUEe::text").getall()[3] if len(response.css("a.scr-link.Department_link__xMUEe::text").getall()) > 3 else None,
            "listing_url": response.url,
            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

# cd scrapy/src
# scrapy crawl single_car -o ../../single_car.jsonl