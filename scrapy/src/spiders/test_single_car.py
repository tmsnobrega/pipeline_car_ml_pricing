import scrapy
from datetime import datetime

class SingleCarSpider(scrapy.Spider):
    name = "test_single_car"

    start_urls = [
        "https://www.autoscout24.com/offers/audi-a7-sportback-50-tfsi-e-quattro-plug-in-300pk-pano-mat-electric-gasoline-grey-b18ed873-a64b-4504-8811-7a27affb947f?ipc=recommendation&ipl=detailpage-engine-itemBased&source=detailpage_recommender&position=3&source_otp=t20&ap_tier=t20&mia_tier=t20&boosting_product=none&relevance_adjustment=organic&applied_mia_tier=t20"
    ]

    def parse(self, response):
        """Extract detailed car information safely, avoiding field misalignment."""

        # Extract detailed data
        overview_containers = response.css("div.VehicleOverview_itemContainer__XSLWi")
        vehicle_overview_data = {}

        for overview in overview_containers:
            label_overview = overview.css("div.VehicleOverview_itemTitle__S2_lb::text").get()
            value_overview = overview.css("div.VehicleOverview_itemText__AI4dA::text").get()
            if label_overview:
                vehicle_overview_data[label_overview.strip()] = value_overview.strip() if value_overview else None

        # Extract overview data
        detailed_containers = response.css("div.DetailsSection_childrenSection__aElbi")
        vehicle_detailed_data = {}

        for detailed in detailed_containers:
            label_detailed = [label.strip() for label in detailed.css("dt.DataGrid_defaultDtStyle__soJ6R::text").getall()]
            value_detailed = [value.strip() for value in detailed.css("dd.DataGrid_defaultDdStyle__3IYpG.DataGrid_fontBold__RqU01::text").getall()]
            if label_detailed:
                for label, value in zip(label_detailed, value_detailed):
                    vehicle_detailed_data[label] = " ".join(value) if isinstance(value, list) else value if value else None

        yield {
            "manufacturer": response.css("span.StageTitle_boldClassifiedInfo__sQb0l::text").get(),
            "car": response.css("span.StageTitle_model__EbfjC.StageTitle_boldClassifiedInfo__sQb0l::text").get(),
            "description": response.css("div.StageTitle_modelVersion__Yof2Z::text").get(),
            "price": response.css("span.PriceInfo_price__XU0aF::text").get(),
            "lease_price_per_month": response.css("div.FinancialLeaseStage_rate__h8aCR span:nth-child(2)::text").get(),
            "km": vehicle_overview_data.get("Mileage", None),
            "gear_type": vehicle_overview_data.get("Gearbox", None),
            "built_in": vehicle_overview_data.get("First registration", None),
            "fuel": vehicle_overview_data.get("Fuel type", None),
            "engine_power": vehicle_overview_data.get("Power", None),
            "seller_type": vehicle_overview_data.get("Seller", None),
            "body_type": vehicle_detailed_data.get("Body type", None),
            "used_or_new": vehicle_detailed_data.get("Type", None),
            "drive_train": vehicle_detailed_data.get("Drivetrain", None),
            "seats": vehicle_detailed_data.get("Seats", None),
            "doors": vehicle_detailed_data.get("Doors", None),
            "previous_owners": vehicle_detailed_data.get("Previous owner", None),
            "full_service_history": vehicle_detailed_data.get("Full service history", None),
            "engine_size": vehicle_detailed_data.get("Engine size", None),
            "gears": vehicle_detailed_data.get("Gears", None),
            "cylinders": vehicle_detailed_data.get("Cylinders", None),
            "empty_weight": vehicle_detailed_data.get("Empty weight", None),
            "emission_class": vehicle_detailed_data.get("Emission class", None),
            "co2_emission": vehicle_detailed_data.get("CO₂-emissions", None),
            "car_color": vehicle_detailed_data.get("Colour", None),
            "manufacturer_color": vehicle_detailed_data.get("Manufacturer colour", None),
            "paint": vehicle_detailed_data.get("Paint", None),
            "upholstery_color": vehicle_detailed_data.get("Upholstery colour", None),
            "upholstery": vehicle_detailed_data.get("Upholstery", None),
            "equipment": response.css("dd.DataGrid_defaultDdStyle__3IYpG ul li::text").getall(),
            "seller_name": response.css("div.RatingsAndCompanyName_dealer__EaECM div::text").get(),
            "active_since": response.css("span.RatingsAndCompanyName_customerSince__Zf7h4::text").get(),
            "seller_address_1": response.css("a.scr-link.Department_link__xMUEe::text").getall()[0] if response.css("a.scr-link.Department_link__xMUEe::text").getall() else None,
            "seller_address_2": response.css("a.scr-link.Department_link__xMUEe::text").getall()[3] if len(response.css("a.scr-link.Department_link__xMUEe::text").getall()) > 3 else None,
            "listing_url": response.url,
            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
