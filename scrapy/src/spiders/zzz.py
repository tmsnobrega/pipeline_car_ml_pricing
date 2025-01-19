import scrapy
from datetime import datetime

class SingleCarSpider(scrapy.Spider):
    name = "old_test_single_car"

    start_urls = [
        "https://www.autoscout24.com/offers/audi-a7-sportback-50-tfsi-e-quattro-pro-line-s-panoramad-electric-gasoline-blue-5aa0056b-da9a-481b-9895-4b01a4aa2fdc?ipc=recommendation&ipl=detailpage-engine-itemBased&source=detailpage_recommender&position=1&source_otp=t30&ap_tier=t30&mia_tier=t40&boosting_product=none&relevance_adjustment=organic&applied_mia_tier=t40"
    ]

    def parse(self, response):
        """Extract detailed car information."""
        data_grid_values = response.css("dd.DataGrid_defaultDdStyle__3IYpG.DataGrid_fontBold__RqU01::text").getall()

        yield {
            "manufacturer": response.css("span.StageTitle_boldClassifiedInfo__sQb0l::text").get(),
            "car": response.css("span.StageTitle_model__EbfjC.StageTitle_boldClassifiedInfo__sQb0l::text").get(),
            "description": response.css("div.StageTitle_modelVersion__Yof2Z::text").get(),
            "price": response.css("span.PriceInfo_price__XU0aF::text").get(),
            "lease_price_per_month": response.css("div.FinancialLeaseStage_rate__h8aCR span:nth-child(2)::text").get(),
            "km": response.css("div.VehicleOverview_itemText__AI4dA::text").getall()[0],
            "gear_type": response.css("div.VehicleOverview_itemText__AI4dA::text").getall()[1],
            "built_in": response.css("div.VehicleOverview_itemText__AI4dA::text").getall()[2],
            "fuel": response.css("div.VehicleOverview_itemText__AI4dA::text").getall()[3],
            "engine_power": response.css("div.VehicleOverview_itemText__AI4dA::text").getall()[4],
            "seller_type": response.css("div.VehicleOverview_itemText__AI4dA::text").getall()[5],
            "body_type": data_grid_values[0] if len(data_grid_values) > 0 else None,
            "used_or_new": data_grid_values[1] if len(data_grid_values) > 1 else None,
            "drive_train": data_grid_values[2] if len(data_grid_values) > 2 else None,
            "seats": data_grid_values[3] if len(data_grid_values) > 3 else None,
            "doors": data_grid_values[4] if len(data_grid_values) > 4 else None,
            "previous_owners": data_grid_values[7] if len(data_grid_values) > 7 else None,
            "full_service_history": data_grid_values[8] if len(data_grid_values) > 8 else None,
            "engine_size": data_grid_values[11] if len(data_grid_values) > 11 else None,
            "cylinders": data_grid_values[12] if len(data_grid_values) > 12 else None,
            "empty_weight": data_grid_values[13] if len(data_grid_values) > 13 else None,
            "car_color": data_grid_values[15] if len(data_grid_values) > 15 else None,
            "upholstery_color": data_grid_values[18] if len(data_grid_values) > 18 else None,
            "upholstery": data_grid_values[19] if len(data_grid_values) > 19 else None,
            "equipment": response.css("dd.DataGrid_defaultDdStyle__3IYpG ul li::text").getall(),
            "seller_name": response.css("div.RatingsAndCompanyName_dealer__EaECM div::text").get(),
            "active_since": response.css("span.RatingsAndCompanyName_customerSince__Zf7h4::text").get(),
            "seller_address_1": response.css("a.scr-link.Department_link__xMUEe::text").getall()[0],
            "seller_address_2": response.css("a.scr-link.Department_link__xMUEe::text").getall()[3],
            "listing_url": response.url,
            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
