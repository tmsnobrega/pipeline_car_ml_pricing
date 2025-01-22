import scrapy
from datetime import datetime

class AutoscoutCarSpider(scrapy.Spider):
  name = "autoscout"

  # Define search variables
  max_pages = 2
  adage = 2

  # Define a dictionary of manufacturers & models
  manufacturers_models = {
        "audi": ["a4"],
        "bmw": ["330", "340"]
    }

  def start_requests(self):
    self.current_page = 1
    for manufacturer, models in self.manufacturers_models.items():
      for model in models:
        for page in range(1, self.max_pages + 1):
          url = self.construct_url(manufacturer, model, page, adage=self.adage)
          yield scrapy.Request(url=url, callback=self.parse, meta={"manufacturer": manufacturer, "model": model, "page": page})


  def construct_url(self, manufacturer, model, page, adage=None):
    url = f"https://www.autoscout24.com/lst/{manufacturer}/{model}?body=1%2C4%2C6&cy=NL&desc=1&fregfrom=2016&sort=age"

    if adage:
      url += f"&adage={adage}"

    url += f"&page={page}"

    return url


  def parse(self, response):
    car_links = response.css("a.ListItem_title__ndA4s::attr(href)").getall()

    for link in car_links:
      absolute_url = response.urljoin(link)
      yield scrapy.Request(url=absolute_url, callback=self.parse_car, meta=response.meta)

    # Get the current page, manufacturer, and model from the response meta
    current_page = response.meta["page"]
    manufacturer = response.meta["manufacturer"]
    model = response.meta["model"]

    if current_page < self.max_pages:
      next_page_url = self.construct_url(manufacturer, model, current_page + 1, self.adage)
      yield scrapy.Request(url=next_page_url, callback=self.parse, meta={"manufacturer": manufacturer, "model": model, "page": current_page + 1})


  def parse_car(self, response):
    # Extract overview data
    overview_containers = response.css("div.VehicleOverview_itemContainer__XSLWi")
    vehicle_overview_data = {}

    for overview in overview_containers:
      label_overview = overview.css("div.VehicleOverview_itemTitle__S2_lb::text").get()
      value_overview = overview.css("div.VehicleOverview_itemText__AI4dA::text").get()
      if label_overview:
        vehicle_overview_data[label_overview.strip()] = value_overview.strip() if value_overview else None

    yield {
        # Vehicle information
        "manufacturer": response.css("span.StageTitle_boldClassifiedInfo__sQb0l::text").get(default=None),
        "car": response.css("span.StageTitle_model__EbfjC.StageTitle_boldClassifiedInfo__sQb0l::text").get(default=None),
        "description": response.css("div.StageTitle_modelVersion__Yof2Z::text").get(default=None),
        "price": response.css("span.PriceInfo_price__XU0aF::text").get(default=None),
        "lease_price_per_month": response.css("div.FinancialLeaseStage_rate__h8aCR span:nth-child(2)::text").get(default=None),

        # Overview data
        "km": vehicle_overview_data.get("Mileage"),
        "gear_type": vehicle_overview_data.get("Gearbox"),
        "built_in": vehicle_overview_data.get("First registration"),
        "fuel": vehicle_overview_data.get("Fuel type"),
        "engine_power": vehicle_overview_data.get("Power"),
        "seller_type": vehicle_overview_data.get("Seller"),

        # Basic data
        "body_type": response.css("dt:contains('Body type') + dd::text").get(default=None),
        "used_or_new": response.css("dt:contains('Type') + dd::text").get(default=None),
        "drive_train": response.css("dt:contains('Drivetrain') + dd::text").get(default=None),
        "seats": response.css("dt:contains('Seats') + dd::text").get(default=None),
        "doors": response.css("dt:contains('Doors') + dd::text").get(default=None),

        # Vehicle history
        "previous_owners": response.css("dt:contains('Previous owner') + dd::text").get(default=None),
        "full_service_history": response.css("dt:contains('Full service history') + dd::text").get(default=None),
        "non-smoker": response.css("dt:contains('Non-smoker vehicle') + dd::text").get(default=None),

        # Technical data
        "engine_size": response.css("dt:contains('Engine size') + dd::text").get(default=None),
        "gears": response.css("dt:contains('Gears') + dd::text").get(default=None),
        "cylinders": response.css("dt:contains('Cylinders') + dd::text").get(default=None),
        "empty_weight": response.css("dt:contains('Empty weight') + dd::text").get(default=None),

        # Energy consumption
        "emission_class": response.css("dt:contains('Emission class') + dd::text").get(default=None),
        "fuel_consumption": " ".join(response.css("dt:contains('Fuel consumption') + dd p::text").getall()),
        "co2_emission": response.css("dt:contains('CO₂-emissions') + dd::text").get(default=None),
        "electric_range": response.css("dt:contains('Electric Range') + dd::text").get(default=None),

        # Appearance
        "car_color": response.css("dt:contains('Colour') + dd::text").get(default=None),
        "manufacturer_color": response.css("dt:contains('Manufacturer colour') + dd::text").get(default=None),
        "paint": response.css("dt:contains('Paint') + dd::text").get(default=None),
        "upholstery_color": response.css("dt:contains('Upholstery colour') + dd::text").get(default=None),
        "upholstery": response.css("dt:contains('Upholstery') + dd::text").get(default=None),

        # Equipment
        "equipment": response.css("dd.DataGrid_defaultDdStyle__3IYpG ul li::text").getall() or None,

        # Seller details
        "seller_name": response.css("div.RatingsAndCompanyName_dealer__EaECM div::text").get(default=None),
        "active_since": response.css("span.RatingsAndCompanyName_customerSince__Zf7h4::text").get(default=None),
        "seller_address_1": response.css("a.scr-link.Department_link__xMUEe::text").getall()[0] if response.css("a.scr-link.Department_link__xMUEe::text").getall() else None,
        "seller_address_2": response.css("a.scr-link.Department_link__xMUEe::text").getall()[3] if len(response.css("a.scr-link.Department_link__xMUEe::text").getall()) > 3 else None,

        # Metadata
        "listing_url": response.url,
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

# cd scrapy/src
# scrapy crawl autoscout -o ../../data/raw/autoscout.jsonl
