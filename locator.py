from selenium.webdriver.common.by import By
class LocatorAvito:
    TITLES = (By.CSS_SELECTOR, "div[itemtype*='http://schema.org/Product']")
    NAME = (By.CSS_SELECTOR, "[itemprop='name']")
    DESCRIPTIONS = (By.CSS_SELECTOR, "p[style='--module-max-lines-size:4']")
    DESCRIPTIONS_FULL_PAGE = (By.CSS_SELECTOR, "[data-marker='item-view/item-description']")
    URL = (By.CSS_SELECTOR, "[itemprop='url']")
    PRICE = (By.CSS_SELECTOR, "[itemprop='price']")
    TOTAL_VIEWS = (By.CSS_SELECTOR, "[data-marker='item-view/total-views']")
    DATE_PUBLIC = (By.CSS_SELECTOR, "[data-marker='item-view/item-date']")
    SELLER_NAME = (By.CSS_SELECTOR, "[data-marker='seller-info/label']")
    SELLER_LINK = (By.CSS_SELECTOR, "[data-marker='seller-link/link']")
    COMPANY_NAME = (By.CSS_SELECTOR, "[data-marker='seller-link/link']")
    COMPANY_NAME_TEXT = (By.CSS_SELECTOR, "span")
    GEO = (By.CSS_SELECTOR, "div[class*='style-item-address']")
    OTHER_GEO = (By.CSS_SELECTOR, 'div[elementtiming="bx.gallery.first-item"]')
    
    # Селекторы для изображений
    GALLERY_IMAGE = (By.CSS_SELECTOR, "img[data-marker='gallery-img']")
    IMAGE_CONTAINER = (By.CSS_SELECTOR, "div[data-marker='item-view/gallery']")
    GALLERY_SLIDES = (By.CSS_SELECTOR, "div[data-marker='item-view/gallery'] div[data-marker='slider-image/image-wrapper']")