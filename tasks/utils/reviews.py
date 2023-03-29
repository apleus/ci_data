from datetime import datetime
import json
import logging
import random

from requests_html import HTMLSession

import utils.user_agents as user_agents

class Reviews:
    """Review scraper for a single product on Amazon marketplace.
    
    Attributes:
        session: HTML Session.
        headers: Dict w/ User Agent string.
        product_id: Unique Amazon product ID.
        url: URL for one page of reviews, sorted by most recent.
    """

    def __init__(self, product_id):
        """Initializes instance based on product ID.
        
        Args:
            product_id (str): Unique Amazon product ID.
        """
        self.session = HTMLSession()
        self.headers = {'User-Agent': user_agents.get_ua()}
        self.product_id = product_id
        self.url = (f'https://www.amazon.com/product-reviews/{self.product_id}'
                    f'/ref=cm_cr_arp_d_viewopt_srt?sortBy=recent&pageNumber=')


    def get_page(self, index):
        """Returns list of reviews (html) from single page of reviews.

        Args:
            index (int): Page index of reviews to scrape.
        Returns:
            reviews (list): List of reviews (html).
        """
        page = self.session.get(self.url + str(index), headers=self.headers)
        page.html.render(sleep=random.random()*3) # circumvent captcha
        reviews = page.html.xpath('//div[@data-hook="review"]')
        return False if not reviews else reviews

    
    def parse_single_review(self, r):
        """Parses single review to extract review content and metadata.

        Args:
            r (html): Single product review.
        Returns:
            review (dict): Dict of review content.
        """
        temp_ld = r.xpath('//span[@data-hook="review-date"]', first=True).text
        location, date = temp_ld.split(" on ")
        if "United States" not in location: return None
        other = ''
        verified = ''
        try:
            other = r.xpath('//a[@data-hook="format-strip"]', first=True).text
        except AttributeError as e:
            logging.info(e)
        try:
            verified = r.xpath('//span[@data-hook="avp-badge"]', first=True).text
        except AttributeError as e:
            logging.info(e)
            
        review = {
            'product_id': self.product_id,
            'review_id': r.attrs['id'],
            'name': r.xpath('//span[@class="a-profile-name"]', first=True).text,
            'rating': r.xpath(('//i[contains(@data-hook, "review-star-rating") '
                              'or contains(@data-hook, "cmps-review-star-rating")]'),
                              first=True).text,
            'title': r.xpath('//a[@data-hook="review-title"]', first=True).text,
            'location': location,
            'date': date,
            'other': other,
            'verified': verified,
            'body': r.xpath('//span[@data-hook="review-body"]', first=True).text
        }
        return review
    

    def parse_pages(self, page_num):
        """Compile reviews of a specified number of pages into a single list.

        Args:
            page_num (int): Number of pages of reviews to scrape.
        Returns:
            all_reviews (list): :List of dicts of reviews.
        """
        reviews = []
        for i in range(1, page_num + 1):
            page_of_reviews = self.get_page(i)
            if page_of_reviews is not False:
                for r in page_of_reviews:
                    review_dict = self.parse_single_review(r)
                    if review_dict: reviews.append(review_dict)
                logging.info(f'Scraped page {i}...')
            else:
                logging.info('End of reviews...')
                break
        return reviews
    

    def save_data(self, results):
        """Save review data to local file: [id]-[YYYMMDD]-reviews.json.

        Args:
            results (list): List of dicts of reviews.
        """
        today = datetime.today().strftime('%Y%m%d')
        with open(f'{self.product_id}-{today}-reviews.json', 'w') as f:
            json.dump(results, f)


    def get_product_info(self):
        """Extract product brand, title, and rating + review count.
        
        Return:
            brand (str): Product brand as listed on Amazon.
            title (str): Title of product as listed on Amazon.
            review_count (int): Number of reviews for product.
        """
        page = self.session.get(self.url + '1', headers=self.headers)
        page.html.render(sleep=random.random()*3) # circumvent captcha
        title = page.html.xpath(
            '//div[@class="a-row product-title"]', first=True).text
        byline = page.html.xpath(
            '//div[@class="a-row product-by-line"]', first=True).text
        brand = byline.split('by')[1]
        review_count = page.html.xpath(
            '//div[@data-hook="cr-filter-info-review-rating-count"]',
            first=True).text
        return [brand, title, review_count]


if __name__ == '__main__':
    """Test class by scraping a few pages of reviews."""

    product_id = 'B06Y4CJVQY' # Nike Socks
    page_num = 2
    amazon_product = Reviews(product_id)

    product_info = amazon_product.get_product_info()
    results = amazon_product.parse_pages(page_num)
    amazon_product.save_data(results)

    print((f'Testing review scraper on first {page_num} pages of '
        f'{product_info[0]}\'s {product_info[1]}...'))
    print(f'This product has {product_info[2]}')
    