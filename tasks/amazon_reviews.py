from requests_html import HTMLSession
import random
from datetime import datetime
import json
import user_agents
import logging


class Reviews:
    def __init__(self, product_id):
        """
        Reviews element for a single product on Amazon including html session + headers,
        and product attributes of url, id
        """
        self.session = HTMLSession()
        self.headers = {'User-Agent': user_agents.get_ua()}
        self.product_id = product_id
        self.url = f'https://www.amazon.com/product-reviews/{self.product_id}/ref=cm_cr_arp_d_viewopt_srt?sortBy=recent&pageNumber='


    def get_page(self, page_num):
        """
        Return list of review html elements from single page of reviews
        (after first loading page and waiting a few seconds to circumvent captcha)

        Args:
            page_num (int): number of pages 
        Returns:
            reviews (list): list of reviews (html elements)
        """
        page = self.session.get(self.url + str(page_num), headers=self.headers)
        page.html.render(sleep=random.random()*3)
        reviews = page.html.xpath('//div[@data-hook="review"]') # get review element by xpath
        if not reviews: return False
        else: return reviews

    
    def parse_single_review(self, r):
        """
        Parses single review to extract review content and metadata

        Args:
            r (html element): single product review
        Returns:
            review (dict): dictionary of review contenta and metadata
        """
        temp_loc_date = r.xpath('//span[@data-hook="review-date"]', first=True).text
        location, date = temp_loc_date.split(" on ")
        other = ''
        try:
            other = r.xpath('//a[@data-hook="format-strip"]', first=True).text
        except AttributeError as e:
            logging.info(e)
        verified = ''
        try:
            verified = r.xpath('//span[@data-hook="avp-badge"]', first=True).text
        except AttributeError as e:
            logging.info(e)
            
        review = {
            'product_id': self.product_id,
            'review_id': r.attrs['id'],
            'name': r.xpath('//span[@class="a-profile-name"]', first=True).text,
            'rating': r.xpath('//i[contains(@data-hook, "review-star-rating") or \
                            contains(@data-hook, "cmps-review-star-rating")]', first=True).text,
            'title': r.xpath('//a[@data-hook="review-title"]', first=True).text,
            'location': location,
            'date': date,
            'other': other,
            'verified': verified,
            'body': r.xpath('//span[@data-hook="review-body"]', first=True).text
        }
        return review
    

    def parse_pages(self, page_num):
        """
        Compile reviews of a specified number of pages into a single json

        Args:
            page_num (int): number of pages of reviews to scrape
        Returns:
            all_reviews (list): json of reviews
        """
        reviews = []
        for i in range(1, page_num + 1):
            page_of_review_elements = self.get_page(i)
            if page_of_review_elements is not False:
                for r in page_of_review_elements:
                    review_json = self.parse_single_review(r)
                    reviews.append(review_json)
                logging.info(f'Scraped page {i}...')
            else:
                logging.info('End of reviews...')
                break
        return reviews
    

    def save_data(self, results):
        """
        Save json data to local file with name [id]-[YYYMMDD]-reviews.json

        Args:
            results (list): json of reviews
        """
        print('Saving data to json...')
        today = datetime.today().strftime('%Y%m%d')
        with open(self.product_id + '-' + today + '-reviews.json', 'w') as f:
            json.dump(results, f)


    def get_product_info(self):
        """
        Scrape product title, brand, and rating + review count
        
        Return:
            brand (str): product brand
            title: product title
            review_count: string describing number of ratings and reviews
        """
        page = self.session.get(self.url + '1', headers=self.headers)
        page.html.render(sleep=random.random()*3)
        title = page.html.xpath('//div[@class="a-row product-title"]', first=True).text
        byline = page.html.xpath('//div[@class="a-row product-by-line"]', first=True).text
        brand = byline.split('by')[1]
        review_count = page.html.xpath('//div[@data-hook="cr-filter-info-review-rating-count"]', first=True).text
        return [brand, title, review_count]


if __name__ == '__main__':
    """
    Test constructor and functions above by scraping first
    2 pages of reviews w/ product id for Nike socks
    """
    product_id = 'B06Y4CJVQY'
    page_num = 2

    amazon_product = Reviews(product_id)
    product_info = amazon_product.get_product_info()
    print(f'Testing review scraper on first {page_num} pages of {product_info[0]}\'s {product_info[1]}...')
    print(f'This product has {product_info[2]}')
    results = amazon_product.parse_pages(page_num)
    amazon_product.save_data(results)
    