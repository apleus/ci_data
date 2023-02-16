from requests_html import HTMLSession
import random
from datetime import datetime
import json
import user_agents


class Reviews:
    def __init__(self, product_id):
        self.product_id = product_id
        self.session = HTMLSession()
        self.headers = {'User-Agent': user_agents.get_ua()}
        self.url = f'https://www.amazon.com/product-reviews/{self.product_id}/ref=cm_cr_arp_d_viewopt_srt?sortBy=recent&pageNumber='


    # return list of review containers from single page of reviews
    def get_page(self, page_num):
        # load page and wait for a few seconds to circumvent captcha...
        page = self.session.get(self.url + str(page_num), headers=self.headers)
        page.html.render(sleep=random.random()*3)
        # find reviews if they exist on page 'page_num'
        reviews = page.html.xpath('//div[@data-hook="review"]')
        if not reviews: return False
        else: return reviews


    # parse review content from single page of reviews
    def parse_page(self, reviews):
        review_content = []
        for r in reviews:
            name = r.xpath('//span[@class="a-profile-name"]', first=True).text
            rating = r.xpath('//i[contains(@data-hook, "review-star-rating") or \
                             contains(@data-hook, "cmps-review-star-rating")]', first=True).text
            title = r.xpath('//a[@data-hook="review-title"]', first=True).text
            temp_loc_date = r.xpath('//span[@data-hook="review-date"]', first=True).text
            location, date = temp_loc_date.split(" on ")
            other = ''
            try:
                other = r.xpath('//a[@data-hook="format-strip"]', first=True).text
            except:
                print("no 'other' data in review...")
            verified = ''
            try:
                verified = r.xpath('//span[@data-hook="avp-badge"]', first=True).text
            except:
                print("no verification")
            body = r.xpath('//span[@data-hook="review-body"]', first=True).text

            review = {
                'product_id': self.product_id,
                'review_id': r.attrs['id'],
                'name': name,
                'rating': rating,
                'title': title,
                'location': location,
                'date': date,
                'other': other,
                'verified': verified,
                'body': body
            }
            review_content.append(review)
        return review_content
    

    # compile reivew content up to page 'page_num'
    def compile_all_pages(self, page_num):
        results = []
        for i in range(1, page_num + 1):
            reviews = self.get_page(i)
            if reviews is not False:
                results.append(self.parse_page(reviews))
                print(f'Scraped page {i}...')
            else:
                print('End of reviews...')
                break
        return results
    

    # save review content to json file locally
    def save_data(self, results):
        print('Saving data to json...')
        today = datetime.today().strftime('%Y%m%d')
        with open(self.product_id + '-' + today + '-reviews.json', 'w') as f:
            json.dump(results, f)


    # scrape product title, brand, and rating + review count
    def get_product_info(self):
        # load page and wait for a few seconds to circumvent captcha...
        page = self.session.get(self.url + '1', headers=self.headers)
        page.html.render(sleep=random.random()*3)
        title = page.html.xpath('//div[@class="a-row product-title"]', first=True).text
        byline = page.html.xpath('//div[@class="a-row product-by-line"]', first=True).text
        brand = byline.split('by')[1]
        review_count = page.html.xpath('//div[@data-hook="cr-filter-info-review-rating-count"]', first=True).text
        return [brand, title, review_count]


if __name__ == '__main__':

    # test values:
    product_id = 'B06Y4CJVQY' # Nike socks
    page_num = 2

    amazon_product = Reviews(product_id)
    product_info = amazon_product.get_product_info()
    print(f'Testing review scraper on first {page_num} pages of {product_info[0]}\'s {product_info[1]}...')
    print(f'This product has {product_info[2]}')
    results = amazon_product.compile_all_pages(page_num)
    amazon_product.save_data(results)
    