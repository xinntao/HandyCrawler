import time
from bs4 import BeautifulSoup
from selenium import webdriver
from urllib.parse import unquote


def get_name_relpath_from_html(html):
    soup = BeautifulSoup(html, 'html.parser')
    results = []
    for tr in soup.findAll('tr', {'class': ''}):  # each for a celebrity
        if tr.find('a') is not None:
            relpath = tr.find('a')['href']
            # get celebrity name
            encoded_name = relpath.split('/')[2]
            name = unquote(encoded_name, encoding='utf-8')
            results.append(dict(name=name, relpath=relpath))
    return results


def main():
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--disable-gpu')
    # chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument("user-agent='Mozilla/5.0 (X11; Linux x86_64) "
                                'AppleWebKit/537.36 (KHTML, like Gecko) '
                                "Chrome/87.0.4280.66 Safari/537.36'")
    client = webdriver.Chrome(options=chrome_options)  # executable_path

    client.get('https://baike.baidu.com/starrank')
    idx_total = 0

    # configs:
    """
    <li class="tab-item" data-rank="11">中国内地男明星榜</li>
    <li class="tab-item" data-rank="10">中国内地女明星榜</li>
    <li class="tab-item" data-rank="1">港台东南亚男明星榜</li>
    <li class="tab-item" data-rank="2">港台东南亚女明星榜</li>
    <li class="tab-item" data-rank="3">韩国男明星榜</li>
    <li class="tab-item" data-rank="4">韩国女明星榜</li>
    <li class="tab-item" data-rank="9">日本男明星榜</li>
    <li class="tab-item" data-rank="8">日本女明星榜</li>
    <li class="tab-item" data-rank="7">欧美男明星榜</li>
    <li class="tab-item" data-rank="6">欧美女明星榜</li>
    <li class="tab-item" data-rank="5">全球组合类明星榜</li>
    """

    f = open('baidu_stars_China_mainland_female.txt', 'w')
    # click other pages
    new_page = client.find_element_by_xpath('//li[@data-rank="10"]')
    new_page.click()
    time.sleep(1)

    idx_page = 1
    next_page = 'Not None'
    while next_page is not None:
        print(f'#### Process Page {idx_page} ...')
        results = get_name_relpath_from_html(client.page_source)
        for result in results:
            name, relpath = result['name'], result['relpath']
            print(f'{idx_total + 1} \t{name}\t{relpath}')
            f.write(f'{name} {relpath}\n')
            idx_total += 1

        # click next page
        next_page = client.find_element_by_class_name('next')
        try:
            next_page.click()
        except Exception as error:
            print(f'Cannot click next page with error: {error}')
            next_page = None
        else:
            time.sleep(1)  # wait to load the new page
            idx_page += 1

    f.close()
    client.quit()


if __name__ == '__main__':
    main()
