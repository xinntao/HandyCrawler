import json
import time
from datetime import datetime
from urllib.parse import urlsplit

from basicsr.utils import sizeof_fmt
from basicsr.utils.crawler_util import baidu_decode_url, setup_session

try:
    import pymongo
except Exception:
    raise ImportError('Please install pymongo')


def main():
    """Parse baidu image search engine results to mongodb.

    The keys in mongodb:
    # for parsing
    img_url
    obj_url
    hover_url
    from_url
    width
    height
    type
    bd_news_date
    page_title
    record_date
    # for download
    try_download
    download_from
    download_date
    md5
    rel_path
    """
    # configuration
    keyword = '老照片'
    offset = 0
    max_num = 2000  # Baidu only returns the first 1000 results
    req_timeout = 5
    interval = 30
    max_retry = 3

    # Set up session
    session = setup_session()

    # Set up mongodb
    mongo_client = pymongo.MongoClient('mongodb://localhost:27017/')
    crawl_img_db = mongo_client['crawl_img']
    old_photo_col = crawl_img_db['old_photo']

    base_url = ('http://image.baidu.com/search/acjson?tn=resultjson_com'
                '&ipn=rj&word={}&pn={}&rn={}')
    num_new_records = 0
    num_exist_records = 0

    for i in range(offset, offset + max_num, interval):
        url = base_url.format(keyword, i, interval)
        # do not support filter (type, color, size) now
        # if filter_str:
        #     url += '&' + filter_str

        # fetch and parse the page
        retry = max_retry
        while retry > 0:
            try:
                split_rlt = urlsplit(url)
                referer_url = f'{split_rlt.scheme}://{split_rlt.netloc}'
                response = session.get(
                    url, timeout=req_timeout, headers={'Referer': referer_url})
            except Exception as e:
                print(f'Exception caught when fetching page {url}, '
                      f'error: {e}, remaining retry times: {retry - 1}')
            else:
                print(f'parsing result page {url}')
                records = parse(response)
                # add to mongodb
                for record in records:
                    result = old_photo_col.find_one(
                        {'img_url': record['img_url']})
                    if result is None:
                        num_new_records += 1
                        insert_rlt = old_photo_col.insert_one(record)
                        print(f'\tInsert one record: {insert_rlt.inserted_id}')
                    else:
                        num_exist_records += 1
                break
            finally:
                retry -= 1
    print(f'New added records: {num_new_records}.\n'
          f'Existed records: {num_exist_records}.')
    # database statistics
    stat = crawl_img_db.command('dbstats')
    print('Stats:\n'
          f'\tNumber of entries: {stat["objects"]}'
          f'\tSize of database: {sizeof_fmt(stat["dataSize"])}')
    mongo_client.close()


def parse(response):
    """Parse response of baidu returned search results."""
    try:
        content = response.content.decode('utf-8',
                                          'ignore').replace("\\'", "'")
        content = json.loads(content, strict=False)
    except Exception as e:
        print(f'Error in parse response: {e}')
        return

    records = []
    for item in content['data']:
        record = dict()
        # obj url (required key)
        if 'objURL' in item:
            obj_url = baidu_decode_url(item['objURL'])
        else:
            obj_url = ''
        record['obj_url'] = obj_url
        # hover url (required key)
        if 'hoverURL' in item:
            hover_url = item['hoverURL']
        else:
            hover_url = ''
        record['hover_url'] = hover_url
        # obj url and hover url, must have one
        if record['obj_url'] == '' and record['hover_url'] == '':
            print('\tNo obj_url and hover_url found.')
            continue
        # img url serves as the identification key. First use obj url.
        if record['obj_url'] == '':
            record['img_url'] = record['hover_url']
        else:
            record['img_url'] = record['obj_url']

        if 'fromURL' in item:
            record['from_url'] = baidu_decode_url(item['fromURL'])
        if 'width' in item:
            record['width'] = item['width']
        if 'height' in item:
            record['height'] = item['height']
        if 'type' in item:
            record['type'] = item['type']
        if 'bdImgnewsDate' in item:
            bd_news_date = datetime.strptime(item['bdImgnewsDate'],
                                             '%Y-%m-%d %H:%M')
            record['bd_news_date'] = time.strftime('%Y-%m-%d %H:%M:%S',
                                                   bd_news_date.timetuple())
        if 'fromPageTitleEnc' in item:
            record['page_title'] = item['fromPageTitleEnc']
        elif 'fromPageTitle' in item:
            record['page_title'] = item['fromPageTitle']

        record['record_date'] = time.strftime('%Y-%m-%d %H:%M:%S',
                                              time.localtime())
        records.append(record)
    return records


if __name__ == '__main__':
    main()
