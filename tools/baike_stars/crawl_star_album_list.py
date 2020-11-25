import pymongo
import re
import time
from bs4 import BeautifulSoup
from urllib.parse import unquote

from handycrawler.crawler_util import get_content, setup_session, sizeof_fmt


def main():
    """Parse baidu image search engine results to mongodb.

    """
    # configuration
    star_list_path = 'tools/baike_stars/baidu_stars_China_mainland_female_201118.txt'  # noqa: E501
    star_type = 'China Mainland Female'
    # Set up session
    session = setup_session()

    # Set up mongodb
    mongo_client = pymongo.MongoClient('mongodb://localhost:27017/')
    crawl_img_db = mongo_client['baike_stars']
    star_albums_col = crawl_img_db['star_albums']

    num_new_records = 0
    num_exist_records = 0

    with open(star_list_path, 'r') as fin:
        person_list = [line.strip().split('/item')[1] for line in fin]

    for idx, person in enumerate(person_list):
        encoded_name, person_id = person.split('/')[1], person.split('/')[2]
        result = star_albums_col.find_one({'id': person_id})
        if result is None:
            # parse
            person_info = parse_albums(encoded_name, person_id, session)
            print(f'{idx} / {len(person_list)} - {person_info["name"]}, '
                  f'num_album: {person_info["num_album"]}, '
                  f'num_photo: {person_info["num_photo"]}')
            # add to mongodb
            num_new_records += 1
            person_info['type'] = star_type
            insert_rlt = star_albums_col.insert_one(person_info)
            print(f'\tInsert one record: {insert_rlt.inserted_id}')
        else:
            print(f'\t{person_id} already exits.')
            num_exist_records += 1

    print(f'New added records: {num_new_records}.\n'
          f'Existed records: {num_exist_records}.')
    # database statistics
    stat = crawl_img_db.command('dbstats')
    print('Stats:\n'
          f'\tNumber of entries: {stat["objects"]}'
          f'\tSize of database: {sizeof_fmt(stat["dataSize"])}')
    mongo_client.close()


def parse_albums(name, person_id, session, req_timeout=5, max_retry=3):
    """Parse album information for each persom.

    For each person:
        id: '681442'
        name: '白岩松'
        num_album: 11
        num_photo: 82

        lemma_id: '36809'
        sub_lemma_id: '36809'
        new_lemma_id: '681442'

        For each album:
            album_id: '126741'
            album_title: '精彩图册'
            album_num_photo: 4
    """
    name_str = unquote(name, encoding='utf-8')
    url = f'https://baike.baidu.com/pic/{name}/{person_id}'
    referer_url = 'https://baike.baidu.com'

    content = get_content(session, url, referer_url, req_timeout, max_retry)

    soup = BeautifulSoup(content, 'html.parser')

    # get num_album and num_photo
    num_album = int(soup.find('span', {'class': 'album-num num'}).getText())
    num_photo = int(soup.find('span', {'class': 'pic-num num'}).getText())

    # for lemma_id, sub_lemma_id, new_lemma_id
    lemma_id, sub_lemma_id, new_lemma_id = None, None, None
    lemma_id_pattern = re.compile(r"lemmaId: '(\d+)',", re.S)
    sub_lemma_id_pattern = re.compile(r"subLemmaId: '(\d+)',", re.S)
    new_lemma_id_pattern = re.compile(r"newLemmaId: '(\d+)',", re.S)

    album_info_list = []
    # get info for each album
    for element in soup.findAll('div', {'class': 'album-item'}):
        album_info = {}
        # get album title and number of photos
        album_title = element.find('div', {'class': 'album-title'}).getText()
        album_info['album_title'] = album_title
        album_num_photo = int(
            element.find('div', {
                'class': 'album-pic-num'
            }).getText())
        album_info['album_num_photo'] = album_num_photo
        # get album cover href for album_id
        album_cover_href = element.find('a',
                                        {'class': 'pic album-cover'})['href']
        album_cover_url = 'https://baike.baidu.com' + album_cover_href

        album_content = get_content(session, album_cover_url, url, req_timeout,
                                    max_retry)

        soup_album = BeautifulSoup(album_content, 'html.parser')
        # parse album page
        script_rlt = soup_album.findAll('script', {'type': 'text/javascript'})
        for script in script_rlt:
            if script.string is not None:
                script_str = str(script.string)
                album_id_pattern = re.compile(
                    r"albumId = useHash \? albumId : '(\d+)';", re.S)
                album_id = album_id_pattern.findall(script_str)[0]
                album_info['album_id'] = album_id

                if lemma_id is None:
                    lemma_id = lemma_id_pattern.findall(script_str)[0]
                if sub_lemma_id is None:
                    sub_lemma_id = sub_lemma_id_pattern.findall(script_str)[0]
                if new_lemma_id is None:
                    new_lemma_id = new_lemma_id_pattern.findall(script_str)[0]

        album_info_list.append(album_info)

    person_info = dict(
        id=person_id,
        name=name_str,
        num_album=num_album,
        num_photo=num_photo,
        lemma_id=lemma_id,
        sub_lemma_id=sub_lemma_id,
        new_lemma_id=new_lemma_id,
        albums=album_info_list,
        crawl_date=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
    return person_info


if __name__ == '__main__':
    main()
