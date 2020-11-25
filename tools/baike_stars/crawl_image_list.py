import json
import pymongo
import time

from handycrawler.crawler_util import get_content, setup_session, sizeof_fmt


def main():
    """Parse baidu image search engine results to mongodb.


    img_url: image url in Baidu cdn
    person_id:
    person_name:
    album_id:
    width: image width
    height: image height
    pic_id: picId in baidu html
    record_date: crawl record data

    # determine during download
    type: image type (typically, jpg or png)
    try_download
    download_date
    download_from: img_url or invalid
    md5
    rel_path:


    save path: person_id/md5.extension


    """
    verbose = False
    # Set up session
    session = setup_session()

    # Set up mongodb
    mongo_client = pymongo.MongoClient('mongodb://localhost:27017/')
    crawl_img_db = mongo_client['baike_stars']
    star_albums_col = crawl_img_db['star_albums']
    img_col = crawl_img_db['all_imgs']

    num_new_records = 0
    num_exist_records = 0

    for document in star_albums_col.find():
        mongo_id = document['_id']
        person_id = document['id']
        person_name = document['name']
        lemma_id = document['lemma_id']
        new_lemma_id = document['new_lemma_id']
        sub_lemma_id = document['sub_lemma_id']
        albums = document['albums']
        has_parsed_imgs = document.get('has_parsed_imgs', False)
        if has_parsed_imgs:
            print(f'{person_name} has parsed, skip.')
        else:
            for idx, album in enumerate(albums):
                album_id = album['album_id']
                album_title = album['album_title']
                album_num_photo = album['album_num_photo']
                print(f'Parse {person_name} - [{idx}/{len(albums)}] '
                      f'{album_title}, {album_num_photo} photos...')

                # parse images
                img_list = parse_imgs(lemma_id, new_lemma_id, sub_lemma_id,
                                      album_id, album_num_photo, session)
                # add to mongodb
                if len(img_list) != album_num_photo:
                    print(f'WARNING: the number of image {len(img_list)} is '
                          f'different from album_num_photo {album_num_photo}.')
                for img_info in img_list:
                    src = img_info['src']
                    width = img_info['width']
                    height = img_info['height']
                    pic_id = img_info['pic_id']

                    img_url = f'https://bkimg.cdn.bcebos.com/pic/{src}'
                    result = img_col.find_one({'img_url': img_url})
                    if result is None:
                        num_new_records += 1
                        record = dict(
                            img_url=img_url,
                            person_id=person_id,
                            person_name=person_name,
                            album_id=album_id,
                            album_title=album_title,
                            width=width,
                            height=height,
                            pic_id=pic_id,
                            record_data=time.strftime('%Y-%m-%d %H:%M:%S',
                                                      time.localtime()))
                        insert_rlt = img_col.insert_one(record)
                        if verbose:
                            print('\tInsert one record: '
                                  f'{insert_rlt.inserted_id}')
                    else:
                        num_exist_records += 1
            # add has_parsed_imgs label
            star_albums_col.update_one({'_id': mongo_id},
                                       {'$set': dict(has_parsed_imgs=True)})

    print(f'New added records: {num_new_records}.\n'
          f'Existed records: {num_exist_records}.')
    # database statistics
    stat = crawl_img_db.command('dbstats')
    print('Stats:\n'
          f'\tNumber of entries: {stat["objects"]}'
          f'\tSize of database: {sizeof_fmt(stat["dataSize"])}')
    mongo_client.close()


def parse_imgs(lemma_id,
               new_lemma_id,
               sub_lemma_id,
               album_id,
               album_num_photo,
               session,
               req_timeout=5,
               max_retry=3):
    referer_url = 'https://baike.baidu.com'
    interval = 50
    img_list = []

    for offset in range(0, album_num_photo, interval):
        url = (f'https://baike.baidu.com/ajax/getPhotos?lemmaid={lemma_id}'
               f'&id={new_lemma_id}&sublemmaid={sub_lemma_id}&aid={album_id}'
               f'&pn={offset}&rn={interval}')

        content = get_content(session, url, referer_url, req_timeout,
                              max_retry)
        content = json.loads(content, strict=False)

        for item in content['data']['list']:
            src = item['src']
            width = item['width']
            height = item['height']
            pic_id = item['picId']
            img_list.append(
                dict(src=src, width=width, height=height, pic_id=pic_id))
    return img_list


if __name__ == '__main__':
    main()
