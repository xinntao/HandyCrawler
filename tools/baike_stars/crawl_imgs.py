import hashlib
import os
import pymongo
import time

from handycrawler.crawler_util import get_img_content, setup_session


def main():
    """Download the image and save it to the corresponding path.

    do not handle images with the same md5, because they may contain different
    person.
    And we will only crop the person belonging to this category.

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
    # configuration
    save_root = 'baike_stars'

    # Set up session
    session = setup_session()

    # Set up mongodb
    mongo_client = pymongo.MongoClient('mongodb://localhost:27017/')
    crawl_img_db = mongo_client['baike_stars']
    img_col = crawl_img_db['all_imgs']

    for idx, document in enumerate(img_col.find()):
        mongo_id = document['_id']
        img_url = document['img_url']
        person_id = document['person_id']
        person_name = document['person_name']
        album_id = document['album_id']

        try_download = document.get('try_download', False)
        download_from = document.get('download_from', 'invalid')
        print(f'{idx}: {person_name} - {img_url.split("/")[-1]}')

        if not try_download or (try_download and download_from == 'invalid'):
            # download
            data, actual_ext = get_img_content(session, img_url)
            if data is not None:
                # save image
                save_folder = os.path.join(save_root, person_id)
                os.makedirs(save_folder, exist_ok=True)
                # calculate md5
                md5hash = hashlib.md5(data).hexdigest()
                filename = f'{album_id}_{md5hash}.{actual_ext}'
                save_path = os.path.join(save_root, person_id, filename)
                with open(save_path, 'wb') as fout:
                    fout.write(data)

                document['type'] = actual_ext
                document['md5'] = md5hash
                document['rel_path'] = save_folder
                document['download_date'] = time.strftime(
                    '%Y-%m-%d %H:%M:%S', time.localtime())
                download_from = 'img_url'
            else:
                download_from = 'invalid'
                if actual_ext is not None:
                    document['type'] = actual_ext
                    print(f'\tinvalid download with extension {actual_ext}')
                else:
                    print('\tinvalid download')

            document['try_download'] = True
            document['download_from'] = download_from

            # update mongodb
            img_col.update_one({'_id': mongo_id}, {'$set': document})
        else:
            print('\tHas downloaded, skip.')


if __name__ == '__main__':
    main()
