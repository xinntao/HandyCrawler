import hashlib
import imghdr
import os
import time

from handycrawler.crawler_util import setup_session

try:
    import pymongo
except Exception:
    raise ImportError('Please install pymongo')


def main():
    """Download the image and save it to the corresponding path."""
    # configuration
    save_root = 'old_photo'

    # Set up session
    session = setup_session()

    # Set up mongodb
    mongo_client = pymongo.MongoClient('mongodb://localhost:27017/')
    crawl_img_db = mongo_client['crawl_img']
    old_photo_col = crawl_img_db['old_photo']

    for idx, document in enumerate(old_photo_col.find()):
        mongo_id = document['_id']
        obj_url = document['obj_url']
        hover_url = document['hover_url']
        extension = document.get('type', 'None')
        page_title = document.get('page_title', 'None')
        try_download = document.get('try_download', False)
        download_from = document.get('download_from', 'invalid')
        print(f'{idx}: {page_title}')

        if not try_download or (try_download and download_from == 'invalid'):
            # download obj_url
            print(f'\tDownload obj_url: {obj_url}')
            md5hash, actual_ext = download(session, obj_url, save_root,
                                           extension)
            download_from = 'obj_url' if md5hash is not None else 'invalid'
            if md5hash is not None:
                result = old_photo_col.find({'md5': md5hash})
                if result is not None:
                    print(document)
                    print('collision', result.count())
                    for x in result:
                        print(x)
                input('Please handle')
                md5hash = None  # use hover_url
            if md5hash is None:  # download hover_url
                print(f'\tDownload hover_url: {hover_url}')
                md5hash, actual_ext = download(session, hover_url, save_root,
                                               extension)
                download_from = 'hover_url' if (md5hash
                                                is not None) else 'invalid'
            # update mongodb record
            if actual_ext != extension:
                document['type'] = actual_ext
                print(f'\tChange ext from {extension} to {actual_ext}')
            document['try_download'] = True
            document['download_from'] = download_from
            if md5hash is not None:
                document['download_date'] = time.strftime(
                    '%Y-%m-%d %H:%M:%S', time.localtime())
                document['md5'] = md5hash
                document['rel_path'] = save_root
            print(f'\tUpdate mongodb: {download_from}')
            old_photo_col.update_one({'_id': mongo_id}, {'$set': document})
        else:
            print('\tHas downloaded, skip.')


def download(session,
             file_url,
             save_root,
             extension,
             max_retry=3,
             timeout=5,
             default_ext='png'):
    retry = max_retry
    while retry > 0:
        try:
            response = session.get(file_url, timeout=timeout)
        except Exception as e:
            print(f'\tException caught when downloading file {file_url}, '
                  f'error: {e}, remaining retry times: {retry - 1}')
        else:
            if response.status_code != 200:
                print(f'Response status code {response.status_code}, '
                      f'file {file_url}')
                break

            # get the response byte
            data = response.content
            if isinstance(data, str):
                print('Converting str to byte, later remove it.')
                data = data.encode(data)
            actual_ext = imghdr.what(extension, data)
            actual_ext = 'jpg' if actual_ext == 'jpeg' else actual_ext
            # do not download original gif, use hover url
            if actual_ext == 'gif' or actual_ext is None:
                return None, actual_ext
            # save image
            md5hash = write(save_root, data, actual_ext)
            return md5hash, actual_ext
        finally:
            retry -= 1
    return None, None


def write(save_root, data, extension):
    os.makedirs(save_root, exist_ok=True)
    # calculate md5
    md5hash = hashlib.md5(data).hexdigest()
    filename = f'{md5hash}.{extension}'
    save_path = os.path.join(save_root, filename)
    with open(save_path, 'wb') as fout:
        fout.write(data)
    return md5hash


if __name__ == '__main__':
    main()
