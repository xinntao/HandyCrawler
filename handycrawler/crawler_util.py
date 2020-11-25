import imghdr
import requests


def sizeof_fmt(size, suffix='B'):
    """Get human readable file size.
    Args:
        size (int): File size.
        suffix (str): Suffix. Default: 'B'.
    Return:
        str: Formated file siz.
    """
    for unit in ['', 'K', 'M', 'G', 'T', 'P', 'E', 'Z']:
        if abs(size) < 1024.0:
            return f'{size:3.1f} {unit}{suffix}'
        size /= 1024.0
    return f'{size:3.1f} Y{suffix}'


def baidu_decode_url(encrypted_url):
    """Decrypt baidu ecrypted url."""
    url = encrypted_url
    map1 = {'_z2C$q': ':', '_z&e3B': '.', 'AzdH3F': '/'}
    map2 = {
        'w': 'a', 'k': 'b', 'v': 'c', '1': 'd', 'j': 'e',
        'u': 'f', '2': 'g', 'i': 'h', 't': 'i', '3': 'j',
        'h': 'k', 's': 'l', '4': 'm', 'g': 'n', '5': 'o',
        'r': 'p', 'q': 'q', '6': 'r', 'f': 's', 'p': 't',
        '7': 'u', 'e': 'v', 'o': 'w', '8': '1', 'd': '2',
        'n': '3', '9': '4', 'c': '5', 'm': '6', '0': '7',
        'b': '8', 'l': '9', 'a': '0'
    }  # yapf: disable
    for (ciphertext, plaintext) in map1.items():
        url = url.replace(ciphertext, plaintext)
    char_list = [char for char in url]
    for i in range(len(char_list)):
        if char_list[i] in map2:
            char_list[i] = map2[char_list[i]]
    url = ''.join(char_list)
    return url


def setup_session():
    headers = {
        'User-Agent': ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_3)'
                       ' AppleWebKit/537.36 (KHTML, like Gecko) '
                       'Chrome/48.0.2564.116 Safari/537.36')
    }
    session = requests.Session()
    session.headers.update(headers)
    return session


def get_content(session, url, referer_url, req_timeout=5, max_retry=3):
    retry = max_retry
    while retry > 0:
        try:
            response = session.get(
                url, timeout=req_timeout, headers={'Referer': referer_url})
        except Exception as e:
            print(f'Exception caught when fetching page {url}, '
                  f'error: {e}, remaining retry times: {retry - 1}')
        else:
            content = response.content.decode('utf-8',
                                              'ignore').replace("\\'", "'")
            break
        finally:
            retry -= 1
    return content


def get_img_content(session,
                    file_url,
                    extension=None,
                    max_retry=3,
                    req_timeout=5):
    """
    Returns:
        (data, actual_ext)
    """
    retry = max_retry
    while retry > 0:
        try:
            response = session.get(file_url, timeout=req_timeout)
        except Exception as e:
            print(f'Exception caught when downloading file {file_url}, '
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
            # do not download original gif
            if actual_ext == 'gif' or actual_ext is None:
                return None, actual_ext

            return data, actual_ext
        finally:
            retry -= 1

    return None, None
