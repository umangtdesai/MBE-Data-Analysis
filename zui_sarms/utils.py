import json
import logging
import os

import requests

log = logging.getLogger(__name__)


def load_config(path):
    with open(path, "r") as f:
        r = json.loads(f.read())
    return r


def download_file(url):
    """
    Download file and return the raw content

    :param url:
    :return:
    """

    rd = RequestsDownloader(raw=True)

    return rd.download(url=url)


def ensure_path_exists(path):
    """
    Make the path if necessary

    :param path:
    """
    dirpath = os.path.abspath(os.path.dirname(path))
    if not os.path.exists(dirpath):
        log.info("Path %s not exists. Making it ourselves!", dirpath)
        os.makedirs(dirpath)
    return path


class BaseDownloader:
    def __init__(self, raw=False):
        self.raw = raw

    def download(self, url, filename=None):
        """
        Download an url
        :param buf: [Optional] Download to a buffer instead
        :param url:
        :param filename:
        :return:
        """

        if not filename:
            filename = self._make_filename(url)
        path = self._make_path(filename)

        self._download(url, path)

        log.info("Downloaded: %s -> %s", url, path)
        return path

    def _download(self, url, path):
        raise NotImplementedError

    @staticmethod
    def _make_path(filename):
        """
        Generate the local system path from filename

        :param filename:
        :return:
        """
        path = os.path.join("/home/zui/kode/gwrdata", filename)
        path = ensure_path_exists(path)
        log.debug("Making path: %s", path)
        return path

    @staticmethod
    def _make_filename(url):
        """
        Make filename from URL

        :param str url:
        :return:
        """
        # This is super naive.
        # Todo: Make filename when the crawler return per site
        # Todo: Make random filename if needed
        filename = url.split("/")[-1]
        log.debug("Making filename: %s -> %s", url, filename)
        return filename


class RequestsDownloader(BaseDownloader):

    def _download(self, url, path):
        log.debug("Downloading %s", url)
        r = requests.get(url)
        if r.ok:
            buf = r.content
        else:
            raise requests.RequestException("Error while downloading file {}".format(url))

        if self.raw:
            return buf

        with open(path, "wb") as f:
            f.write(buf)

        return path


def parse_coor(s):
    """
    Parse the string to tuple of coordinate
    In the format of (lat, long)
    """

    lat, long = s.split(", ")
    lat = lat[1:]
    long = long[:-1]
    lat = float(lat)
    long = float(long)

    return [lat, long]


def geocoding(address):
    """
    Take in an address and return the proper address and the coordinate tuple
    """

    r = requests.get(f"https://maps.googleapis.com/maps/api/geocode/json", params={
        "address": address,
        "key": AUTH["GMAP_API"]
    })

    if r.status_code == 200:
        r = r.json()
        results = r["results"]
        if len(results) < 1:
            log.error("No result geocoding for %s", address)
            return (-1, -1)

        result = results[0]
        proper_address = result["formatted_address"]
        loc = result["geometry"]["location"]
        lat = loc["lat"]
        lng = loc["lng"]

        return (proper_address, lat, lng)

    else:
        log.error("Error in Geocoding %s", address)
        return (-1, -1)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    # Quick test for RequestsDownloader
    rd = RequestsDownloader()
    log.info("Starting download")
