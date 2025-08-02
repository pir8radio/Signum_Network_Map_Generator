import subprocess
import sys
import logging
import random
import socket
import zipfile
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse
from time import sleep
import requests
from pykml.factory import KML_ElementMaker as KML
from lxml import etree

def install_packages():
    required_packages = ["requests", "pykml", "lxml"]
    for package in required_packages:
        try:
            __import__(package)
        except ImportError:
            subprocess.check_call([sys.executable, "-m", "pip", "install", package])

install_packages()

PEERS_SCAN_DELAY = 0
DEFAULT_P2P_PORT = 8123
MIN_PEER_VERSION = "1.0.0"
BRS_BOOTSTRAP_PEERS = ["us-east.signum.network", "europe.signum.network", "europe1.signum.network"]
BRS_P2P_VERSION = "3.8.4"
CUSTOM_PIN_URL = "https://signum.network/assets/img/wallet/Signum_Logomark_blue_filled.svg"

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

if PEERS_SCAN_DELAY > 0:
    logger.info(f"Peers sleeping for {PEERS_SCAN_DELAY} seconds...")
    sleep(PEERS_SCAN_DELAY)

def get_ip_by_domain(peer: str) -> str or None:
    if not peer.startswith("http"):
        peer = f"http://{peer}"
    hostname = urlparse(peer).hostname
    if not hostname:
        return None
    if ":" in hostname:
        return hostname
    try:
        return socket.gethostbyname(hostname)
    except socket.gaierror as e:
        logger.debug("Can't resolve host: %s - %r", peer, e)
        return None

def get_geo_info(ip: str) -> dict:
    try:
        response = requests.get(f"https://ipwho.is/{ip}")
        response.raise_for_status()
        json_response = response.json()
        geo_info = {
            "country_code": json_response["country_code"] or "??",
            "latitude": json_response["latitude"],
            "longitude": json_response["longitude"]
        }
        logger.info("Geo lookup, found peer from: %s", geo_info)
        return geo_info
    except (requests.RequestException, ValueError, KeyError):
        logger.warning("Geo lookup ERROR!")
        return {"country_code": "??", "latitude": None, "longitude": None}

def is_good_version(version: str) -> bool:
    if not version:
        return False
    if version[0] == "v":
        version = version[1:]
    try:
        from distutils.version import LooseVersion
        return LooseVersion(version) >= LooseVersion(MIN_PEER_VERSION)
    except TypeError:
        return False

class P2PApi:
    headers = {
        "User-Agent": f"BRS/{BRS_P2P_VERSION}",
        "Content-Type": "application/json"
    }
    _default_port = DEFAULT_P2P_PORT

    def __init__(self, node_address: str) -> None:
        if not node_address.startswith("http"):
            node_address = f"http://{node_address}"
        parsed_url = urlparse(node_address)
        if not parsed_url.port:
            node_address = f"{node_address}:{self._default_port}"
        self.node_url = node_address
        self._session = requests.session()

    def _close_session(self) -> None:
        if self._session:
            self._session.close()

    def __del__(self) -> None:
        self._close_session()

    def _request(self, endpoint: str, params: dict = None, method: str = "POST") -> dict:
        url = f"{self.node_url}/burst"
        logger.info(f"Making request to {url} with method {method} and params {params}")
        body = {
            "requestType": endpoint,
            "protocol": "B1"
        }
        try:
            response = self._session.request(
                method,
                url,
                headers=self.headers,
                json=body if method == "POST" else None,
                params=params if method == "GET" else None,
                timeout=10,
                verify=False,
            )
            response.raise_for_status()
        except requests.RequestException as e:
            logger.error(f"Network error: {e}")
            return {}
        try:
            json_response = response.json()
            logger.info(f"Response from {url}: {json_response}")
        except ValueError as e:
            logger.error(f"Malformed JSON: {e}")
            return {}
        return json_response

    def get_peers(self) -> list:
        response = self._request("getPeers")
        if "peers" in response:
            return response["peers"]
        else:
            logger.error(f"Unexpected response structure: {response}")
            return []

    def get_version(self) -> str:
        response = self._request("getInfo")
        if "version" in response:
            return response["version"]
        elif "application" in response and "version" in response["application"]:
            return response["application"]["version"]
        else:
            logger.error(f"No version found in getInfo response: {response}")
            return "Unknown"

def explore_peer(address: str, updates: dict, peers_to_explore: set, checked_addresses: set):
    logger.debug("Peer: %s", address)
    if address in updates or address in checked_addresses:
        return
    checked_addresses.add(address)
    try:
        p2p_api = P2PApi(address)
        peers = p2p_api.get_peers()
        version = p2p_api.get_version()
        peers_to_explore.update(peers)
    except Exception as ex:
        logger.debug("Can't connect to peer: %s", address)
        updates[address] = None
        return
    ip = get_ip_by_domain(address)
    geo_info = get_geo_info(ip) if ip else {"country_code": "??", "latitude": None, "longitude": None}
    updates[address] = {
        "announced_address": address,
        "real_ip": ip,
        "country_code": geo_info["country_code"],
        "latitude": geo_info["latitude"],
        "longitude": geo_info["longitude"],
        "version": version,
        "peers": peers,
    }
    print(f"Scanned peer: {address}")

def explore_node(address: str, updates: dict, peers_to_explore: set, checked_addresses: set):
    logger.debug("Node: %s", address)
    print(f"Scanning node: {address}")
    if address in checked_addresses:
        return
    checked_addresses.add(address)
    try:
        p2p_api = P2PApi(address)
        peers = p2p_api.get_peers()
        version = p2p_api.get_version()
        peers_to_explore.update(peers)
        explore_peer(address, updates, peers_to_explore, checked_addresses)
    except Exception as ex:
        logger.error(f"Error exploring node {address}: {ex}")
        return
    with ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(lambda p: explore_peer(p, updates, peers_to_explore, checked_addresses), peers)

def get_nodes_list() -> list:
    addresses_other = BRS_BOOTSTRAP_PEERS
    random.shuffle(addresses_other)
    return addresses_other

def build_network_chart(updates: dict):
    def kml_color(hex_color):
        rr = hex_color[0:2]
        gg = hex_color[2:4]
        bb = hex_color[4:6]
        return f"ff{bb}{gg}{rr}"

    def random_color():
        return "%06x" % random.randint(0, 0xFFFFFF)

    doc = KML.kml(
        KML.Document(
            KML.name("P2P Network Map"),
            KML.open(1),
        )
    )

    node_colors = {}
    style_ids = {}

    for i, node in enumerate(updates):
        color = random_color()
        node_colors[node] = color
        style_id = f"nodeStyle{i}"
        style_ids[node] = style_id
        doc.Document.append(
            KML.Style(
                KML.IconStyle(
                    KML.Icon(
                        KML.href(CUSTOM_PIN_URL)
                    ),
                ),
                KML.LabelStyle(
                    KML.color(kml_color(color)),
                ),
                KML.LineStyle(
                    KML.color(kml_color(color)),
                    KML.width(2)
                ),
                id=style_id
            )
        )

    node_placemarks = {}
    for node, data in updates.items():
        if data and data["latitude"] and data["longitude"]:
            placemark = KML.Placemark(
                KML.name(data["announced_address"]),
                KML.styleUrl(f"#{style_ids[node]}"),
                KML.Point(
                    KML.coordinates(f'{data["longitude"]},{data["latitude"]},0')
                )
            )
            doc.Document.append(placemark)
            node_placemarks[node] = placemark

            peers_count = len(data["peers"]) if "peers" in data and data["peers"] else 0
            node_version = data.get("version", "Unknown")
            bubble_lat = data["latitude"]
            bubble_lon = data["longitude"]
            bubble_alt = 10000  # Raise label above ground

            bubble_name = f"Node: {data['announced_address']}"
            bubble_desc = (
                f"Version: {node_version}<br/>"
                f"Peers List: {peers_count}"
            )

            doc.Document.append(
                KML.Placemark(
                    KML.name(bubble_name),
                    KML.description(bubble_desc),
                    KML.styleUrl(f"#{style_ids[node]}"),
                    KML.Point(
                        KML.coordinates(f'{bubble_lon},{bubble_lat},{bubble_alt}')
                    )
                )
            )

    for node, data in updates.items():
        if data:
            for peer in data["peers"]:
                if peer in updates and peer != node:
                    peer_data = updates[peer]
                    if peer_data["latitude"] and peer_data["longitude"]:
                        line = KML.Placemark(
                            KML.styleUrl(f"#{style_ids[node]}"),
                            KML.LineString(
                                KML.coordinates(
                                    f'{data["longitude"]},{data["latitude"]},0 {peer_data["longitude"]},{peer_data["latitude"]},0'
                                )
                            )
                        )
                        doc.Document.append(line)

    with open("network.kml", "w") as f:
        f.write(etree.tostring(doc, pretty_print=True).decode('utf-8'))

    with zipfile.ZipFile("network.kmz", "w", zipfile.ZIP_DEFLATED) as kmz:
        kmz.write("network.kml")

def main():
    logger.info("Start the scan")
    addresses = get_nodes_list()
    updates = {}
    peers_to_explore = set()
    checked_addresses = set()
    with ThreadPoolExecutor(max_workers=20) as executor:
        executor.map(lambda address: explore_node(address, updates, peers_to_explore, checked_addresses), addresses)
    while peers_to_explore:
        new_peers = peers_to_explore.copy()
        peers_to_explore.clear()
        with ThreadPoolExecutor(max_workers=20) as executor:
            executor.map(lambda address: explore_node(address, updates, peers_to_explore, checked_addresses), new_peers)
    build_network_chart(updates)
    logger.info("Done")

if __name__ == "__main__":
    main()
