import asyncio
import re
from pathlib import Path
from typing import Iterator, List, Tuple

import aiofile
import aiohttp
import requests
from alive_progress import alive_it
from bs4 import BeautifulSoup

from config import get_config


async def download_files(files_list, destination, website_name, page=0):
    sema = asyncio.BoundedSemaphore(5)

    async def download_file(semaphore, session, url, name):
        fname = f"{website_name}-Page{page}-ReplayN{name}.SC2Replay"
        async with semaphore:
            async with session.get(url) as resp:
                assert resp.status == 200
                data = await resp.read()

        async with aiofile.async_open(destination / fname, "wb") as outfile:
            await outfile.write(data)

    async with aiohttp.ClientSession() as session:
        tasks = [
            download_file(sema, session, url, file_name)
            for (file_name, url) in files_list
        ]
        await asyncio.gather(*tasks)


class ReplayDownloader:
    def __init__(
        self,
        destination_path,
        config_path,
        max_count: int = -1,
        jupyter=None,
    ) -> None:
        self.config = get_config(config_path)
        self.max_count = max_count if max_count > 0 else 10e5
        self.change_destination(destination_path)
        self.success_urls = []
        self.failed_urls = []
        self.jupyter = jupyter

    def start_download(
        self,
        website_name,
        game_matchup=None,
        game_min_length=0,
        game_max_length=3600,
        league=None,
        is_ladder=None,
    ):
        # Check website availability
        try:
            self.url = self.config[website_name]["url"]
        except KeyError:
            raise KeyError(f'Unknown website name: "{website_name}"')
        self.website_name = website_name
        r = requests.head(self.url)
        assert r.status_code == 200

        init_soup = self._get_parsed_site(
            game_matchup=game_matchup,
            game_min_length=game_min_length if game_min_length > 0 else None,
            game_max_length=game_max_length if game_max_length > 0 else None,
            league=league,
            is_ladder=is_ladder,
        )
        page_start = self.config[self.website_name]["keys"]["page_start"]
        max_page = self._get_max_pages(init_soup)
        if self.jupyter in (True, False):
            bar = alive_it(range(page_start, max_page), force_tty=self.jupyter)
        else:
            bar = alive_it(range(page_start, max_page))
        file_count = 0
        for page in bar:
            bar.text(f"Processing page{page} of {max_page-1}")
            init_soup = self._get_parsed_site(
                page=page,
                game_matchup=game_matchup,
                game_min_length=game_min_length,
                game_max_length=game_max_length,
                league=league,
                is_ladder=is_ladder,
            )
            files_list = [
                (game_name, url)
                for (game_name, url, game_len) in self._yield_link_and_length(init_soup)
                if game_len is not None
                if game_len > game_min_length
                if game_len < game_max_length
            ]
            page_count = len(files_list)
            diff_count = self.max_count - file_count
            if diff_count < page_count:
                files_list = files_list[: diff_count - 1]

            asyncio.run(
                download_files(files_list, self.destination, self.website_name, page)
            )
            page += self.config[self.website_name]["keys"]["page_increment"]
            file_count += page_count
            if diff_count < page_count:
                break

    def _get_max_pages(self, soup) -> int:
        if self.website_name == "spawningtool":
            for row in soup.find_all("h3"):
                if hasattr(row, "string"):
                    result = re.search(r"\d+ of (\d+)", row.string)
                    if result:
                        return int(result.group(1))
        if self.website_name == "sc2rep":
            navbar = soup.find("div", class_="navigation-central-div")
            curr_max_page = 0
            for row in navbar.find_all("a"):
                result = re.search(r"page=(\d+)", row["href"])
                if result:
                    curr_max_page = max(curr_max_page, int(result.group(1)))
            page_increment = int(
                self.config[self.website_name]["keys"]["page_increment"]
            )
            return curr_max_page // page_increment
        else:
            return 0

    def change_destination(self, new_destination):
        new_destination = Path(new_destination)
        new_destination.mkdir(exist_ok=True)
        assert new_destination.is_dir()
        self.destination = new_destination

    def _get_parsed_site(
        self,
        page=1,
        game_matchup=None,
        game_min_length=None,
        game_max_length=None,
        league=None,
        is_ladder=None,
    ):
        params = {k: v for (k, v) in self.config[self.website_name]["values"].items()}
        website_keys = self.config[self.website_name]["keys"]

        page_inc = self.config[self.website_name]["keys"]["page_increment"]
        page_fixed = page * page_inc
        to_send = {
            "page": page_fixed,
            "matchup": game_matchup,
            "min_length": game_min_length,
            "max_length": game_max_length,
            "league": int(league) if league is not None else None,
            "is_ladder": int(is_ladder) if is_ladder is not None else None,
        }
        for k, v in to_send.items():
            try:
                params[website_keys[k]] = v
            except KeyError:
                pass
        headers = {"User-Agent": self.config["headers"]["user_agent"]}
        search_url = self.url + self.config[self.website_name]["header"]
        r = requests.get(search_url, params=params, headers=headers)
        if r.status_code != 200:
            raise ConnectionError(f"Bad server response: code {r.status_code}")
        return BeautifulSoup(r.content, "html5lib")

    def _yield_link_and_length(
        self, soup: BeautifulSoup
    ) -> Iterator[Tuple[str, str, int]]:
        def parse_data(game_id, ref_link, game_len_str) -> Tuple[str, str, int]:
            link = self.url + ref_link
            game_len = 0
            if game_len_str == "":
                game_len = 0
            else:
                h, m = game_len_str.split(":")
                game_len += 60 * int(h)
                game_len += int(m)
            return game_id, link, game_len

        if self.website_name == "spawningtool":
            for game_id, link, game_len_str in self.spawningtool_yield(soup):
                yield parse_data(game_id, link, game_len_str)
        if self.website_name == "sc2rep":
            for game_id, link, game_len_str in self.sc2rep_yield(soup):
                yield parse_data(game_id, link, game_len_str)

    def spawningtool_yield(self, soup: BeautifulSoup):
        soup = soup.find("table", class_="table table-striped")
        for row in soup.find_all("tr"):
            ref_link = ""
            game_len = ""
            for i, column in enumerate(row.find_all("td")):
                if i > 1:
                    if column.string is not None and ":" in column.string:
                        game_len = column.string
                    if column.a is not None and "down" in column.a["href"]:
                        ref_link = column.a["href"]
                        break
            game_id = ref_link.strip("/").split("/")[0]
            yield (game_id, ref_link, game_len)

    def sc2rep_yield(self, soup: BeautifulSoup):
        soup = soup.find_all(
            "table", attrs={"width": "95%", "cellspacing": "2", "cellpadding": "2"}
        )[1]
        for row in soup.find_all("tr", class_="trgreen"):
            ref_link = ""
            game_len = ""
            for i, column in enumerate(row.find_all("td")):
                if i > 4:
                    if column.string is not None and ":" in column.string:
                        game_len = column.string
                    if column.a is not None and "down" in column.a["href"]:
                        ref_link = column.a["href"]
                        break

            game_id = ref_link.split("=")[-1]
            yield (game_id, ref_link, game_len)


if __name__ == "__main__":
    downloader = ReplayDownloader("./replays_qa", "./configs/downloader_config.yml")
    downloader.start_download("sc2rep")
