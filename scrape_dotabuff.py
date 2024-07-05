"""
Scrape dotabuff matches
 
- Download raw html files for all pages of matches in a Dotabuff profile.
- Parse matches into a csv file.
"""

from pathlib import Path
import requests
from time import sleep
import random
from bs4 import BeautifulSoup
import re
import argparse
import pandas as pd
from typing import Dict, List, NewType

PlayerID = NewType("PlayerID", str)


def main(player_id: PlayerID, cache_dir: str | Path, output_csv: str | Path):
    """Scrape dotabuff matches

    Args:
        player_id: Dotabuff player ID.
        cache_dir: Directory containing raw HTML files of Dotabuff matches pages.
        output_csv: Path to parsed csv file.
    """
    cache_dir = Path(cache_dir)

    # Fetch raw html.
    fetch_dotabuff_matches(player_id=player_id, cache_dir=cache_dir)

    # Parse raw html.
    data = []
    for html_file in cache_dir.glob("*.html"):
        with open(html_file, "rb") as f:
            html_page = f.read()
        table_parsed = parse_dotabuff_matches(html_page)
        data.extend(table_parsed)

    # Save to csv.
    pd.DataFrame(data).convert_dtypes().sort_values(
        "timestamp", ascending=False
    ).to_csv(output_csv, index=False)
    print(f"Saved to {output_csv}.")


def fetch_dotabuff_matches(
    player_id: PlayerID,
    cache_dir: Path,
):
    """Fetch raw HTML files of dotabuff match pages.
    Note: Page count starts from the last page.

    Args:
        player_id: Dotabuff player ID.
        cache_dir: Directory to save raw HTML files of Dotabuff match pages.
    """
    cache_dir = Path(cache_dir)
    total_pages = fetch_total_num_pages(player_id)
    page_nums = list(range(total_pages, 0, -1))  # Counting from the last page.

    # Already downloaded HTML pages, assuming filenames are page numbers counting from the last page.
    page_nums_loaded = [int(s.stem) for s in cache_dir.glob("*.html")]
    page_nums_to_load = list(set(page_nums) - set(page_nums_loaded))

    print(
        f"Downloading {total_pages} Dotabuff pages for player {player_id} into {cache_dir}."
    )
    if len(page_nums_to_load) > 0:
        print(
            f"Found {len(page_nums_loaded)} loaded pages in {cache_dir}. Downloading remaining {len(page_nums_to_load)}."
        )
    for i, page_num in enumerate(page_nums_to_load):
        page_num_from_first = total_pages - page_num + 1  # Counting from first page.
        print(f"Page {page_num_from_first}/{total_pages}")
        page = fetch_dotabuff_match_page(player_id, page_num=page_num_from_first)
        with open(cache_dir / f"{str(page_num)}.html", "wb") as f:
            f.write(page)
        sleep(random.uniform(min(5 + i, total_pages), total_pages))
    print("Done.")


def parse_dotabuff_matches(html_page) -> List[Dict]:
    """Parse raw html of a Dotabuff matches page

    Args:
        html_page: Raw html.

    Returns:
        list of match details
    """

    def parse_row(row) -> dict:
        """
        Parse a row in the Dotabuff matches table.
        Some rows contain two values (e.g. hero and skill level).
        """
        colnames = [
            "icon",  # ignore
            "hero-skill_level",
            "sep",  # ignore
            "match_result-match_id-datetime",
            "bracket-game_mode",
            "match_length",
            "kda",
            "items",
        ]
        cols = list(row.children)
        if len(cols) <= 1:
            return dict()

        # Column 2
        col_hero = cols[1]
        hero_name = col_hero.find("a").text
        match_id = col_hero.find("a").attrs["href"].replace("/matches/", "")
        skill_level = col_hero.find("div").text

        # Column 4
        col_match = cols[3]
        result = col_match.find("a").text
        datetime = col_match.find("div").find("time").attrs["datetime"]

        # Column 5
        col_mode = cols[4]
        bracket = col_mode.text
        game_mode = col_mode.find("div").text

        # Column 6
        duration = cols[5].text

        # Col 7
        kda = "-".join(
            [s.text for s in cols[6].find_all("span", attrs={"class": "value"})]
        )

        # Col 8
        items = ",".join(
            [e.attrs["href"].replace("/items/", "") for e in cols[7].find_all("a")]
        )

        return {
            "hero": hero_name,
            "match_id": match_id,
            "skill_level": skill_level,
            "result": result,
            "timestamp": datetime,
            "type": bracket,
            "game_mode": game_mode,
            "duration": duration,
            "kda": kda,
            "items": items,
        }

    soup = BeautifulSoup(html_page, features="lxml")
    tables = soup.find_all("table")
    if len(tables) == 0:
        return []
    table = max(tables, key=lambda x: len(x.text))
    # table_colnames = table.contents[0]
    table_rows = table.contents[1]
    return [parse_row(row) for row in table_rows]


def fetch_total_num_pages(player_id: PlayerID):
    """Number of total pages of matches in a Dotabuff profile."""
    page = fetch_dotabuff_match_page(player_id, page_num=1)
    soup = BeautifulSoup(page, features="lxml")
    last_page = (
        soup.find_all("span", attrs={"class": "last"})[0].find("a").attrs["href"]
    )
    try:
        num_pages = int(re.search("(?<=page=)\d+", last_page).group())
    except AttributeError:
        raise Exception("Failed regex for number of pages.")
    return num_pages


def fetch_dotabuff_match_page(player_id: PlayerID, page_num: int = 1):
    """Get a page of Dotabuff matches for a player."""
    r = requests.get(
        url=f"https://www.dotabuff.com/players/{player_id}/matches?page={str(page_num)}",
        headers={"User-agent": "one time script"},
    )
    return r.content


def duration_to_sec(d: str) -> int:
    """Convert from a string hh:mm:ss or mm:ss to int seconds"""
    ds = [int(s) for s in d.split(":")]
    if len(ds) == 3:
        return ds[0] * 3600 + ds[1] * 60 + ds[2]
    if len(ds) == 2:
        return ds[0] * 60 + ds[1]
    if len(ds) == 1:
        return ds


def make_argparser():
    p = argparse.ArgumentParser()
    p.add_argument("-i", "--player_id", type=str, help="Dotabuff player ID.")
    p.add_argument(
        "-c",
        "--cache_dir",
        type=int,
        help="Cache directory for raw HTML files. Defaults to playerID.",
        default=None,
    )
    p.add_argument(
        "-o",
        "--output_csv",
        type=int,
        help="Output csv. Defaults to playerID.",
        default=None,
    )
    return p


if __name__ == "__main__":
    argparser = make_argparser()
    args = argparser.parse_args()

    player_id = args.player_id
    cache_dir = args.cache_dir
    output_csv = args.output_csv

    if cache_dir is None:
        cache_dir = Path(player_id)
        cache_dir.mkdir(parents=True, exist_ok=True)

    if output_csv is None:
        output_csv = Path(f"{str(player_id)}.csv")

    main(player_id=args.player_id, cache_dir=cache_dir, output_csv=output_csv)
