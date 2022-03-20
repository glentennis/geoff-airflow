import os
import sys
import time
import uuid
from json import dumps
from datetime import date, datetime
from tempfile import gettempdir
from nba_api.stats.endpoints import leaguegamefinder, playbyplayv2


def get_game_play_by_play(game_id):
    pbp = playbyplayv2.PlayByPlayV2(
        game_id=game_id
    )  

    results_dict = pbp.get_normalized_dict()
    play_by_play = results_dict['PlayByPlay']
    for d in play_by_play:
        d['_id'] = str(uuid.uuid4())
    return play_by_play


def get_games(logical_date):
    api_date_str = logical_date.strftime('%m/%d/%Y')
    gamefinder = leaguegamefinder.LeagueGameFinder(
        date_from_nullable=api_date_str,
        date_to_nullable=api_date_str,
    )  

    results_dict = gamefinder.get_normalized_dict()
    games = results_dict['LeagueGameFinderResults']
    return games


def write_newline_json(data, fpath):
    with open(fpath, "w") as f:
        content = '\n'.join([dumps(record) for record in data])
        f.write(content)


def main(logical_date):
    filename_date_str = logical_date.strftime('%m_%d_%Y')
    games_filename = f"games_{filename_date_str}.json"
    play_by_play_filename = f"play_by_play_{filename_date_str}.json"
    temp_dir_path = gettempdir()
    print(temp_dir_path)
    games = get_games(logical_date)
    time.sleep(5)
    game_ids = [g['GAME_ID'] for g in games]
    fpath = os.path.join(temp_dir_path, games_filename)
    write_newline_json(games, fpath)

    play_by_play = []
    for game_id in game_ids:
        play_by_play += get_game_play_by_play(game_id)
        time.sleep(5)
    fpath = os.path.join(temp_dir_path, play_by_play_filename)
    write_newline_json(play_by_play, fpath)


if __name__ == '__main__':
    logical_date = datetime.strptime(sys.argv[1], "%Y%m%d")
    main(logical_date=logical_date)
