from datetime import datetime
from pathlib import Path

import pandas as pd
from alive_progress import alive_it

from database_access import BuildOrder, GameInfo, MapInfo, PlayerInfo
from setup_logger import get_logger
from starcraft2_replay_parse.replay_tools import BuildOrderData, ReplayData


class ReplayFilter:
    """
    Setups and runs a replay filter.
    Can print skipped replays and filter configuration.
    Where are several criteria:
        is_ladder:
            Check if it is a ladder game.
        league:
            Compare min. players league.
        time_played:
            Sets date constrain on the games.
        is_1v1:
            Filters out non-1v1 game such as Coop, 2v2, Campaign and so on.
        has_race:
            Check if the replay has a player with the requested game race.
        matchup:
            Checks matchup, works in reverse too.
        game_len:
            Skip if the game is too short or too long.

    """

    _is_ladder_types = {
        "disable": None,
        "valid": [bool],
    }
    _league_types = {
        "disable": None,
        "valid": [list, int],
    }
    _time_played_types = {
        "disable": None,
        "valid": [datetime, {"root": list, "contains": [datetime]}],
    }
    _is_1v1_types = {
        "disable": None,
        "valid": [bool],
    }
    _has_race_types = {
        "disable": None,
        "valid": [{"root": str, "vals": ["z", "t", "p"]}],
    }
    _matchup_types = {
        "disable": None,
        "valid": [
            {
                "root": str,
                "vals": ["ZvZ", "ZvT", "ZvP", "TvT", "TvP", "PvP"],
                "misc": "Works both in direct or reverse order",
            }
        ],
    }
    _game_len_types = {
        "disable": None,
        "valid": [int, {"root": list, "contains": [int]}],
    }

    _list_filters = (
        "is_ladder",
        "league",
        "time_played",
        "is_1v1",
        "has_race",
        "matchup",
        "game_len",
    )

    def __init__(self) -> None:
        for filter_name in self._list_filters:
            setattr(self, filter_name, None)

        self.passed_filters = [True for i in self._list_filters]
        self.logger = get_logger(__name__)
        self.report = "No report to show, please call this instance"

    def get_valid_types(self, name):
        """
            Prints info about the class' filter
            Args:
                name: str - filter_name
        """
        assert hasattr(self, name)
        types_dict = getattr(self, f"_{name}_types")
        disable_type = types_dict["disable"]
        to_print = f"Disable keyword = '{disable_type}'"
        print(to_print)
        self.logger.info(to_print)
        valid = types_dict["valid"]
        for v_type in valid:
            if isinstance(v_type, dict):
                type_name = valid["root"]
                child_type = valid.get("contains", None)
                possible_vals = valid.get("vals", None)
                comments = valid.get("misc", None)
                str_out = "Valid_types = "
                str_out += (
                    f"{type_name}" if not child_type else f"{type_name}[{child_type}]"
                )
                if possible_vals:
                    str_out += f"\n\tChosen from: {possible_vals}"
                if comments:
                    str_out += f"\n\t({comments})"
                print(str_out)
                self.logger.info(str_out)
            else:
                print(to_print)
                self.logger.info(to_print)

    def is_val_matches(self, val, v_type):
        """
            Check if the compared value matches that of the
            accepted filter values
            Args:
                val: Any - compared value
                v_type: dict - filter data
            Returns:
                is_matches: bool
        """
        root_type = v_type["root"]
        if not isinstance(val, root_type):
            return False
        child_type_lst = v_type.get("contains", None)
        if child_type_lst is not None:
            try:
                child = val[0]
            except TypeError:
                return False
            if not any(isinstance(child, ct) for ct in child_type_lst):
                return False
        possible_vals = v_type.get("vals", None)
        if possible_vals is not None:
            if val not in possible_vals:
                return False
        return True

    def setup_val(self, name, val):
        """
            Setup filter by name
            Args:
                name: str - filter name
                val: Any - filter value
        """
        bad_finish = False
        my_types = getattr(self, f"_{name}_types")
        for key, types in my_types.items():
            if val is types:
                break
            if isinstance(types, list):
                for v_type in types:
                    if isinstance(v_type, dict):
                        bad_finish = not self.is_val_matches(val, v_type)
                    else:
                        if not isinstance(val, v_type):
                            bad_finish = True
                        else:
                            break
        if not bad_finish:
            print(f"For filter '{name}' selected action is '{key}' ({val})")
            return val
        print(f"Bad value for '{name}' ({val}), setting to 'disable'")
        return my_types["disable"]

    @property
    def is_ladder(self):
        return self._is_ladder

    @is_ladder.setter
    def is_ladder(self, val):
        self._is_ladder = self.setup_val("is_ladder", val)

    @property
    def league(self):
        return self._league

    @league.setter
    def league(self, val):
        self._league = self.setup_val("league", val)

    @property
    def time_played(self):
        return self._time_played

    @time_played.setter
    def time_played(self, val):
        self._time_played = self.setup_val("time_played", val)

    @property
    def is_1v1(self):
        return self._is_1v1

    @is_1v1.setter
    def is_1v1(self, val):
        self._is_1v1 = self.setup_val("is_1v1", val)

    @property
    def has_race(self):
        return self._has_race

    @has_race.setter
    def has_race(self, val):
        self._has_race = self.setup_val("has_race", val)

    @property
    def matchup(self):
        return self._matchup

    @matchup.setter
    def matchup(self, val):
        self._matchup = self.setup_val("matchup", val)

    @property
    def game_len(self):
        return self._game_len

    @game_len.setter
    def game_len(self, val):
        self._game_len = self.setup_val("game_len", val)

    def check_is_ladder(self, replay_dict):
        if self.is_ladder == self._is_ladder_types["disable"]:
            return True
        return replay_dict["is_ladder"] == self.is_ladder

    def check_league(self, replay_dict):
        if self.league == self._league_types["disable"]:
            return True
        if isinstance(self.league, list):
            return replay_dict["league"] in self.league
        return replay_dict["league"] == self.league

    def check_time_played(self, replay_dict):
        if self.time_played == self._time_played_types["disable"]:
            return True
        replay_date = replay_dict["date"]
        if isinstance(self.time_played, list):
            return self.time_played[0] <= replay_date <= self.time_played[1]
        return replay_date >= self.time_played

    def check_is_1v1(self, replay_dict):
        if self.is_1v1 == self._is_1v1_types["disable"]:
            return True
        return replay_dict["mode"] == "1v1"

    def check_has_race(self, replay_dict):
        if self.has_race == self._has_race_types["disable"]:
            return True
        return self.has_race in replay_dict["matchup"].casefold()

    def check_matchup(self, replay_dict):
        if self.matchup == self._matchup_types["disable"]:
            return True
        return (
            self.matchup.lower() == replay_dict["matchup"].lower()
            or self.matchup.lower()[::-1] == replay_dict["matchup"].lower()
        )

    def check_game_len(self, replay_dict):
        if self.game_len == self._game_len_types["disable"]:
            return True
        replay_len = replay_dict["frames"]
        if isinstance(self.game_len, list):
            return replay_len >= self.game_len[0] and replay_len <= self.game_len[1]
        return replay_len >= self.game_len

    def __call__(self, replay):
        replay_dict = replay.as_dict()
        replay_dict["date"] = replay.replay.date
        for i, name in enumerate(self._list_filters):
            check_method = getattr(self, f"check_{name}")
            self.passed_filters[i] = check_method(replay_dict)
        self.report = "\n".join(
            [
                f"{'! '*val}{name}==>{'Pass' if val else 'Fail'}"
                for name, val in zip(self._list_filters, self.passed_filters)
            ]
        )
        return all(self.passed_filters)


class ReplayProcess:
    """
        Loads replays from the filesystem, processes them
        using the starcraft2_replay_parse lib and sends them 
        to the database.

        This is a preprocessing step. The training data is 
        prepared in the pipeline.
    """
    def __init__(
        self,
        secrets_path,
        db_config,
        game_data_path,
        max_tick=28800,
        ticks_per_pos=32,
        jupyter=None,
    ) -> None:
        """
            Args:
                secrets_path: str - path to the secrets file
                db_config: str - path to the db config
                game_data_path: str - path to the game_info.csv file
                max_tick: int - maximum game length in tick (1s = 16 ticks)
                ticks_per_pos: int - step size between values in the DB
                jupyter: bool | None - fix the progress bar issues
        """
        self.game_info_db = GameInfo(secrets_path, db_config)
        self.build_order_db = BuildOrder(secrets_path, db_config)
        self.player_info_db = PlayerInfo(secrets_path, db_config)
        self.map_info_db = MapInfo(secrets_path, db_config)
        self.dbs = [
            self.map_info_db,
            self.player_info_db,
            self.game_info_db,
            self.build_order_db,
        ]

        self.init_dbs()
        self.build_order_cls = BuildOrderData(max_tick, ticks_per_pos, game_data_path)
        self.game_data = pd.read_csv(game_data_path, index_col="name")
        self.jupyter = jupyter
        self.logger = get_logger(__name__)
        self.corrupted_data_list = []

    def init_dbs(self):
        """
            Creates the necessary DBs
        """
        for db in self.dbs:
            # with db:
            #     db.drop()
            with db:
                db.create_table()

    def _upload_game_info(self, replay, replay_path):
        """
            Upload data into the game_info DB
        """
        replay_data = replay.as_dict()
        game_info = {}
        date_played = replay.replay.date
        game_length = replay.replay.game_length
        game_length = (
            game_length.hours * 3600 + game_length.mins * 60 + game_length.secs
        )
        player_1_name = replay.player_names[0]
        player_2_name = replay.player_names[1]
        game_info["timestamp_played"] = int(date_played.timestamp())
        game_info["date_processed"] = replay_data["processed_on"]
        game_info["players_hash"] = replay.players_hash
        game_info["end_time"] = game_length
        game_info["player_1_id"] = replay_data["players_data"][player_1_name]["id"]
        game_info["player_1_race"] = replay_data["players_data"][player_1_name]["race"][
            0
        ].lower()
        game_info["player_1_league"] = replay_data["players_data"][player_1_name][
            "league"
        ]
        game_info["player_1_winner"] = replay_data["players_data"][player_1_name][
            "is_winner"
        ]
        game_info["player_2_id"] = replay_data["players_data"][player_2_name]["id"]
        game_info["player_2_race"] = replay_data["players_data"][player_2_name]["race"][
            0
        ].lower()
        game_info["player_2_league"] = replay_data["players_data"][player_2_name][
            "league"
        ]
        game_info["player_2_winner"] = replay_data["players_data"][player_2_name][
            "is_winner"
        ]
        game_info["map_hash"] = replay.map_hash
        game_info["matchup"] = replay_data["matchup"]
        game_info["is_ladder"] = replay.is_ranked
        game_info["replay_path"] = str(replay_path.resolve())
        game_id = self._upload_info(self.game_info_db, game_info)
        return game_id

    def _upload_map_info(self, replay):
        """
            Upload data into the map_info DB
        """
        replay_data = replay.as_dict()
        num_players = len(replay_data["matchup"].split("v")) // 2
        matchup_type = f"{num_players}v{num_players}"
        date = replay.replay.date
        map_info = {
            "map_hash": replay.map_hash,
            "map_name": replay.map_name,
            "matchup_type": matchup_type,
            "game_date": date,
        }
        self._upload_info(self.map_info_db, map_info)

    def _upload_player_info(self, replay):
        """
            Upload data into the player_info DB
        """
        replay_data = replay.as_dict()
        forbidden_symbols = "%<>&;"
        for name in replay.player_names:
            if any(s in name for s in forbidden_symbols):
                nickname = "||||||||||||"
            else:
                nickname = name
            player_info = {
                "player_id": replay_data["players_data"][name]["id"],
                "nickname": nickname,
                "race": replay_data["players_data"][name]["race"][0].lower(),
                "league_int": replay_data["players_data"][name]["league"],
                "is_win": name in replay_data["winners"],
            }
            self._upload_info(self.player_info_db, player_info)

    def delete_game(self, game_id):
        """
            Deletes game from the game_info table.
            Useful then the upload was interrupted.
        """
        with self.game_info_db as db:
            db.delete_id(game_id)

    def _upload_build_order(self, replay, game_id, bar=None):
        """
            Upload data into the build_order DB
        """
        replay_data = replay.as_dict()
        full_upload_dict = {}
        try:
            unit_counts = list(self.build_order_cls.yield_unit_counts(replay_data))
        except KeyError as exc:
            msg = "INVALID REPLAY: %s" % exc
            self.logger.warning(msg)
            print(msg)
            self.delete_game(game_id)
            return

        for i, build_order_dict in enumerate(unit_counts):
            for key, val in build_order_dict.items():
                try:
                    val_type = self.game_data.loc[key, "type"].lower()
                except KeyError:
                    val_type = "special"
                new_key = f"player_{i+1}_{val_type}_{key.lower()}"
                full_upload_dict[new_key] = val

        ticks = self.build_order_cls.get_ticks()
        ticks_len = len(ticks)
        exit_code = False
        for j, tick in enumerate(ticks):
            to_upload_dict = {}
            for key, val in full_upload_dict.items():
                to_upload_dict[key] = val[j]
            to_upload_dict["game_id"] = game_id
            to_upload_dict["tick"] = tick
            if tick == 0:
                # One of this values is always > 0, if not, the game is corrupted
                s, d, p = (
                    to_upload_dict["player_1_unit_scv"],
                    to_upload_dict["player_1_unit_drone"],
                    to_upload_dict["player_1_unit_probe"],
                )
                if s == d == p == 0:
                    print(f"Corrupted data at game_id = {game_id}")
                    self.corrupted_data_list.append(game_id)
                    return
            if bar is not None:
                bar.text = f"Processed {j/ticks_len:.1%}"
            try:
                self._upload_info(self.build_order_db, to_upload_dict)
            except KeyboardInterrupt:
                print("KeyboardInterrupt detected! Exiting after data upload finishes")
                exit_code = True
        if exit_code:
            raise KeyboardInterrupt

    def _upload_info(self, db, to_upload_dict):
        """
            Upload the parsed data into the DB
        """
        out = None
        with db:
            out = db.put(**to_upload_dict)
        return out

    def game_id_if_exists(self, players_hash, timestamp_played):
        """
            Returns game id if the replay object already exists
            Args:
                players_hash: str - hash of players' nicknames
                timestamp_player: datetime.timestamp - date played
            Returns:
                game_id: int | None - return id if it exists
        """
        with self.game_info_db:
            return self.game_info_db.get_id_if_exists(players_hash, timestamp_played)

    def process_replays(self, replay_dir, filt=None):
        """
            Load replay from the filesystem into the DB.
            Parse data from `.SC2Replay` object into the DB rows.
            Shows progress bar
            Args:
                replay_dir: str - path to the directory with replays
                filt: ReplayFilter | None - filter instance
        """
        replay_dir = Path(replay_dir)
        list_file = [p for p in replay_dir.iterdir() if p.suffix == ".SC2Replay"]
        if self.jupyter in (True, False):
            bar = alive_it(list_file, force_tty=self.jupyter)
        else:
            bar = alive_it(list_file)

        for replay_path in bar:
            try:
                replay = ReplayData().parse_replay(replay_path)
            except Exception as exc:
                msg = f"Replay skipped, reason:\n{exc}"
                print(msg)
                self.logger.error(msg)
                continue

            if filt is not None:
                if not filt(replay):
                    info = f"Replay skipped, reason: \nStopped by filter: {filt.report}"
                    self.logger.info(info)
                    print(info)
                    continue

            players_hash = replay.players_hash
            timestamp_played = int(replay.replay.date.timestamp())

            game_id = self.game_id_if_exists(players_hash, timestamp_played)
            if game_id is None:
                self._upload_map_info(replay)
                self._upload_player_info(replay)
                id = self._upload_game_info(replay, replay_path)
                self._upload_build_order(replay, id, bar=bar)
            else:
                with self.game_info_db:
                    self.game_info_db.update_path(game_id, replay_path)
                info = (
                    "Replay skipped, reason:\nAlready exists in the db (path updated)"
                )
                self.logger.info(info)
                print(info)


if __name__ == "__main__":
    replay_filter = ReplayFilter()
    replay_filter.is_1v1 = True
    replay_filter.game_len = [1920, 28800]
    replay_filter.time_played = datetime(2021, 1, 1)
    processor = ReplayProcess(
        "./configs/secrets.yml",
        "configs/database.yml",
        "./starcraft2_replay_parse/game_info.csv",
        ticks_per_pos=32,
    )
    processor.process_replays("../replays/", filt=replay_filter)
