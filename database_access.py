import logging
from pathlib import Path
from time import strftime, strptime
from typing import Any, Tuple

import psycopg2 as pgsql

from config import get_config
from setup_handler import get_handler


class DB:
    def __init__(self, config_path: str):
        self.config = get_config(config_path)
        self.conn = pgsql.connect(
            host=self.config["db_host"],
            database=self.config["db_name"],
            user=self.config["db_user"],
            password=self.config["db_password"],
        )

        self.logger = logging.getLogger(__name__)
        self.logger.addHandler(get_handler())
        self.logger.setLevel(logging.DEBUG)

    def __enter__(self):
        try:
            self.cur = self.conn.cursor()
        except (Exception, pgsql.DatabaseError) as error:
            self.logger.critical(error)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.cur.close()
        if isinstance(exc_value, Exception):
            self.conn.rollback()
        else:
            self.conn.commit()
        if exc_type:
            self.logger.error(f"{type}: {exc_value}\n{traceback}")
        self.conn.close()

    def _set_attrs(self, config_path, db_name):
        self.schema_config = get_config(config_path)[db_name]
        self.column_names = self.schema_config["columns"].keys()
        self.name = self.schema_config["table_name"]

    def create_table(self):
        query = f"CREATE TABLE IF NOT EXISTS {self.name}(\n"
        for key, val in self.schema_config["columns"].items():
            to_append = f"{key} {val},\n"
            query += to_append
        if "creation_closure" in self.schema_config:
            query += ",\n".join(self.schema_config["creation_closure"].values())
        query += ");"

        self.cur.execute(query)

    def _table_exists(self, tb_name):
        self.cur.execute(
            """SELECT table_name FROM information_schema.tables
               WHERE table_schema = 'public'"""
        )
        tables = list(self.cur.fetchall())
        return tb_name in tables

    def _exec_query_one(self, query, *args):
        self.last_query = query.replace("?", "{}").format(*args)
        self.cur.execute(query, args)
        self.logger.debug(self.last_query)
        return self.cur.fetchone()

    def _exec_query_many(self, query, *args):
        self.last_query = query.replace("?", "{}").format(*args)
        self.cur.execute(query, args)
        self.logger.debug(self.last_query)
        while True:
            item = self.cur.fetchone()
            yield item if item is not None else StopIteration

    def _exec_update(self, query, *args):
        self.last_query = query.replace("?", "{}").format(*args)
        self.cur.execute(query, args)
        self.logger.debug(self.last_query)

    def put(self, *args):
        raise NotImplementedError

    def get(self, *args):
        raise NotImplementedError


class GameInfo(DB):
    def __init__(self, secrets_path: str, schema_config_path: str):
        super().__init__(secrets_path)
        self._set_attrs(schema_config_path, "game_info")

    def put(
        self,
        timestamp_played,
        timestamp_processed,
        players_hash,
        end_time,
        player_1_id,
        player_1_race,
        player_1_league,
        player_2,
        player_2_race,
        player_2_league,
        map_hash,
        matchup,
        is_ladder,
        replay_path,
    ):
        args = (
            timestamp_played,
            timestamp_processed,
            players_hash,
            end_time,
            player_1_id,
            player_1_race,
            player_1_league,
            player_2,
            player_2_race,
            player_2_league,
            map_hash,
            matchup,
            is_ladder,
            replay_path,
        )
        query = f"INSERT INTO {self.name} ({tuple(self.column_names[1:])})\n"
        query += f"VALUES ({', '.join(['%s' for _ in args])})\n"
        query += "RETURNING id;"
        return self._exec_query_one(query, *args)[0]

    def get(self, *args):
        if args:
            query = f"SELECT ({', '.join(['%s' for _ in args])}) FROM {self.name};"
        else:
            query = f"SELECT * FROM {self.name};"
        self._exec_query_many(query, *args)


class PlayerInfo(DB):
    def __init__(self, secrets_path: str, schema_config_path: str):
        super().__init__(secrets_path)
        self._set_attrs(schema_config_path, "player_info")

    def put(
        self,
        battle_tag: int,
        nickname: str,
        race: str,
        league_int: int,
        is_win: bool,
    ):
        get_args_dict = {
            "nickname": "",
            "games_played": 0,
            "zerg_played": 0,
            "protoss_played": 0,
            "terran_played": 0,
            "wins": 0,
            "loses": 0,
            "most_played_race": "Z",
            "highest_league": 0,
        }
        get_prev_query = f"""SELECT ({', '.join(['%s' for _ in get_args_dict.keys()])})
                             FROM {self.name},
                             WHERE id = {battle_tag},
                             """
        query_result = self._exec_query_one(get_prev_query, get_args_dict.keys())
        if query_result is not None:
            for i, key in enumerate(get_args_dict.keys()):
                get_args_dict[key] = query_result[i]
        get_args_dict["nickname"] = nickname
        get_args_dict["games_played"] += 1
        if race.casefold() == "z":
            get_args_dict["zerg_played"] += 1
        elif race.casefold() == "p":
            get_args_dict["protoss_played"] += 1
        elif race.casefold() == "t":
            get_args_dict["terran_played"] += 1
        else:
            raise ValueError(f'race value "{race}" is not in ["z", "p", "t"]')

        if is_win:
            get_args_dict["wins"] += 1
        else:
            get_args_dict["loses"] += 1

        race_plays = [
            get_args_dict["zerg_played"],
            get_args_dict["protoss_played"],
            get_args_dict["terran_played"],
        ]
        get_args_dict["most_played_race"] = ["Z", "P", "T"][
            race_plays.index(max(race_plays))
        ]
        get_args_dict["highest_league"] = max(
            get_args_dict["highest_league"], league_int
        )

        query = f"INSERT INTO {self.name} ({tuple(get_args_dict.keys())})\n"
        query += f"VALUES ({', '.join(['%s' for _ in get_args_dict.values()])});"
        self._exec_update(query, *get_args_dict.values())

    def get(self, *args):
        if args:
            query = f"SELECT ({', '.join(['%s' for _ in args])}) FROM {self.name};"
        else:
            query = f"SELECT * FROM {self.name};"
        self._exec_query_many(query, *args)


class MapInfo(DB):
    def __init__(self, secrets_path: str, schema_config_path: str):
        super().__init__(secrets_path)
        self._set_attrs(schema_config_path, "map_info")

    def put(
        self,
        map_hash: str,
        map_name: str,
        matchup_type: str,
        game_date: str,
    ):
        get_args_dict = {
            "map_hash": map_hash,
            "map_name": map_name,
            "matchup_type": matchup_type,
            "first_game_date": "01-01-2010",
        }

        get_prev_query = f"""SELECT ({', '.join(['%s' for _ in get_args_dict.keys()])})
                             FROM {self.name},
                             WHERE map_hash = {map_hash},
                             """
        query_result = self._exec_query_one(get_prev_query, get_args_dict.keys())
        if query_result is not None:
            for i, key in enumerate(get_args_dict.keys()):
                get_args_dict[key] = query_result[i]
        new_date = strptime(game_date, "%d-%M-%Y")
        old_date = strptime(get_args_dict["first_game_date"], "%d-%M-%Y")
        new_date = max(new_date, old_date)
        get_args_dict["first_game_date"] = strftime("%d-%M-%Y", new_date)

        query = f"INSERT INTO {self.name} ({tuple(get_args_dict.keys())})\n"
        query += f"VALUES ({', '.join(['%s' for _ in get_args_dict.values()])});"
        self._exec_update(query, *get_args_dict.values())

    def get(self, *args):
        if args:
            query = f"SELECT ({', '.join(['%s' for _ in args])}) FROM {self.name};"
        else:
            query = f"SELECT * FROM {self.name};"
        self._exec_query_many(query, *args)


class BuildOrder(DB):
    def __init__(self, secrets_path: str, schema_config_path: str):
        super().__init__(secrets_path)
        self._set_attrs(schema_config_path, "build_order")

    def put(self, **col_data):
        query = f"INSERT INTO {self.name} ({tuple(col_data.keys())})\n"
        query += f"VALUES ({', '.join(['%s' for _ in col_data.values()])});"
        self._exec_update(query, *col_data.values())

    def get(self, *args):
        if args:
            query = f"SELECT ({', '.join(['%s' for _ in args])}) FROM {self.name};"
        else:
            query = f"SELECT * FROM {self.name};"
        self._exec_query_many(query, *args)
