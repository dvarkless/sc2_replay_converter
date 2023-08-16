import logging
import sys
from datetime import datetime
from time import strftime, strptime

import psycopg2 as pgsql
from psycopg2 import sql
from psycopg2.extras import DictCursor

from config import get_config
from setup_handler import get_handler


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class DB(metaclass=Singleton):
    def __init__(self, config_path: str, db_return_type=None):
        self.config = get_config(config_path)
        self.db_return_type = db_return_type
        self.logger = logging.getLogger(__name__)
        self.logger.addHandler(get_handler())
        self.logger.setLevel(logging.DEBUG)

    def __enter__(self):
        self.conn = self._get_connection()
        cur_factory = None
        if self.db_return_type == "dict":
            cur_factory = DictCursor
        try:
            if cur_factory is not None:
                self.cur = self.conn.cursor(cursor_factory=cur_factory)
            else:
                self.cur = self.conn.cursor()
        except (Exception, pgsql.DatabaseError) as error:
            print(f"Critical: {error}")
            self.logger.critical(error)
            raise pgsql.DatabaseError(error)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.cur.close()
        if isinstance(exc_value, Exception):
            self.conn.rollback()
        else:
            self.conn.commit()
        if exc_type:
            self.logger.error(f"{exc_type}: {exc_value}\n{traceback}")
        self.conn.close()

    @property
    def query(self):
        return self._query

    @query.setter
    def query(self, query_file):
        _query = open(query_file).read()
        self._query = self._compose_query(_query)

    def _get_connection(self):
        return pgsql.connect(
            host=self.config["db_host"],
            database=self.config["db_name"],
            user=self.config["db_user"],
            password=self.config["db_password"],
        )

    def _set_attrs(self, config_path, db_name):
        self.db_config = get_config(config_path)[db_name]
        self.name = self.db_config["table_name"]

    def _save_changes(self):
        try:
            self.conn.commit()
        except (Exception, pgsql.DatabaseError) as error:
            self.logger.critical(error)
            print(error)
            self.conn.rollback()
        self.cur = self.conn.cursor()
        self.logger.info(f'manual commit at "{self.name}"')

    def _compose_query(self, query):
        if "{}" in query:
            return sql.SQL(query).format(sql.Identifier(self.name))
        return sql.SQL(query)

    def drop(self):
        self.query = self.db_config["drop_table_file"]
        self._exec_update(self.query, self.name)
        self.logger.info(f'table "{self.name}" dropped')

    def get_columns(self):
        self.query = self.db_config["get_columns_file"]
        return self._exec_query_one(self.query, {})

    def create_table(self, query=None):
        if query is None:
            query_path = self.db_config["create_table_file"]
            query = open(query_path).read()
        try:
            self.cur.execute(query)
        except pgsql.ProgrammingError as e:
            print(e)
            self.logger.error(e)
            self.conn.rollback()
        except pgsql.InterfaceError as e:
            print(e)
            self.logger.error(e)
            self.conn = self._get_connection()
            self.cur = self.conn.cursor()
        self._save_changes()

    def exists(self):
        self.query = self.db_config["get_tables_file"]
        tables = list(self._exec_query_many(self.query, {}))
        return self.name in tables

    def _exec_query_one(self, query, kwargs):
        query = self.cur.mogrify(query, kwargs)
        self.last_query = query
        try:
            self.cur.execute(query)
        except pgsql.ProgrammingError as e:
            print(f"Error:{e} \nAT QUERY: '{query}'")
            self.logger.error(f"Error:{e} \nAT QUERY: '{query}'")
            self.conn.rollback()
        except pgsql.InterfaceError as e:
            print(f"Error:{e} \nAT QUERY: '{query}'")
            self.logger.error(f"Error:{e} \nAT QUERY: '{query}'")
            self.conn = self._get_connection()
            self.cur = self.conn.cursor()
        except Exception as e:
            print(f"Error:{e} \nAT QUERY: '{query}'")
            self.logger.error(f"Error:{e} \nAT QUERY: '{query}'")
            sys.exit()

        self.logger.debug(self.last_query)
        out = None
        try:
            out = self.cur.fetchone()
        except pgsql.ProgrammingError as e:
            print(e)
            self.logger.warning(e)
        self.logger.debug(f"Output: {out}")
        return out if out else None

    def _exec_query_many(self, query, kwargs):
        query = self.cur.mogrify(query, kwargs)
        self.last_query = query
        try:
            self.cur.execute(query)
        except pgsql.ProgrammingError as e:
            print(f"Error:{e} \nAT QUERY: '{query}'")
            self.logger.error(f"Error:{e} \nAT QUERY: '{query}'")
            self.conn.rollback()
        except pgsql.InterfaceError as e:
            print(f"Error:{e} \nAT QUERY: '{query}'")
            self.logger.error(f"Error:{e} \nAT QUERY: '{query}'")
            self.conn = self._get_connection()
            self.cur = self.conn.cursor()
        except Exception as e:
            print(f"Error:{e} \nAT QUERY: '{query}'")
            self.logger.error(f"Error:{e} \nAT QUERY: '{query}'")
            sys.exit()
        self.logger.debug(self.last_query)
        while True:
            item = None
            try:
                item = self.cur.fetchone()
            except pgsql.ProgrammingError as e:
                print(e)
                self.logger.warning(e)
            self.logger.debug(f"Output: {item}")
            yield item if item is not None else StopIteration

    def _exec_insert(self, query, kwargs):
        query = self.cur.mogrify(query, kwargs)
        self.last_query = query
        try:
            self.cur.execute(query)
        except pgsql.ProgrammingError as e:
            print(f"Error:{e} \nAT QUERY: '{query}'")
            self.logger.error(f"Error:{e} \nAT QUERY: '{query}'")
            self.conn.rollback()
        except pgsql.InterfaceError as e:
            print(f"Error:{e} \nAT QUERY: '{query}'")
            self.logger.error(f"Error:{e} \nAT QUERY: '{query}'")
            self.conn = self._get_connection()
            self.cur = self.conn.cursor()
        except Exception as e:
            print(f"Error:{e} \nAT QUERY: '{query}'")
            self.logger.error(f"Error:{e} \nAT QUERY: '{query}'")
            sys.exit()

        self.logger.debug(self.last_query)

    def _exec_update(self, query, kwargs):
        query = self.cur.mogrify(query, kwargs)
        self.last_query = query
        try:
            self.cur.execute(query)
        except pgsql.ProgrammingError as e:
            print(f"Error:{e} \nAT QUERY: '{query}'")
            self.logger.error(f"Error:{e} \nAT QUERY: '{query}'")
            self.conn.rollback()
        except pgsql.InterfaceError as e:
            print(f"Error:{e} \nAT QUERY: '{query}'")
            self.logger.error(f"Error:{e} \nAT QUERY: '{query}'")
            self.conn = self._get_connection()
            self.cur = self.conn.cursor()
        except Exception as e:
            print(f"Error:{e} \nAT QUERY: '{query}'")
            self.logger.error(f"Error:{e} \nAT QUERY: '{query}'")
            sys.exit()

    def put(self, args):
        raise NotImplementedError

    def get(self):
        self.query = self.db_config["select_all"]
        self._exec_query_many(self.query, dict())


class GameInfo(DB):
    def __init__(self, secrets_path: str, db_config_path: str):
        super().__init__(secrets_path)
        self._set_attrs(db_config_path, "game_info")

    def put(
        self,
        timestamp_played,
        date_processed,
        players_hash,
        end_time,
        player_1_id,
        player_1_race,
        player_1_league,
        player_2_id,
        player_2_race,
        player_2_league,
        map_hash,
        matchup,
        is_ladder,
        replay_path,
    ):
        timestamp_played = pgsql.TimestampFromTicks(timestamp_played)
        query_args = {
            "timestamp_played": timestamp_played,
            "date_processed": date_processed,
            "players_hash": players_hash,
            "end_time": end_time,
            "player_1_id": player_1_id,
            "player_1_race": player_1_race,
            "player_1_league": player_1_league,
            "player_2_id": player_2_id,
            "player_2_race": player_2_race,
            "player_2_league": player_2_league,
            "map_hash": map_hash,
            "matchup": matchup,
            "is_ladder": is_ladder,
            "replay_path": replay_path,
        }
        self.query = self.db_config["insert_file"]
        game_id = self._exec_query_one(self.query, query_args)
        if game_id is None:
            raise ValueError(f"returning game_id = {game_id}")
        return game_id[0]

    def update_path(self, game_id, replay_path):
        to_upload = {
            "game_id": game_id,
            "replay_path": str(replay_path),
        }
        self.query = self.db_config["update_path_file"]
        self._exec_update(self.query, to_upload)

    def get_id_if_exists(self, players_hash, timestamp_played):
        timestamp_played = pgsql.TimestampFromTicks(timestamp_played)
        to_upload = {
            "players_hash": players_hash,
            "timestamp_played": timestamp_played,
        }
        self.query = self.db_config["select_game_id_file"]
        out = self._exec_query_one(
            self.query,
            to_upload,
        )
        if self.db_return_type == "dict":
            result_id = out["game_id"] if out is not None else None
        else:
            result_id = out[0] if out is not None else None
        return result_id if out is not None else None

    def get_players_info(self, game_id):
        self.query = self.db_config["select_player"]
        out = self._exec_query_one(self.query, {"game_id": game_id})
        return out


class PlayerInfo(DB):
    def __init__(self, secrets_path: str, db_config_path: str):
        super().__init__(secrets_path)
        self._set_attrs(db_config_path, "player_info")

    def put(
        self,
        player_id: int,
        nickname: str,
        race: str,
        league_int: int,
        is_win: bool,
    ):
        get_args_dict = {
            "games_played": 0,
            "zerg_played": 0,
            "protoss_played": 0,
            "terran_played": 0,
            "wins": 0,
            "loses": 0,
            "most_played_race": "z",
            "highest_league": 0,
        }
        get_prev_query = open(self.db_config["preinsert_select_file"]).read()
        pass_id = {"player_id": player_id}
        query_result = self._exec_query_one(get_prev_query, pass_id)
        val_exists = False
        if query_result is not None:
            val_exists = True
            for i, key in enumerate(get_args_dict.keys()):
                get_args_dict[key] = query_result[i]
        get_args_dict["nickname"] = nickname
        get_args_dict["player_id"] = player_id
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
        get_args_dict["most_played_race"] = ["z", "p", "t"][
            race_plays.index(max(race_plays))
        ]
        get_args_dict["highest_league"] = max(
            get_args_dict["highest_league"], league_int
        )

        if val_exists:
            self.query = self.db_config["update_file"]
            self._exec_update(self.query, get_args_dict)
        else:
            self.query = self.db_config["insert_file"]
            self._exec_insert(self.query, get_args_dict)


class MapInfo(DB):
    def __init__(self, secrets_path: str, db_config_path: str):
        super().__init__(secrets_path)
        self._set_attrs(db_config_path, "map_info")

    def put(
        self,
        map_hash: str,
        map_name: str,
        matchup_type: str,
        game_date,
    ):
        game_date = datetime.date(game_date)
        get_args_dict = {
            "map_hash": map_hash,
            "map_name": map_name,
            "matchup_type": matchup_type,
            "first_game_date": datetime(2010, 1, 1).date(),
        }
        get_prev_query = open(self.db_config["preinsert_select_file"]).read()
        pass_hash = {"map_hash": map_hash}
        query_result = self._exec_query_one(get_prev_query, pass_hash)
        val_exists = False
        if query_result is not None:
            val_exists = True
            for i, key in enumerate(get_args_dict.keys()):
                get_args_dict[key] = query_result[i]

        first_date = get_args_dict["first_game_date"]
        get_args_dict["first_game_date"] = max(game_date, first_date)

        if val_exists:
            self.query = self.db_config["update_file"]
            self._exec_update(self.query, get_args_dict)
        else:
            self.query = self.db_config["insert_file"]
            self._exec_insert(self.query, get_args_dict)


class BuildOrder(DB):
    def __init__(self, secrets_path: str, db_config_path: str):
        super().__init__(secrets_path, db_return_type="dict")
        self._set_attrs(db_config_path, "build_order")

    def put(self, **col_data):
        self.query = self.db_config["insert_file"]
        self._exec_insert(self.query, col_data)

    def get(self):
        self.query = self.db_config["select_file"]
        return self._exec_query_many(self.query, {})

    def get_by_keys(self, game_id, tick):
        to_upload = {
            "game_id": game_id,
            "tick": tick,
        }
        self.query = self.db_config["select_by_keys"]
        return self._exec_query_one(self.query, to_upload)


class MatchupDB(DB):
    def __init__(self, table_name, secrets_path: str, db_config_path: str):
        super().__init__(secrets_path, db_return_type="dict")
        self._set_attrs(db_config_path, table_name)

    def construct_create_query(
        self, player_entities: dict, enemy_entities: dict, out_entities: dict
    ):
        input_entities = player_entities | enemy_entities
        query = ",\n".join((f"{name} INTEGER" for name in input_entities.keys()))
        query += ",\n"
        query += ",\n".join((f"{name} NUMERIC(4, 3)" for name in out_entities.keys()))
        template_query = open(
            self.db_config["create_table_file"], encoding="utf-8"
        ).read()
        query = template_query.format(self.name, cols=query)
        return query

    def construct_insert_query(
        self, player_entities: dict, enemy_entities: dict, out_entities: dict
    ):
        entities = player_entities | enemy_entities | out_entities
        input_query = ", ".join((f"{name}" for name in entities.keys()))
        get_query = ", ".join((f"%({name})s" for name in entities.keys()))
        template_query = open(
            self.db_config["create_table_file"], encoding="utf-8"
        ).read()
        query = template_query.format(
            self.name, cols=input_query, formatted_cols=get_query
        )
        return query

    def create_table(
        self, player_entities: dict, enemy_entities: dict, out_entities: dict
    ):
        query = self.construct_create_query(
            player_entities, enemy_entities, out_entities
        )
        return super().create_table(query=query)

    def put(self, player_entities: dict, enemy_entities: dict, out_entities: dict):
        query = self.construct_insert_query(
            player_entities, enemy_entities, out_entities
        )
        self._exec_insert(query, player_entities | enemy_entities | out_entities)

    def get_id(self, game_id):
        to_pass = {"game_id": game_id}
        self.query = self.db_config["select_where_id"]
        self._exec_query_many(self.query, to_pass)

    def get(self):
        self.query = self.db_config["select_file"]
        self._exec_query_many(self.query, {})
