from math import exp
from pathlib import Path
from random import gauss

import pandas as pd
from psycopg2 import ProgrammingError

from database_access import MatchupDB
from replay_process import ReplayFilter


class ReorganizePlayers:
    def __init__(
        self, player: str, enemy: str, min_league: int, include_unranked=True
    ) -> None:
        self.player = player
        self.enemy = enemy
        self.min_league = min_league
        self.include_unranked = include_unranked

    def check_matchup(self, player_race, enemy_race):
        if player_race == self.player and enemy_race == self.enemy:
            return True
        return False

    def check_league(self, player_data):
        player_league = player_data["league"]
        if self.include_unranked:
            return bool(player_league >= self.min_league or player_league == 0)
        return bool(player_league >= self.min_league)

    def transform(self, data_dict):
        p1r, p1w = data_dict["player_1"]["race"], data_dict["player_1"]["is_win"]
        p2r, p2w = data_dict["player_2"]["race"], data_dict["player_2"]["is_win"]
        p1_pass = self.check_league(data_dict["player_1"])
        p2_pass = self.check_league(data_dict["player_2"])
        if self.check_matchup(p1r, p2r) and p1_pass:
            return (True, "player_1", p1w)
        if self.check_matchup(p2r, p1r) and p2_pass:
            return (True, "player_2", p2w)
        return (False, "player_1", p1w)


class RandomPoints:
    def __init__(
        self, mean_step, sigma, get_final_point: bool, final_point_step, tick_step
    ) -> None:
        self.mean_step = mean_step
        self.sigma = sigma
        self.get_final_point = get_final_point
        self.final_point_step = final_point_step
        self.tick_step = tick_step
        self.final_point_pos = self._from_tick(final_point_step)

    def _to_tick(self, val):
        return int(val) * self.tick_step

    def _from_tick(self, val):
        return val // self.tick_step

    def get_random_ticks(self, end_val):
        end_pos = self._from_tick(end_val)
        out_pos = 0
        out_list = []
        end_pos = (
            end_pos if not self.get_final_point else end_pos - self.final_point_pos
        )
        out_pos += self._from_tick(gauss(self.mean_step, self.sigma)) // 2
        while out_pos < end_pos:
            out_list.append(out_pos)
            out_pos += self._from_tick(gauss(self.mean_step, self.sigma))

        worst_case_val = [end_pos // 2]
        return out_list if out_list else worst_case_val

    def get_final_points(self, starting_points, end_val):
        final_points = []
        if self.get_final_point:
            for point in starting_points:
                final_point = point + self.final_point_pos
                final_point = min(final_point, self._from_tick(end_val))
                final_points.append(final_point)
        return final_points

    def transform(self, end_val):
        out_points = self.get_random_ticks(end_val)
        final_points = self.get_final_points(out_points, end_val)
        out_ticks = [self._to_tick(p) for p in out_points]
        final_ticks = [self._to_tick(p) for p in final_points]
        return (out_ticks, final_ticks)


class NormalizeColumns:
    game_race_dict = {
        "z": "Zerg",
        "p": "Protoss",
        "t": "Terran",
    }
    special_names = [
        "minerals_available",
        "vespene_available",
    ]

    def __init__(self, game_info_file, supply_data_file) -> None:
        self.supply_data = pd.read_csv(supply_data_file, index_col="name")
        self.game_info = pd.read_csv(game_info_file, index_col="name")
        self.game_info = self.game_info.rename(index=str.lower)

    def setup_filter(
        self,
        player,
        r,
        include_buildings=False,
        include_upgrades=False,
        include_special=False,
        include_tick=False,
        include_units=True,
    ):
        self.player = player
        self.r = self.game_race_dict[r]
        self.include_buildings = include_buildings
        self.include_upgrades = include_upgrades
        self.include_special = include_special
        self.include_tick = include_tick
        self.include_units = include_units

    def filter_columns(self, data):
        # ("player_N_column", int_val)
        pairs = list(data.items())
        # Example:
        # Pass if player_1 in player_1_unit_Drone
        pairs = [pair for pair in pairs if self.player in pair[0]]
        # player_1_unit_Drone ==> unit_Drone
        pairs = [(pair[0].removeprefix(self.player + "_"), pair[1]) for pair in pairs]
        # unit_Drone ==> Drone
        for prefix in ("upgrade_", "building_", "special_", "unit_"):
            pairs = [(pair[0].removeprefix(prefix), pair[1]) for pair in pairs]
        # Pass if Drone in zerg units
        df = self._filter_race(self.game_info, self.r)
        if not self.include_units:
            df = self._filter_units(df)
        if not self.include_buildings:
            df = self._filter_buildings(df)
        if not self.include_upgrades:
            df = self._filter_upgrades(df)
        ind_list = list(df.index)
        if self.include_tick:
            ind_list += ["tick"]
        if self.include_special:
            ind_list += self.special_names
        pairs = [pair for pair in pairs if pair[0] in ind_list]
        # ("Zergling", 4) ==> ("Zergling", 2)
        pairs = [self.normalize_units(*pair) for pair in pairs]
        return dict(pairs)

    def _filter_race(self, df, race):
        df = df[df.loc[:, "race"] == race]
        return df

    def _filter_units(self, df):
        df = df[df.loc[:, "type"] != "Unit"]
        return df

    def _filter_upgrades(self, df):
        df = df[df.loc[:, "type"] != "Upgrade"]
        return df

    def _filter_buildings(self, df):
        df = df[df.loc[:, "type"] != "Building"]
        return df

    def normalize_units(self, name, val):
        if name in self.supply_data.index:
            val *= self.supply_data.loc[name, "supply"]
        return (name, val)

    def transform(self, data):
        data = self.filter_columns(data)
        return data


class CalcWinprob:
    def __init__(self, delay=5) -> None:
        self.delay = delay

    def transform(self, final_tick, is_win, end_tick):
        arg = is_win * (final_tick / end_tick) * self.delay
        return 1 / (1 + exp(-arg))


class DensityVals:
    def __init__(
        self,
        supply_data_file,
        reducer="avg",
    ) -> None:
        self.supply_data = pd.read_csv(supply_data_file, index_col="name")
        self.supply_data = self.supply_data.rename(index=str.lower)
        possible_reducers = ("avg", "softmax")
        if reducer not in possible_reducers:
            raise KeyError(f"Key 'reducer' should be chosen from {possible_reducers}")
        self.reducer_func = self.reducer_funcs(reducer)

    def reducer_funcs(self, name):
        reducer_funcs = {
            "avg": self._get_avg_vals,
            "softmax": self._get_softmax_vals,
        }
        return reducer_funcs[name]

    def ceil(self, data):
        for key, val in data.items():
            if key in self.supply_data.index:
                if isinstance(val, int):
                    data[key] = max(val, 0)
                elif isinstance(val, float):
                    data[key] = max(val, 0.0)
        return data

    def get_diff(self, data_start, data_end):
        return_dict = {}
        for key, val_end in data_end.items():
            try:
                val_start = data_start[key]
            except KeyError as exc:
                raise ValueError(
                    f"Bad input data, key '{key}' is not in second dict"
                ) from exc
            return_dict[key] = val_end - val_start
        return return_dict

    def _get_avg_vals(self, data):
        my_sum = 0
        for key, val in data.items():
            if key in self.supply_data.index:
                my_sum += val * self.supply_data.loc[key, "supply"]
        my_sum = max(my_sum, 1.0)
        new_dict = {}
        for key, val in data.items():
            new_dict[key] = min(val / my_sum, 1.0)
        return new_dict

    def _get_softmax_vals(self, data):
        my_sum = 0
        for key, val in data.items():
            if key in self.supply_data.index:
                my_sum += exp(val)
        my_sum = my_sum if my_sum else 1
        new_dict = {}
        for key, val in data.items():
            new_dict[key] = exp(val) / my_sum
        return new_dict

    def transform_diff(self, data_start, data_end):
        diff_dict = self.get_diff(data_start, data_end)
        diff_dict = self.ceil(diff_dict)
        new_dict = self.reducer_func(diff_dict)
        return new_dict

    def transform_single(self, data):
        data = self.ceil(data)
        new_dict = self.reducer_func(data)
        return new_dict


class Extractor:
    def __init__(self, game_info_db, build_order_db, ticks_per_second) -> None:
        self.game_info_db = game_info_db
        self.build_order_db = build_order_db
        self.ticks_per_second = ticks_per_second

    def extract_ids(self):
        ids = []
        with self.game_info_db as db:
            ids = [row[0] for row in db.get()]
        return ids

    def extract_data(self, game_id):
        with self.game_info_db as db:
            out = db.get_players_info(game_id)
            end_seconds, p1r, p1w, p1l, p2r, p2w, p2l = out
            data = {
                "end_tick": end_seconds * self.ticks_per_second,
                "player_1": {
                    "is_win": p1w if p1w is not None else False,
                    "race": p1r,
                    "league": p1l,
                },
                "player_2": {
                    "is_win": p2w if p2w is not None else False,
                    "race": p2r,
                    "league": p2l,
                },
            }
        return data

    def extract_build_order(self, game_id, ticks):
        return_dicts = []
        with self.build_order_db as db:
            for tick in ticks:
                try:
                    return_dicts.append(dict(db.get_by_keys(game_id, tick)))
                except TypeError as exc:
                    msg = f"Data not found for inputs game_id={game_id}, tick={tick} (out of {ticks})"
                    print(msg)
                    msg = "Consider cleaning the db"
                    print(msg)
                    raise TypeError from exc

        return return_dicts


class Loader:
    possible_r = set(("z", "t", "p"))
    possible_table_types = set(("comp", "winprob", "enemycomp"))

    def __init__(
        self, secrets_path, db_config_path, player_r, enemy_r, table_type
    ) -> None:
        assert player_r in self.possible_r
        assert enemy_r in self.possible_r
        assert table_type in self.possible_table_types

        self.secrets_path = secrets_path
        self.db_config_path = db_config_path
        self.player_r = player_r
        self.enemy_r = enemy_r
        self.table_name = f"{player_r}v{enemy_r}_{table_type}"

        self.db = MatchupDB(self.table_name, self.secrets_path, self.db_config_path)
        self.db_accessed = False

    def _format_entity_dict(self, entity_dict, prefix="p"):
        entity_dict = entity_dict.copy()
        new_entity_dict = {}
        for key, val in entity_dict.items():
            new_key = prefix + "_" + key.lower()
            new_entity_dict[new_key] = val
        return new_entity_dict

    def _get_formatted_dicts(
        self, player_entities: dict, enemy_entities: dict, out_entities: dict
    ):
        player_entities = self._format_entity_dict(player_entities, "p")
        enemy_entities = self._format_entity_dict(enemy_entities, "e")
        out_entities = self._format_entity_dict(out_entities, "out")
        return player_entities, enemy_entities, out_entities

    def prepare(self):
        self.db.change_table(self.table_name)

    def check_if_game_exists(self, game_id):
        with self.db as db:
            try:
                out = db.get_id(game_id)
            except AttributeError:
                return False
            except ProgrammingError:
                return False
            return bool(out)

    def check_if_tick_exists(self, game_id, tick):
        with self.db as db:
            try:
                out = db.get_by_key(game_id, tick)
            except AttributeError:
                return False
            except ProgrammingError:
                return False
            return bool(out)

    def upload_data(
        self,
        game_id: int,
        tick: int,
        player_entities: dict,
        enemy_entities: dict,
        out_entities: dict,
    ):
        player_entities, enemy_entities, out_entities = self._get_formatted_dicts(
            player_entities, enemy_entities, out_entities
        )

        with self.db as db:
            if not self.db_accessed:
                self.db.change_table(self.table_name)
                db.create_table(player_entities, enemy_entities, out_entities)
                self.db_accessed = True
            db.put(game_id, tick, player_entities, enemy_entities, out_entities)
