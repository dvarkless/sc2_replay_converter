from math import exp
from pathlib import Path
from random import gauss

import pandas as pd

from database_access import BuildOrder, GameInfo, MatchupDB, PlayerInfo
from replay_process import ReplayFilter


class ReorganizePlayers:
    def __init__(self, player: str, enemy: str, min_league: int, include_unranked=True) -> None:
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

    def get_random_ticks(self, end_val, tick_step, constrained=True):
        end_val = int(end_val // tick_step * tick_step)
        out_tick = 0
        out_list = []
        end_tick = end_val if not constrained else end_val - self.final_point_step
        while out_tick < end_tick:
            out_tick = int(out_tick // tick_step * tick_step)
            out_list.append(out_tick)
            out_tick += self.mean_step * (gauss(self.mean_step, self.sigma) + 1)

        worst_case_val = [int(end_val // 2 // tick_step * tick_step)]
        return out_list if out_list else worst_case_val

    def get_final_points(self, starting_points, end_val, tick_step):
        end_val = int(end_val // tick_step * tick_step)
        final_points = []
        if self.get_final_point:
            for point in starting_points:
                final_point = point + self.final_point_step
                final_point = int(final_point // tick_step * tick_step)
                final_point = min(final_point, end_val)
                final_points.append(point)
        return final_points

    def transform(self, end_val):
        out_points = self.get_random_ticks(
            end_val, self.tick_step, constrained=self.get_final_point
        )
        final_points = self.get_final_points(out_points, end_val, self.tick_step)
        return (out_points, final_points)


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
        self.r = r
        self.include_buildings = include_buildings
        self.include_upgrades = include_upgrades
        self.include_special = include_special
        self.include_tick = include_tick
        self.include_units = include_units

    def filter_columns(self, data):
        # ("player_N_column", int_val)
        pairs = list(data.items())
        # Example:
        # Pass if player_1 in player_1_Drone
        pairs = [pair for pair in pairs if self.player in pair[0]]
        # player_1_Drone ==> Drone
        pairs = [(pair[0].removeprefix(self.player + "_"), pair[1]) for pair in pairs]
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
        if name in self.supply_data.loc[:, "name"]:
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
    ) -> None:
        self.supply_data = pd.read_csv(supply_data_file, index_col="name")

    def ceil(self, data):
        for key, val in data.items():
            if key in self.supply_data.index:
                if isinstance(val, int):
                    data[key] = max(val, 0)
                elif isinstance(val, float):
                    data[key] = max(val, 0.0)
        return data

    def _get_sum(self, data):
        sum = 0
        for key, val in data.items():
            if key in self.supply_data.index:
                sum += val
        return sum

    def transform(self, data):
        units_sum = self._get_sum(data)
        new_dict = {}
        for key, val in data.items():
            new_dict[key] = val/units_sum
        return self.ceil(new_dict)


class Extractor:
    def __init__(self, game_info_db, build_order_db) -> None:
        self.game_info_db = game_info_db
        self.build_order_db = build_order_db

    def extract_ids(self):
        ids = []
        with self.game_info_db as db:
            for row in db.get():
                ids.append(row[0])
        return ids

    def extract_data(self, game_id):
        with self.game_info_db as db:
            end_tick, p1r, p1w, p1l, p2r, p2w, p2l = db.get_players_info(game_id)
            data = {
                "end_tick": end_tick,
                "player_1": {
                    "is_win": p1w,
                    "race": p1r,
                    "league": p1l,
                },
                "player_2": {
                    "is_win": p2w,
                    "race": p2r,
                    "league": p2l,
                },
            }
        return data

    def extract_build_order(self, game_id, ticks):
        return_dicts = []
        with self.build_order_db as db:
            for tick in ticks:
                return_dicts.append(db.get_by_keys(game_id, tick))

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

        self.db = self.get_init_table()

    def get_init_table(self):
        if not hasattr(self, "db"):
            return MatchupDB(self.table_name, self.secrets_path, self.db_config_path)
        return self.db

    def _format_entity_dict(self, entity_dict, prefix="p"):
        entity_dict = entity_dict.copy()
        for key, val in entity_dict.items():
            new_key = prefix + "_" + key
            entity_dict[new_key] = val
            del entity_dict[key]
        return entity_dict

    def _get_formatted_dicts(
        self, player_entities: dict, enemy_entities: dict, out_entities: dict
    ):
        player_entities = self._format_entity_dict(player_entities, "p")
        enemy_entities = self._format_entity_dict(enemy_entities, "e")
        out_entities = self._format_entity_dict(out_entities, "out")
        return player_entities, enemy_entities, out_entities

    def upload_data(
        self, player_entities: dict, enemy_entities: dict, out_entities: dict
    ):
        player_entities, enemy_entities, out_entities = self._get_formatted_dicts(
            player_entities, enemy_entities, out_entities
        )

        with self.db as db:
            db.put(player_entities, enemy_entities, out_entities)
