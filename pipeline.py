from itertools import zip_longest

from alive_progress import alive_bar, alive_it

from database_access import BuildOrder, GameInfo
from training_data import (CalcWinprob, CeilOutput, DensityVals, Extractor,
                           FilterRace, Loader, NormalizeColumns,
                           NormalizeUnits, RandomPoints, ReorganizePlayers)


class Pipeline:
    steps = []
    possible_r = set(("z", "t", "p"))
    ticks_per_min = 960

    def __init__(
        self, player_r, enemy_r, mins_per_point, tick_step=16, jupyter=None
    ) -> None:
        self.player_r = player_r
        self.enemy_r = enemy_r
        self.ticks_per_point = mins_per_point * self.ticks_per_min
        self.jupyter = jupyter
        self.tick_step = tick_step

    def configure_dbs(self, secrets_path, db_config_path):
        self.secrets_path = secrets_path
        self.db_config_path = db_config_path
        self.game_info_db = GameInfo(secrets_path, db_config_path)
        self.build_order_db = BuildOrder(secrets_path, db_config_path)

    def configure_organize(self, player_r, enemy_r, min_league, include_unranked=True):
        self.organize = ReorganizePlayers(
            player_r, enemy_r, min_league, include_unranked
        )

    def configure_points(self, sigma, get_final_point, final_point_step):
        self.points = RandomPoints(
            self.ticks_per_point,
            sigma,
            get_final_point,
            final_point_step,
            self.tick_step,
        )

    def configure_normalize(self, game_info_file, supply_data_file):
        self.normalize = NormalizeColumns(game_info_file, supply_data_file)

    def configure_calc_winprob(self, delay):
        self.winprob = CalcWinprob(delay)

    def configure_dense(self, supply_data_file):
        self.dense = DensityVals(supply_data_file)

    def configure_extractor(self):
        self.extractor = Extractor(self.game_info_db, self.build_order_db)

    def transform_player(self, data, player):
        raise NotImplementedError

    def transform_enemy(self, data, enemy):
        raise NotImplementedError

    def transform_out(self, data, player, enemy, final_tick, is_win, end_tick):
        raise NotImplementedError

    def _configure_loader(self, table_type):
        self.loader = Loader(
            self.secrets_path,
            self.db_config_path,
            self.player_r,
            self.enemy_r,
            table_type,
        )

    def configure_loader(self):
        raise NotImplementedError

    def _iter_id_player_is_win(self, ids):
        if self.jupyter is not None:
            alive_bar()
            bar = alive_it(ids, title="Pipeline", force_tty=self.jupyter)
        else:
            bar = alive_it(ids, title="Pipeline")

        for id in bar:
            data = self.extractor.extract_data(id)
            p1_data = data["player_1"].copy()
            p2_data = data["player_2"].copy()
            for i in range(2):
                if i:
                    data["player_1"] = p1_data
                    data["player_2"] = p2_data
                else:
                    data["player_1"] = p2_data
                    data["player_2"] = p1_data

                is_pass, curr_player, is_win = self.organize.transform(data)
                if is_pass:
                    yield (id, curr_player, is_win, data["end_tick"])

    def run(self):
        assert all((hasattr(self, name) for name in self.steps))
        ids = self.extractor.extract_ids()

        for game_id, player, is_win, end_tick in self._iter_id_player_is_win(ids):
            enemy = "player_1" if player == "player_2" else "player_2"
            starting_points, end_points = self.points.transform(end_tick)
            starting_dicts = self.extractor.extract_build_order(game_id, starting_points)
            if end_points:
                end_dicts = self.extractor.extract_build_order(game_id, end_points)
            else:
                end_dicts = []
            for start_dict, end_dict, end_point in zip_longest(
                starting_dicts, end_dicts, end_points
            ):
                player_dict = self.transform_player(start_dict, player)
                enemy_dict = self.transform_enemy(start_dict, enemy)
                out_data = end_dict if end_dict is not None else start_dict
                out_dict = self.transform_out(
                    out_data, player, enemy, end_point, is_win, end_tick
                )
                self.loader.upload_data(player_dict, enemy_dict, out_dict)


class CompPipeline(Pipeline):
    steps = [
        "extractor",
        "filter",
        "organize",
        "points",
        "normalize",
        "dense",
        "loader",
    ]

    def configure_loader(self):
        super()._configure_loader("comp")

    def transform_player(self, data, player):
        self.normalize.setup_filter(
            player,
            self.player_r,
            include_buildings=True,
            include_special=True,
            include_tick=True,
        )
        return self.normalize.transform(data)

    def transform_enemy(self, data, enemy):
        self.normalize.setup_filter(enemy, self.enemy_r)
        return self.normalize.transform(data)

    def transform_out(self, data, player, enemy, final_tick, is_win, end_tick):
        self.normalize.setup_filter(player, self.player_r)
        out_dict = self.normalize.transform(data)
        out_dict = self.dense.transform(out_dict)
        return out_dict


class WinprobPipeline(Pipeline):
    steps = [
        "extractor",
        "organize",
        "points",
        "normalize",
        "winprob",
        "dense",
        "loader",
    ]

    def configure_loader(self):
        super()._configure_loader("winprob")

    def transform_player(self, data, player):
        self.normalize.setup_filter(
            player,
            self.player_r,
            include_buildings=True,
            include_upgrades=True,
            include_special=True,
            include_tick=True,
        )
        return self.normalize.transform(data)

    def transform_enemy(self, data, enemy):
        self.normalize.setup_filter(enemy, self.enemy_r, include_buildings=True)
        return self.normalize.transform(data)

    def transform_out(self, data, player, enemy, final_tick, is_win, end_tick):
        out_dict = {}
        out_dict["is_win"] = self.winprob.transform(final_tick, is_win, end_tick)
        return out_dict


class EnemycompPipeline(Pipeline):
    steps = [
        "extractor",
        "filter",
        "organize",
        "points",
        "normalize",
        "dense",
        "loader",
    ]

    def configure_loader(self):
        super()._configure_loader("winprob")

    def transform_player(self, data, player):
        return {}

    def transform_enemy(self, data, enemy):
        self.normalize.setup_filter(
            enemy,
            self.enemy_r,
            include_buildings=True,
            include_tick=True,
            include_units=False,
        )
        return self.normalize.transform(data)

    def transform_out(self, data, player, enemy, final_tick, is_win, end_tick):
        self.normalize.setup_filter(enemy, self.enemy_r, include_buildings=True)
        out_dict = self.normalize.transform(data)
        out_dict = self.dense.transform(out_dict)
        return out_dict


class PipelineComposer:
    def __init__(self, matchup: str, tick_step=16, jupyter=None) -> None:
        self.player_r, self.enemy_r = matchup.lower().split("v")
        self.jupyter = jupyter
        self.tick_step = tick_step
        self.secrets_path = "./configs/secrets.yml"
        self.db_config_path = "./configs/database.yml"
        self.game_info_file = "./starcraft2_replay_parse/game_info.csv"
        self.supply_data_file = "./game_data/supply_data.csv"

    def get_compositon(self, mins_per_sample, prediction_minute_step, min_league):
        pipeline = CompPipeline(
            self.player_r, self.enemy_r, mins_per_sample, self.tick_step, self.jupyter
        )
        final_point_step = prediction_minute_step * pipeline.ticks_per_min
        pipeline.configure_dbs(self.secrets_path, self.db_config_path)
        pipeline.configure_extractor()
        pipeline.configure_organize(self.player_r, self.enemy_r, min_league)
        pipeline.configure_points(
            final_point_step * 0.5,
            get_final_point=True,
            final_point_step=final_point_step,
        )
        pipeline.configure_normalize(self.game_info_file, self.supply_data_file)
        pipeline.configure_dense(self.supply_data_file)
        pipeline.configure_loader()
        return pipeline

    def get_winprob(self, mins_per_sample, prediction_minute_step, min_league):
        pipeline = WinprobPipeline(
            self.player_r, self.enemy_r, mins_per_sample, self.tick_step, self.jupyter
        )
        final_point_step = prediction_minute_step * pipeline.ticks_per_min
        pipeline.configure_dbs(self.secrets_path, self.db_config_path)
        pipeline.configure_extractor()
        pipeline.configure_organize(self.player_r, self.enemy_r, min_league)
        pipeline.configure_points(
            final_point_step * 0.5,
            get_final_point=True,
            final_point_step=final_point_step,
        )
        pipeline.configure_normalize(self.game_info_file, self.supply_data_file)
        # Delay determines tolerance for game lengths >> final_point_step
        pipeline.configure_calc_winprob(delay=5)
        pipeline.configure_dense(self.supply_data_file)
        pipeline.configure_loader()
        return pipeline

    def get_enemycomp(self, mins_per_sample, prediction_minute_step, min_league):
        pipeline = CompPipeline(
            self.player_r, self.enemy_r, mins_per_sample, self.jupyter
        )
        final_point_step = prediction_minute_step * pipeline.ticks_per_min
        pipeline.configure_dbs(self.secrets_path, self.db_config_path)
        pipeline.configure_extractor()
        pipeline.configure_organize(self.player_r, self.enemy_r, min_league)
        pipeline.configure_points(
            final_point_step * 0.5,
            get_final_point=True,
            final_point_step=final_point_step,
        )
        pipeline.configure_normalize(self.game_info_file, self.supply_data_file)
        pipeline.configure_dense(self.supply_data_file)
        pipeline.configure_loader()
        return pipeline
