from itertools import permutations, zip_longest

from alive_progress import alive_bar, alive_it

from database_access import BuildOrder, GameInfo
from training_data import (CalcWinprob, DensityVals, Extractor, Loader,
                           NormalizeColumns, RandomPoints, ReorganizePlayers)


class Pipeline:
    """
    Transforms preprocessed data into datasets

    Uses data from tables:
        `game_info`,
        `build_order`

    Creates datasets in tables:
        `(matchup)_comp`
        `(matchup)_winprob`
        `(matchup)_enemycomp`
    """

    steps = []
    possible_r = set(("z", "t", "p"))
    ticks_per_min = 960

    def __init__(
        self,
        player_r,
        enemy_r,
        mins_per_point,
        game_ticks_per_second=16,
        tick_step=16,
        min_len=1920,
        jupyter=None,
    ) -> None:
        """
        Args:
            player_r: str - player's game race
            enemy_r: str - enemy's game race
            mins_per_point: int - used in random sampling
            game_ticks_per_second: int - replay game speed
            tick_step: int - step in tick in the database
            min_len: int - minimum game length in ticks
            jupyter: bool | None - fix progress bar
        """
        self.player_r = player_r
        self.enemy_r = enemy_r
        self.ticks_per_point = mins_per_point * self.ticks_per_min
        self.jupyter = jupyter
        self.game_ticks_per_second = game_ticks_per_second
        self.tick_step = tick_step
        self.min_len = min_len

    def configure_dbs(self, secrets_path, db_config_path):
        """
        Configures preprocessed data tables.

        Args:
            secrets_path: str - path to secrets file
            db_config_path: str - path to db config
        """
        self.secrets_path = secrets_path
        self.db_config_path = db_config_path
        self.game_info_db = GameInfo(secrets_path, db_config_path)
        self.build_order_db = BuildOrder(secrets_path, db_config_path)

    def configure_organize(self, player_r, enemy_r, min_league, include_unranked=True):
        """
        Configures ReorganizePlayers class
        Args:
            player_r: str - player's game race
            enemy_r: str - enemy's game race
            min_league: int - filter out leagues below this val
            include_unranked: bool - include league=0
        """
        self.organize = ReorganizePlayers(
            player_r, enemy_r, min_league, include_unranked
        )

    def configure_points(self, sigma, get_final_point, final_point_step):
        """
        Configures ReorganizePlayers class
        Args:
            player_r: str - player's game race
            enemy_r: str - enemy's game race
            min_league: int - filter out leagues below this val
            include_unranked: bool - include league=0
        """
        self.points = RandomPoints(
            mean_step=self.ticks_per_point,
            sigma=sigma,
            get_final_point=get_final_point,
            final_point_step=final_point_step,
            tick_step=self.tick_step,
        )

    def configure_normalize(self, game_info_file, supply_data_file):
        """
        Configures NormalizeColumns class
        Args:
            game_info_file: str - path to game_info.csv
            supply_data_file: str - path to supply_data file
        """
        self.normalize = NormalizeColumns(game_info_file, supply_data_file)

    def configure_calc_winprob(self, delay):
        """
        Configures CalcWinprob class
        Args:
            delay: int - Lag defining the sigmoid function
                         sensibility
        """
        self.winprob = CalcWinprob(delay)

    def configure_dense(self, supply_data_file, reducer):
        """
        Configures DensityVals class
        Args:
            supply_data_file: str - path to game_info.csv
            reducer: str - ['avg', 'softmax'] reducer formula
        """
        self.dense = DensityVals(supply_data_file, reducer)

    def configure_extractor(self):
        """
        Configures Extractor class
        """
        self.extractor = Extractor(
            self.game_info_db, self.build_order_db, self.game_ticks_per_second
        )

    def transform_player(self, data, player):
        raise NotImplementedError

    def transform_enemy(self, data, enemy):
        raise NotImplementedError

    def transform_out(
        self, earlier_data, later_data, player, enemy, final_tick, is_win, end_tick
    ):
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
                if i == 0:
                    data["player_1"] = p1_data
                    data["player_2"] = p2_data
                elif i == 1:
                    data["player_1"] = p2_data
                    data["player_2"] = p1_data

                is_pass, curr_player, is_win = self.organize.transform(data)
                if is_pass:
                    yield (id, curr_player, is_win, data["end_tick"])

    def run(self):
        """
        Run pipeline.

        Transformation process:
            1. Extract all game ids from the game_info
            2. For each player:
            2.1 Get random starting and ending ticks
            2.2 Get player's build_order data for each tick
            2.3 Transform extracted data columns to expected format
            2.4 Load data into a new table
        """
        if not all((hasattr(self, name) for name in self.steps)):
            vals = [f"{name}: {hasattr(self, name)}\n" for name in self.steps]
            raise ValueError(f"Missing configured steps: \n{vals}")

        ids = self.extractor.extract_ids()
        self.loader.prepare()

        for game_id, player, is_win, end_tick in self._iter_id_player_is_win(ids):
            if self.loader.check_if_game_exists(game_id):
                continue

            enemy = "player_1" if player == "player_2" else "player_2"
            starting_points, end_points = self.points.transform(end_tick)
            starting_dicts = self.extractor.extract_build_order(
                game_id, starting_points
            )
            if end_points:
                end_dicts = self.extractor.extract_build_order(game_id, end_points)
            else:
                end_dicts = []
            for start_dict, end_dict, end_point in zip_longest(
                starting_dicts, end_dicts, end_points
            ):
                player_dict = self.transform_player(start_dict, player)
                enemy_dict = self.transform_enemy(start_dict, enemy)
                out_dict = self.transform_out(
                    start_dict, end_dict, player, enemy, end_point, is_win, end_tick
                )

                tick = start_dict["tick"]
                if not self.loader.check_if_tick_exists(game_id, tick):
                    self.loader.upload_data(
                        game_id, tick, player_dict, enemy_dict, out_dict
                    )


class CompPipeline(Pipeline):
    """
    Pipeline for creating `(matchup)_comp` datasets
    """

    steps = [
        "extractor",
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
        )
        return self.normalize.transform(data)

    def transform_enemy(self, data, enemy):
        self.normalize.setup_filter(enemy, self.enemy_r)
        return self.normalize.transform(data)

    def transform_out(
        self, earlier_data, later_data, player, enemy, final_tick, is_win, end_tick
    ):
        self.normalize.setup_filter(player, self.player_r)
        out1_dict = self.normalize.transform(earlier_data)
        out2_dict = self.normalize.transform(later_data)
        out_dict = self.dense.transform_diff(out1_dict, out2_dict)
        return out_dict


class WinprobPipeline(Pipeline):
    """
    Pipeline for creating `(matchup)_winprob` datasets
    """

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
        )
        return self.normalize.transform(data)

    def transform_enemy(self, data, enemy):
        self.normalize.setup_filter(enemy, self.enemy_r, include_buildings=True)
        return self.normalize.transform(data)

    def transform_out(
        self, earlier_data, later_data, player, enemy, final_tick, is_win, end_tick
    ):
        out_dict = {}
        out_dict["is_win"] = self.winprob.transform(final_tick, is_win, end_tick)
        return out_dict


class EnemycompPipeline(Pipeline):
    """
    Pipeline for creating `(matchup)_enemycomp` datasets
    """

    steps = [
        "extractor",
        "organize",
        "points",
        "normalize",
        "dense",
        "loader",
    ]

    def configure_loader(self):
        super()._configure_loader("enemycomp")

    def transform_player(self, data, player):
        return {}

    def transform_enemy(self, data, enemy):
        self.normalize.setup_filter(
            enemy,
            self.enemy_r,
            include_buildings=True,
            include_units=False,
        )
        return self.normalize.transform(data)

    def transform_out(
        self, earlier_data, later_data, player, enemy, final_tick, is_win, end_tick
    ):
        self.normalize.setup_filter(enemy, self.enemy_r, include_buildings=True)
        out_dict = self.normalize.transform(earlier_data)
        out_dict = self.dense.transform_single(out_dict)
        return out_dict


class PipelineComposer:
    """
    Configures and returns pipeline for each case.
    """

    def __init__(self, matchup: str, tick_step=16, jupyter=None) -> None:
        """
        Args:
            matchup: str - two game races separated with 'v' ['ZvT', 'TvP' ...]
            tick_step: int - step of data in preprocessed DB
            jupyter: bool | None - fix progress bar
        """
        self.player_r, self.enemy_r = matchup.lower().split("v")
        self.jupyter = jupyter
        self.tick_step = tick_step
        self.secrets_path = "./configs/secrets.yml"
        self.db_config_path = "./configs/database.yml"
        self.game_info_file = "./starcraft2_replay_parse/data/game_info.csv"
        self.supply_data_file = "./game_data/supply_data.csv"

    def change_matchup(self, matchup):
        """
        Changes dataset table with new matchup value.

        Args:
            matchup: str - two game races separated with 'v' ['ZvT', 'TvP' ...]

        """
        self.player_r, self.enemy_r = matchup.lower().split("v")

    def get_compositon(
        self,
        mins_per_sample: int,
        prediction_minute_step: int,
        min_league: int,
        reducer="avg",
    ):
        """
        Configure and return CompPipeline
        """
        pipeline = CompPipeline(
            self.player_r,
            self.enemy_r,
            mins_per_sample,
            game_ticks_per_second=16,
            tick_step=self.tick_step,
            jupyter=self.jupyter,
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
        pipeline.configure_dense(self.supply_data_file, reducer)
        pipeline.configure_loader()
        return pipeline

    def get_win_probability(
        self, mins_per_sample, prediction_minute_step, min_league, reducer="avg"
    ):
        """
        Configure and return WinprobPipeline
        """
        pipeline = WinprobPipeline(
            self.player_r,
            self.enemy_r,
            mins_per_sample,
            game_ticks_per_second=16,
            tick_step=self.tick_step,
            jupyter=self.jupyter,
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
        pipeline.configure_dense(self.supply_data_file, reducer)
        pipeline.configure_loader()
        return pipeline

    def get_enemy_composition(
        self, mins_per_sample, prediction_minute_step, min_league, reducer="avg"
    ):
        """
        Configure and return EnemycompPipeline
        """
        pipeline = EnemycompPipeline(
            self.player_r,
            self.enemy_r,
            mins_per_sample,
            game_ticks_per_second=16,
            tick_step=self.tick_step,
            jupyter=self.jupyter,
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
        pipeline.configure_dense(self.supply_data_file, reducer)
        pipeline.configure_loader()
        return pipeline


if __name__ == "__main__":
    MINS_PER_SAMPLE = 4
    PRED_STEP = 1
    MIN_LEAGUE = 3
    r_pairs = permutations("ZTP", 2)
    matchups = ["v".join((r1, r2)) for r1, r2 in r_pairs]
    composer = PipelineComposer("ZvZ", tick_step=32)
    matchups = ["zvt"]
    for matchup in matchups:
        composer.change_matchup(matchup)
        comp_pipeline = composer.get_compositon(MINS_PER_SAMPLE, PRED_STEP, MIN_LEAGUE)
        winprob_pipeline = composer.get_win_probability(
            MINS_PER_SAMPLE, PRED_STEP, MIN_LEAGUE
        )
        enemycomp_pipeline = composer.get_enemy_composition(
            MINS_PER_SAMPLE, PRED_STEP, MIN_LEAGUE
        )

        comp_pipeline.run()
        winprob_pipeline.run()
        enemycomp_pipeline.run()
