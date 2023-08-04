from pathlib import Path

from alive_progress import alive_it

from database_access import BuildOrder, GameInfo, MapInfo, PlayerInfo
from starcraft2_replay_parse.replay_tools import BuildOrderData, ReplayData


class ReplayProcess:
    def __init__(
        self, secrets_path, db_config, game_data_path, max_tick=28800, ticks_per_pos=32
    ) -> None:
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

    def init_dbs(self):
        for db in self.dbs:
            with db:
                db.drop()
        for db in self.dbs:
            with db:
                db.create_table()

    def upload_game_info(self, replay, replay_path):
        replay_data = replay.as_dict()
        game_info = dict()
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
        game_info["player_2_id"] = replay_data["players_data"][player_2_name]["id"]
        game_info["player_2_race"] = replay_data["players_data"][player_2_name]["race"][
            0
        ].lower()
        game_info["player_2_league"] = replay_data["players_data"][player_2_name][
            "league"
        ]
        game_info["map_hash"] = replay.map_hash
        game_info["matchup"] = replay_data["matchup"]
        game_info["is_ladder"] = replay.is_ranked
        game_info["replay_path"] = str(replay_path.resolve())
        self.upload_info(self.game_info_db, **game_info)

    def upload_map_info(self, replay):
        replay_data = replay.as_dict()
        num_players = len(replay_data["matchup"].split("v")) // 2
        matchup_type = f"{num_players}v{num_players}"
        date = replay.replay.date
        map_info = {
            "map_hash": replay.map_hash,
            "map_name": replay.map_name,
            "matchup_type": matchup_type,
            "first_game_date": date.strftime("%d-%m-%Y"),
        }
        self.upload_info(self.map_info_db, **map_info)

    def upload_player_info(self, replay):
        replay_data = replay.as_dict()
        for name in replay.player_names:
            player_info = {
                "battle_tag": replay_data["players_data"][name]["id"],
                "nickname": name,
                "race": replay_data["players_data"][name]["race"][0].lower(),
                "league_int": replay_data["players_data"][name]["league"],
                "is_win": name in replay_data["winners"],
            }
            self.upload_info(self.player_info_db, **player_info)

    def upload_build_order(self, replay):
        replay_data = replay.as_dict()
        for build_order_dict in self.build_order_cls.yield_unit_counts(replay_data):
            self.upload_info(self.build_order_db, **build_order_dict)

    def upload_info(self, db, *args, **kwargs):
        if args:
            db.put(*args)
        elif kwargs:
            db.put(**kwargs)
        else:
            ValueError("Empty arguments")

    def process_replays(self, replay_dir):
        replay_dir = Path(replay_dir)
        list_file = [p for p in replay_dir.iterdir() if p.suffix == ".SC2Replay"]
        bar = alive_it(list_file)

        for i, replay_path in enumerate(bar):
            bar.text = f"Processing {i}/{len(list_file)}"

            replay = ReplayData().parse_replay(replay_path)
            self.upload_game_info(replay, replay_path)
            self.upload_map_info(replay)
            self.upload_player_info(replay)
            self.upload_build_order(replay)


if __name__ == "__main__":
    processor = ReplayProcess(
        "./configs/secrets.yml",
        "configs/database.yml",
        "./starcraft2_replay_parse/game_info.csv",
    )
    processor.process_replays("./replays/")
