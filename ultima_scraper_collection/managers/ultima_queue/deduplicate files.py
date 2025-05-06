from ultima_scraper_collection.config import UltimaScraperCollectionConfig

config = UltimaScraperCollectionConfig().load_default_config()
for directory_path in config.site_apis.onlyfans.download_setup.directories:
    assert directory_path.path
    directory_path = directory_path.path.joinpath("OnlyFans")
    if not directory_path.exists():
        continue
    sorted_dirs = sorted(directory_path.iterdir(), key=lambda x: x.name.lower())
    total_bytes_deleted = 0
    for letter in sorted_dirs:
        print(letter)
        letter_path = directory_path.joinpath(letter)
        for performer_folder in letter_path.iterdir():
            messages_folder = performer_folder.joinpath("Messages")
            mass_messages_folder = performer_folder.joinpath("MassMessages")
            if messages_folder.exists() and mass_messages_folder.exists():
                free_messages_folder = messages_folder.joinpath("Free")
                free_mass_messages_folder = mass_messages_folder.joinpath("Free")
                paid_messages_folder = messages_folder.joinpath("Paid")
                paid_mass_messages_folder = mass_messages_folder.joinpath("Paid")
                collection = [
                    [free_messages_folder, free_mass_messages_folder],
                    [paid_messages_folder, paid_mass_messages_folder],
                ]
                for value_message_folder, value_mass_message_folder in collection:
                    if (
                        value_message_folder.exists()
                        and value_mass_message_folder.exists()
                    ):
                        v_m_f = value_message_folder.rglob("*.mp4")
                        v_m_m_f = value_mass_message_folder.rglob("*.mp4")
                        for video_filepath in v_m_m_f:
                            for video_filepath2 in v_m_f:
                                if video_filepath.name == video_filepath2.name:
                                    video_filepath_bytes = video_filepath.read_bytes()
                                    video_filepath2_bytes = video_filepath2.read_bytes()
                                    if video_filepath_bytes == video_filepath2_bytes:
                                        print(
                                            f"Duplicate found: {video_filepath} and {video_filepath2}"
                                        )
                                        video_filepath2.unlink()
                                        total_bytes_deleted += (
                                            video_filepath.stat().st_size
                                        )
                                        print(
                                            f"Total bytes deleted: {total_bytes_deleted / (1024 * 1024 * 1024):.2f} GB"
                                        )
                                        pass
                                    else:
                                        print(
                                            f"Intervention needed: {video_filepath} and {video_filepath2}"
                                        )
                                        input("Press enter to continue.")
