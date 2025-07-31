class ConverterMf4Xylon(ConverterBase):
    converter_path: Path

    def __init__(
        self,
        root_insecure_directory: Path,
        root_secure_directory: Path,
        thumbnails_directory: Path,
        root_intermediate_directory: Path,
        sftp_connection: SFTPConnection,
        downscaled_video_width: int,
        database_connection: DatabaseConnection
    ) -> None:
        super().__init__(
            root_insecure_directory,
            root_secure_directory,
            thumbnails_directory,
            root_intermediate_directory,
            sftp_connection,
            downscaled_video_width,
            database_connection)
        self.config = Config(Path(__file__).resolve().parents[4] / "config" / "marking_config.yaml")
        self.database_connection = database_connection
        self.converter_path = self.config.APTIV_MF4_TO_MP4_CONVERTER
        self.singularity_path = self.config.APTIV_SINGULARITY_IMAGE
        self.logger.info("Config seems to be correct.")

    def process(
        self,
        recording: Recording,
        recording_id: int,
        watermark_parameters: WatermarkParameters,
        remote_thumbnails_directory: Path,
        framerate: int,
    ) -> RecordingMetadata | None:
        if recording.path.is_file():
            self.logger.info(f"Processing {recording.path.name}.")
            source_mf4_path: Path = recording.path
            collection_recording_path = Path(*recording.path.parts[-3:-2])
            secure_destination_directory_path = self.config.SECURED_PROJECT_ROOT / self.config.APPROVED_DIRECTORY_RELATIVE / collection_recording_path / recording.name
            secure_destination_directory_path.mkdir(parents=True, exist_ok=True)
            unsecure_destination_directory_path = self.config.UNSECURED_PROJECT_ROOT / self.config.APPROVED_DIRECTORY_RELATIVE / collection_recording_path / recording.name
            unsecure_destination_directory_path.mkdir(parents=True, exist_ok=True)
            destination_mp4_path = secure_destination_directory_path / (recording.name + ".mp4")
            marked_full_scale_mp4_path: Path = unsecure_destination_directory_path / (recording.name + ".mp4")
            marked_downscaled_mp4_path: Path = marked_full_scale_mp4_path.with_stem(f"{marked_full_scale_mp4_path.stem}_downscaled")
            destination_raw_data_after_move_path = self.config.DESTINATION_RAW_FILE_PATH
            self.logger.info(f"{source_mf4_path=}.")
            self.logger.info(f"{secure_destination_directory_path=}.")
            self.logger.info(f"{unsecure_destination_directory_path=}.")
            self.logger.info(f"{destination_mp4_path=}.")
            self.logger.info(f"{marked_full_scale_mp4_path=}.")
            self.logger.info(f"{marked_downscaled_mp4_path=}.")
            self.logger.info(f"{self.config.APPROVED_DIRECTORY_RELATIVE=}.")
            self.logger.info(f"{self.config.SECURED_PROJECT_ROOT=}.")
            self.logger.info(f"{recording.path=}.")
            self.logger.info(f"{recording.name=}.")

            self._move_raw_data_before_convert(source_mf4_path, destination_raw_data_after_move_path, recording_id)
            self._convert_mf4_to_mp4(self.move_raw_data_file_path, destination_mp4_path, recording_id) # RAW_MP4
            self._extract_test_commander_csv(self.move_raw_data_file_path, destination_mp4_path)
            self._create_marked_videos(
                destination_mp4_path,
                recording_id,
                watermark_parameters,
                marked_full_scale_mp4_path,
                marked_downscaled_mp4_path,
                self.downscaled_video_width,
            )
            frame_number_csv_path = self._get_frame_number_csv_path(destination_mp4_path)
            frame_numbers = self._get_frame_numbers(frame_number_csv_path)
            self._embed_frame_numbers(frame_numbers, marked_full_scale_mp4_path)
            self._create_thumbnails(marked_downscaled_mp4_path, unsecure_destination_directory_path)
            self._upload_files_to_remote(remote_thumbnails_directory, unsecure_destination_directory_path, marked_downscaled_mp4_path)
            self._set_read_only(self.destination_dir_for_copy)
            self._delete_source_raw_data_file(self.source_copy_path)
            self._set_read_only(destination_mp4_path.parent)

            return self._extract_movie_metadata(destination_mp4_path)

        raise ConversionError(f"File {recording.path} has not been found.")

    def _convert_mf4_to_mp4(self, source_mf4_path: Path, destination_mp4_path: Path, recording_id: int) -> None:
        destination_mp4_path.parent.mkdir(parents=True, exist_ok=True)
        self.logger.info("Starting to convert video from mf4 to mp4")
        process = subprocess.run(
            [
                "singularity",
                "exec",
                "--bind",
                f"{source_mf4_path.parent}:/indir",
                "--bind",
                f"{destination_mp4_path.parent}:/outdir",
                "--bind",
                f"{self.converter_path}:/tools",
                f"{self.singularity_path}",
                "/bin/bash",
                "-c",
                "/tools/MDF4ToMp4Converter -i /indir -o /outdir -c",
            ],
            capture_output=True,
            check=True,
        )
        destination_directory_mp4_path = destination_mp4_path.parent
        self.logger.info(f"{destination_directory_mp4_path}=")
        for file in destination_directory_mp4_path.iterdir():
            if file.is_file() and "_c1_01" in file.stem:
                new_mp4_file_name = file.stem.replace("_c1_01", "") + file.suffix
                destination_mp4_rename_path = file.with_name(new_mp4_file_name)
                file.rename(destination_mp4_rename_path)
                if file.suffix == ".mp4":
                    self.database_connection.insert_file_into_database(recording_id=recording_id,
                                                                       path_to_file=destination_mp4_rename_path,
                                                                       file_status=FileStatus.GENERATED,
                                                                       size=destination_mp4_rename_path.stat().st_size,
                                                                       file_type=FileType.RAW_MP4)
                elif file.name.endswith("_FrameNumber.csv"):
                    self.database_connection.insert_file_into_database(recording_id=recording_id,
                                                                       path_to_file=destination_mp4_rename_path,
                                                                       file_status=FileStatus.GENERATED,
                                                                       size=destination_mp4_rename_path.stat().st_size,
                                                                       file_type=FileType.VIDEO_FRAMENUMBER_CSV)
                elif ".json" in file.suffixes and ".webm" in file.suffixes:
                    self.database_connection.insert_file_into_database(recording_id=recording_id,
                                                                       path_to_file=destination_mp4_rename_path,
                                                                       file_status=FileStatus.GENERATED,
                                                                       size=destination_mp4_rename_path.stat().st_size,
                                                                       file_type=FileType.VIDEO_WEBM_JSON)
                else:
                    self.database_connection.insert_file_into_database(recording_id=recording_id,
                                                                       path_to_file=destination_mp4_rename_path,
                                                                       file_status=FileStatus.GENERATED,
                                                                       size=destination_mp4_rename_path.stat().st_size)


        if error := process.stderr.decode().strip():
            self.logger.error(error)
        self.logger.info(process.stdout.decode().strip())

    def _get_frame_number_csv_path(self, mp4_path: Path) -> Path:
        return mp4_path.parent / f"{mp4_path.stem}_FrameNumber.csv"

    def _find_test_commander_mf4_file(self, source_mf4_directory_path: Path) -> Path | None:
        for pattern in ["*_sme6_*1.mf4", "*_sme2_*1.mf4"]:
            matches = list(source_mf4_directory_path.glob(pattern))
            if len(matches) == 1:
                return matches[0]

        return None

    def _extract_test_commander_csv(self,
                                 source_mf4_path: Path,
                                 mp4_path: Path):

        destination_directory_path = mp4_path.parent
        if test_commander_mf4_path := self._find_test_commander_mf4_file(source_mf4_path.parent):
            metadata = ffmpeg.probe(mp4_path)["streams"][0]
            frames_per_second = int(int(metadata["avg_frame_rate"].split("/")[0]) / int(metadata["avg_frame_rate"].split("/")[1]))
            try:
                convert_mf4_to_test_commander_csv(recording_name=destination_directory_path.name,
                                                  tc_mf4_file=test_commander_mf4_path,
                                                  target_directory_path=destination_directory_path,
                                                  frames_per_second=frames_per_second)
            except Exception as e:
                self.logger.error(f"An error occurred while extracting test_commander csv. Details: {e}")
        else:
            self.logger.error(f"Cannot find test commander mf4 in {source_mf4_path}")

    def _extract_movie_metadata(self, mp4_movie: Path) -> RecordingMetadata | None:
        if mp4_movie.is_file() and mp4_movie.suffix == ".mp4":
            self.logger.info(f"Filename {mp4_movie.stem} is valid. Extracting it's metadata.")
            if data_from_file := self._get_mp4_metadata(mp4_movie):
                name: str = mp4_movie.stem
                split_video_name: list = name.split("_")
                car_code_name: str = split_video_name[1]
                scenario_code_name: str = split_video_name[2]
                recording_software_version_code_name: str = split_video_name[3]
                recording_hardware_version_code_name: str = split_video_name[4]
                date_string: str = split_video_name[5]
                year: str = date_string[:4]
                month: str = date_string[4:6]
                day: str = date_string[6:8]
                hour: str = date_string[8:10]
                minute: str = date_string[10:12]
                second: str = date_string[12:14]
                recording_date: str = f"{year}-{month}-{day} {hour}:{minute}:{second}"

                return RecordingMetadata(
                    name,
                    data_from_file.frames_recorded,
                    data_from_file.frames_per_second,
                    data_from_file.image_width,
                    data_from_file.image_height,
                    data_from_file.path_to_video,
                    car_code_name,
                    recording_date,
                    recording_software_version_code_name,
                    recording_hardware_version_code_name,
                    scenario_code_name,
                )

            self.logger.error(f"Failed to get metadata from {mp4_movie.name}.")
            return None

        self.logger.error(f"Filename {mp4_movie.stem} is not valid.")
        return None

    def _get_frame_numbers(self, csv_path: Path) -> list[int]:
        self.logger.info(f"Getting recording's frame numbers")
        frame_numbers: list[int] = []
        if not csv_path.is_file():
            self.logger.error(f"Frame number CSV file not found: {csv_path}")
            return frame_numbers
        with open(csv_path, newline='') as csvfile:
            reader = csv.DictReader(csvfile, delimiter=';')
            for row in reader:
                value = row.get('Frame_number', '').strip()
                if value.isdigit():
                    frame_numbers.append(int(value))
                else:
                    self.logger.info(f'Empty or invalid value in row:{row}')
        return frame_numbers

    def _compare_directory_by_size(self, original_path: Path, copy_path: Path):
        for source_file in original_path.rglob("*"):
            relative_path = source_file.relative_to(original_path)
            destination_file = copy_path / relative_path
            if not destination_file.exists():
                return False
            if source_file.stat().st_size != destination_file.stat().st_size:
                return False
        return True

    def _move_raw_data_before_convert(self, source_mf4_path: Path, destination_raw_data_after_move_path: Path, recording_id : int):
        collection_dir_name = source_mf4_path.parents[1].name
        collection_backup_path = destination_raw_data_after_move_path / f"{collection_dir_name}_backup"
        self.logger.info(f"Collection backup path: {collection_backup_path}.")
        if not collection_backup_path.exists():
            collection_backup_path.mkdir(parents=True)
        copy_dir_name = source_mf4_path.parent.name
        self.destination_dir_for_copy = collection_backup_path / copy_dir_name
        self.move_raw_data_file_path = self.destination_dir_for_copy / source_mf4_path.name
        self.source_copy_path = source_mf4_path.parent
        shutil.copytree(str(self.source_copy_path), str(self.destination_dir_for_copy))
        if not self._compare_directory_by_size(self.source_copy_path, self.destination_dir_for_copy):
            raise ConversionError(f"Copy verification failed for: {self.destination_dir_for_copy}.")
        # At this point there should only be the raw MDF + tracked files so it's safe to extract everything by simple recording id
        recording_files = self.database_connection.get_files_from_recording_id(recording_id)
        # Change each file path to the new one
        for linked_file in recording_files:
            new_file_location = self.destination_dir_for_copy / Path(linked_file).name
            self.database_connection.modify_property(filter_value=linked_file,
                                                     property_name="path",
                                                     property_value=new_file_location,
                                                     table_name="file",
                                                     filter_name="path",)
        self.logger.info("The copy process was completed successfully.")

    def _set_read_only(self, new_path: Path):
        for file_path in new_path.rglob("*"):
            if file_path.is_file():
                file_path.chmod(stat.S_IRUSR | stat.S_IRGRP)
        self.logger.info("Permission is set.")

    def _delete_source_raw_data_file(self, source_copy_path: Path):
        if source_copy_path.exists():
            _remove_directory(source_copy_path)
            self.logger.info("Deleted source files.")
        else:
            self.logger.info("Source files not exist.")


class ConverterMf4Xylon2(ConverterMf4Xylon):
    """
    Class that handles conversion of recordings where frame data length is equal to 1333280.
    """

    def _convert_mf4_to_mp4(self, source_mf4_path: Path, destination_mp4_path: Path) -> None:
        destination_mp4_path.parent.mkdir(parents=True, exist_ok=True)
        frame_number_csv_path = self._get_frame_number_csv_path(destination_mp4_path)
        raw_frames_directory = destination_mp4_path.parent / "raw_frames"
        raw_frames_directory.mkdir(parents=True, exist_ok=True)
        self.logger.info("Starting to convert video from mf4 to mp4")

        video_mf4_list = sorted(source_mf4_path.parent.glob("*_c1_*.mf4"), key=lambda x: int(x.stem.split("_")[-1]))
        total_frame_count = sum([readFF(mf4).getFrameCount() for mf4 in video_mf4_list])
        video = readFF(video_mf4_list[0])
        video.seek(1)
        initial_timestamp = video.getMeta()["Time"] / 1_000_000_000
        self.logger.info(f"{frame_number_csv_path=}")
        self.logger.info(f"{total_frame_count=}")

        with open(frame_number_csv_path, mode="w+") as frame_number_csv_file:
            frame_number_csv_file.write("Frame_index;Frame_number;Absolute_timestamp;Relative_timestamp\n")
            i = 1
            for path_to_mf4 in video_mf4_list:
                self.logger.info(f"Processing {path_to_mf4.name}")
                video = readFF(path_to_mf4)

                for j in range(1, video.getFrameCount() + 1):
                    try:
                        video.seek(j)
                        frame_raw = dstack([video.getRaw()] * 3) * 255
                        frame_number = video.getMeta()["GId"]
                        absolute_timestamp = video.getMeta()["Time"] / 1_000_000_000
                        relative_timestamp = absolute_timestamp - initial_timestamp
                        frame_number_csv_file.write(f"{i};{frame_number};{absolute_timestamp};{relative_timestamp}\n")
                        raw_frame_path = raw_frames_directory / f"frame_{i:>06}.png"
                        imwrite(str(raw_frame_path), frame_raw)
                    except RuntimeError as e:
                        self.logger.warning(f"Failed reading frame at index {j}: {e}")

                    # log progress after each 100 frames processed:
                    if i % 100 == 0:
                        self.logger.info(f"Raw frames dump progress: {100 * (i / total_frame_count)}%")

                    i += 1

        self.logger.info(f"All raw frames saved to {raw_frames_directory}")
        self.logger.info("Combining raw frames into mp4 with ffmpeg started")
        (
            ffmpeg.input(f"{raw_frames_directory}/frame_*.png", framerate=60, pattern_type="glob")
            .output(str(destination_mp4_path), vcodec="copy")
            .run(quiet=False, overwrite_output=True)
        )

        if destination_mp4_path.is_file():
            self.logger.info(f"Saved mp4 to {destination_mp4_path}")
        else:
            self.logger.error(f"Cannot find mp4 in {destination_mp4_path}")

        self.logger.info(f"Removing raw frames directory: {raw_frames_directory}")
        _remove_directory(raw_frames_directory)
        self.logger.info("Raw frames directory removed")
