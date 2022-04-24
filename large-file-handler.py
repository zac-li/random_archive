import json
import logging
import multiprocessing
import os
import sys
import time
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Dict, Optional

logging.basicConfig(
    level=logging.DEBUG,
    format="[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


class LargeFileHandler(object):
    def __init__(
        self,
        input_file_path: Path,
        output_file_path: Path,
        cores: int,
        file_splitted_size: Optional[int] = None,
    ):
        self.input_file_path = input_file_path
        self.output_file_path = output_file_path
        self.cores = cores

        if not file_splitted_size:
            file_size = input_file_path.stat().st_size
            self.file_splitted_size = file_size // cores
        else:
            self.file_splitted_size = file_splitted_size

        manager = multiprocessing.Manager()
        self.queue = manager.Queue()

    def process_wrapper(self, chunk_start: int, chunk_size: int) -> None:
        """The wrapper to call process given chunk_start and chunk_size."""

        logger.info(f"Worker: {multiprocessing.current_process().name} start!")
        try:
            counter = 0
            with open(self.input_file_path) as f:
                f.seek(chunk_start)
                lines = f.read(chunk_size).splitlines()
                for line in lines:
                    self.process(line)
                    counter += 1

                logger.info(
                    f"Worker: {multiprocessing.current_process().name} Processed: {counter} lines (start {chunk_start}, size {chunk_size})"
                )
        except Exception as e:
            logger.error(
                f"Exception caught! Worker: {multiprocessing.current_process().name} counter: {counter}, start {chunk_start}, size {chunk_size})"
            )

    def process(self, line: str) -> None:
        """The method to execute handle method given line. The return of handle would be dumped to queue."""

        dict_per_line = json.loads(line)
        self.transform_and_validate(dict_per_line)
        result = handle(**dict_per_line)
        logger.info(f"Processed {dict_per_line['id']}")

        self.queue.put(result + "\n")

    @staticmethod
    def transform_and_validate(dict_per_line: Dict) -> None:
        """Transform (and validate the schema of) the dict input so it can be consumed by handle function."""

        # transform the dict_per_line so it can be used as kwargs for function 'handle'
        dict_per_line["datetime"] = datetime.strptime(
            dict_per_line["datetime"], "%Y-%m-%d %H:%M:%S.%f"
        )
        dict_per_line["dt"] = dict_per_line["datetime"]
        del dict_per_line["datetime"]

        dict_per_line["price"] = Decimal(dict_per_line["price"])
        dict_per_line["quantity"] = Decimal(dict_per_line["quantity"])

        # TODO: validate the dict using some 3rd party lib

    def chunkify(self):
        """Calculate and generate chunk_start, chunk_size of the input file."""

        file_end = self.input_file_path.stat().st_size
        with open(self.input_file_path, "rb") as f:
            chunk_end = f.tell()  # current file position
            while True:
                chunk_start = chunk_end
                f.seek(
                    self.file_splitted_size, 1
                )  # sets the file's current position at the offset
                f.readline()  # make sure it goes to the newline char, because we need to read whole line, that's why split size is not equal to chunk
                chunk_end = f.tell()  # chunk_end keeps moving
                yield chunk_start, chunk_end - chunk_start
                if chunk_end > file_end:
                    break

    def listener(self):
        """listener of the queue, it will then populate the output file."""

        f = open(self.output_file_path, "w")
        while True:
            m = self.queue.get()
            if m == "KILL WORKER":
                break
            f.write(m)
            f.flush()
        f.close()

    def run(self):
        """Main API to handle the file in parallel."""

        pool = multiprocessing.Pool(self.cores)
        jobs = []

        # start the queue listener for output file population
        listener = pool.apply_async(aysnc_result_listener, (self,))

        # start processer jobs
        for chunk_start, chunk_size in self.chunkify():
            jobs.append(
                pool.apply_async(aysnc_processer, (self, chunk_start, chunk_size))
            )

        # wait for jobs to finish
        for job in jobs:
            job.get()

        # wrap up
        self.queue.put("KILL WORKER")
        pool.close()  # no new task
        pool.join()  # main process wait for tasks to be done

    def run_single(self) -> None:
        """Main API to handle the file sequentially."""

        try:
            with open(self.input_file_path) as f_input, open(
                self.output_file_path, "w+"
            ) as f_output:
                for line in f_input:
                    try:
                        dict_per_line = json.loads(line)
                        self.transform_and_validate(dict_per_line)
                        f_output.write(handle(**dict_per_line) + "\n")
                    except json.JSONDecodeError:
                        pass  # TODO: handle the exception here
        except IOError:
            pass  # TODO: handle the exception here


def handle(
    id: str, symbol: str, price: Decimal, quantity: Decimal, type: str, dt: datetime
) -> str:

    # dummy logic
    time.sleep(0.1)
    return f"processed TRADE {id}. TYPE: {type}, SYMBOL: {symbol}, PRICE: {price.quantize(Decimal('.01'))}, QUANTITY: {quantity.quantize(Decimal('.01'))}, TIME: {dt}"


def aysnc_result_listener(handler_obj: LargeFileHandler):
    handler_obj.listener()


def aysnc_processer(handler_obj: LargeFileHandler, chunk_start: int, chunk_size: int):
    handler_obj.process_wrapper(chunk_start, chunk_size)


if __name__ == "__main__":
    handler = LargeFileHandler(
        input_file_path=Path("/Users/zhichenli/sample.txt"),
        output_file_path=Path("/Users/zhichenli/result.txt"),
        cores=8,
    )
    handler.run()
    # handler.run_single()  # if running sequentially
