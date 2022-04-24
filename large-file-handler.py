"""
说明：

1. LargeFileHandler.run_single方法用于顺序处理简单的小量数据
2. LargeFileHandler.run 用于单机并行处理较大量数据(仅为prototype)
   如果我们假设目前有多个计算机器（64核/128GB内存/500GB硬盘）和存储机器（64核/128GB内存/1PB硬盘）, 而输入文本文件在TB级别，那么high level计算流程如下：
   a. data storage 将TB级文本切分成若干个每台计算机单机可以单次存储的小文本（< 500G）
   b. 每台计算机获得其中一份小文本。
   c. 创建LargeFileHandler实例，调用run方法, 生成result.txt，具体流程如下：
	  i.   hanlder init的时候建立queue和根据计算机核心数量决定每个进程处理的文本大小
	  ii.  run方法里建立aysnc_result_listener进程和aysnc_processer进程，并开始运行
	  iii. aysnc_processer会根据计算出来的offset(chunk_start)和size(chunk_size)依次读取输入文件里的特定段落，调用handler方法, 将结果放入queue
	  iv.  aysnc_result_listener监听queue, 把结果写入result.txt

   d. 如有需要，所有计算生成的result.txt可被合并，否则可直接被消费者消费
3. 本Solution的其他假设:
   a. 输入文件非空，数据完全符合schema，如有需要，可使用3rd party validation工具来validate schema和报错
4. Log记录了每个aysnc_processer处理的进度，可用于程序意外停止后的修复（需脚本手动回复）
5. LargeFileHandler.run_single和run已经过小量数据检测

"""


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
