#!/usr/bin/env python3

import subprocess
import shutil
import logging
from typing import Iterator, TextIO

from log_parser import LogFormat


def sort_log_stream(input_stream: TextIO, format: LogFormat = LogFormat.SYSTEMD) -> Iterator[str]:
    """Сортирует строки лога в обратном порядке используя sort
    """
    try:
        if format == LogFormat.SYSTEMD:
            # В логах в формате systemd правильное время находится на 6 месте, поэтому -k6,6
            ts_pos = 6
        else:
            ts_pos = 1

        process = subprocess.Popen(
            ['sort', f'-k{ts_pos},{ts_pos}', '-r'],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding='utf-8'
        )
        
        try:
            # Так вроде бы эффективно
            shutil.copyfileobj(input_stream, process.stdin)
            process.stdin.close()
        except BrokenPipeError:
            pass
        logging.info("Log read sucessfuly")
        yield process.stdout.readline()
        logging.info("Log sorted sucessfuly")
        for line in process.stdout:
            yield line.rstrip('\n')
        
        return_code = process.wait()
        if return_code != 0:
            stderr_output = process.stderr.read()
            raise RuntimeError(f"Sort command failed with return code {return_code}: {stderr_output}")
            
    except FileNotFoundError:
        raise RuntimeError("Sort command not found. Please ensure 'sort' is installed and available in PATH.")
    except Exception as e:
        raise RuntimeError(f"Error running sort command: {e}")
    