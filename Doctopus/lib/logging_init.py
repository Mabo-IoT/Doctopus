# -*- coding: utf-8 -*-

import logging
import logging.handlers
import os


def setup_logging(conf):
    """
    Initialize the logging module settings
    :param conf: dict, Initialize parameters
    :return:
    """
    level = {"DEBUG": logging.DEBUG, "INFO": logging.INFO, "WARNING": logging.WARNING,
             "ERROR": logging.ERROR, "CRITICAL": logging.CRITICAL}

    console = conf['console']  # console output
    console_level = conf['console_level']  # choose console log level to print
    file = conf['file']  # local log file output
    file_level = conf['file_level']  # choose log file level to save
    logfile = conf['log_file']  # local log file save position
    backup_count = conf['backup_count']  # count of local log files
    max_size = conf['max_size']  # size of each local log file
    format_string = conf['format_string']  # log message format

    log = logging.getLogger('Doctopus')
    log.setLevel(logging.DEBUG)

    formatter = logging.Formatter(format_string, datefmt='%Y-%d-%m %H:%M:%S')

    if file:
        # 如果 log 文本不存在，创建文本
        dir_path = os.path.dirname(logfile)
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)

        # 实例化一个 rotate file 的处理器，让日志文件旋转生成
        fh = logging.handlers.RotatingFileHandler(filename=logfile, mode='a', maxBytes=max_size,
                                                  backupCount=backup_count, encoding='utf-8')
        fh.setLevel(level[file_level])
        fh.setFormatter(formatter)
        log.addHandler(fh)

    if console:
        # 实例化一个流式处理器，将日志输出到终端
        ch = logging.StreamHandler()
        ch.setLevel(level[console_level])
        ch.setFormatter(formatter)
        log.addHandler(ch)

    return log


def test():
    log = logging.getLogger("root.test")
    log.warning("函数调用")


def test2():
    log = logging.getLogger("root")
    log.critical("同名调用")


if __name__ == '__main__':
    from Doctopus.utils.util import get_conf

    conf = get_conf('../conf/conf.toml')['log_configuration']
    log = setup_logging(conf)
    log.info("测试脚本")
    log.error("错误信息")
    test()
    test2()
