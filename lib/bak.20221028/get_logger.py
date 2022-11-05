import logging

def get_logger(name):
    log_format = '%(asctime)s | %(name)8s | %(levelname)7s | %(message)s'
    logging.basicConfig(level=logging.INFO,
                        format=log_format,
                        filename='../log/audit.log',
                        filemode='a')
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    console.setFormatter(logging.Formatter(log_format))
    logging.getLogger(name).addHandler(console)
    return logging.getLogger(name)

