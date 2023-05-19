from utils.logger import setup_logger


def main():
    logger = setup_logger(name='data_downloader_logger', folder_path='.//data')

    logger.info('Hello, world')


if __name__ == '__main__':
    main()
